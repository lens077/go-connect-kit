// Package redisclient builds go-redis clients and keeps one hot-swappable client per process.
//
// Like pgpool, this replaces a buildRedis + LiveRedis pair that each consumer used to copy.
// Consumers only map their configuration message to Options.
package redisclient

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lens077/go-connect-kit/config"
	"github.com/lens077/go-connect-kit/internal/stale"
	otelpkg "github.com/lens077/go-connect-kit/otel"
	"github.com/redis/go-redis/v9"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

const (
	// drainTimeout is how long a replaced client stays open so in-flight commands can finish.
	drainTimeout = 30 * time.Second
	// defaultPingTimeout bounds the startup ping when DialTimeout is unset. Without it a
	// zero timeout would fail the ping immediately.
	defaultPingTimeout = 5 * time.Second
)

// TLSOptions configures TLS to the server. The server name is taken from Addr.
type TLSOptions struct {
	Enabled            bool
	InsecureSkipVerify bool
	// CAPEM is the PEM-encoded CA bundle; empty means the system roots.
	CAPEM string
}

// Options is the provider-neutral Redis client configuration. Zero durations and sizes
// keep go-redis defaults.
type Options struct {
	Addr     string
	Username string
	Password string
	DB       int

	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	PoolSize     int
	MinIdleConns int

	TLS TLSOptions
}

// Projector maps a caller-owned configuration message into client Options.
type Projector[T proto.Message] func(T) Options

// Live holds the current client and lets it be replaced as a whole.
//
// redis.Client has too many methods to forward, so callers go through Client() every
// time. Never store its result in a struct field or package variable: that brings back
// "captured once at startup", and the stored client is closed after the next change.
type Live struct {
	c     atomic.Pointer[redis.Client]
	stale stale.State
}

// NewLive wraps an existing client. Module is the normal constructor; this exists for
// tests that bring their own client.
func NewLive(client *redis.Client) *Live {
	live := &Live{}
	live.c.Store(client)
	return live
}

// Client returns the current client.
func (l *Live) Client() *redis.Client { return l.c.Load() }

func (l *Live) swap(client *redis.Client) *redis.Client { return l.c.Swap(client) }

// Stale reports why the most recently pushed configuration is not in use; see pgpool.Live.Stale
// for the semantics and for why it must not fail health checks.
func (l *Live) Stale() error { return l.stale.Err() }

// Module provides a *Live built from the caller's configuration and rebuilds it when the
// projected Options change. A rebuild that fails keeps the current client.
func Module[T proto.Message](project Projector[T]) fx.Option {
	return fx.Module("redisclient",
		fx.Provide(func(lc fx.Lifecycle, conf T, live *config.Live[T], logger *zap.Logger) (*Live, error) {
			return newLive(lc, project, conf, live, logger, Build)
		}),
	)
}

type buildFunc func(context.Context, Options, *zap.Logger) (*redis.Client, error)

func newLive[T proto.Message](lc fx.Lifecycle, project Projector[T], conf T, live *config.Live[T], logger *zap.Logger, build buildFunc) (*Live, error) {
	applied := project(conf)
	client, err := build(context.Background(), applied, logger)
	if err != nil {
		return nil, err
	}
	holder := NewLive(client)
	if err := stale.Register("redisclient", &holder.stale); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("redisclient: %w", err)
	}

	// Compare against the options actually in use, so a failed rebuild is retried by the
	// next push that differs from what is running.
	var mu sync.Mutex
	unsubscribe := live.Subscribe(func(_, cur T) {
		next := project(cur)
		mu.Lock()
		defer mu.Unlock()
		if next == applied {
			// Also reached when a failed push is reverted: the client already matches it.
			holder.stale.Set(nil)
			return
		}
		logger.Info("redis config changed, rebuilding client", zap.String("addr", next.Addr))
		client, err := build(context.Background(), next, logger)
		if err != nil {
			logger.Error("rebuild redis client failed, keeping the current one", zap.Error(err))
			holder.stale.Set(fmt.Errorf("latest config not applied, previous client still in use: %w", err))
			return
		}
		prev := holder.swap(client)
		applied = next
		holder.stale.Set(nil)
		logger.Info("redis client rebuilt")
		if prev != nil {
			time.AfterFunc(drainTimeout, func() {
				if err := prev.Close(); err != nil {
					logger.Warn("closing the previous redis client failed", zap.Error(err))
				}
			})
		}
	})

	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		logger.Info("closing redis connection")
		unsubscribe()
		return holder.Client().Close()
	}})
	return holder, nil
}

// Build creates a client and returns it only after a successful ping.
func Build(ctx context.Context, options Options, logger *zap.Logger) (*redis.Client, error) {
	clientOptions, err := buildOptions(options)
	if err != nil {
		return nil, err
	}
	routeLibraryLogs(logger)
	if err := probe(ctx, clientOptions, options, logger); err != nil {
		return nil, err
	}

	// Metrics must be installed before the first client exists, or its pool gauges are missed.
	otelpkg.EnsureRedisInstrumentation(logger)
	client := redis.NewClient(clientOptions)
	logger.Info("redis connected", zap.String("addr", options.Addr), zap.Bool("tls", options.TLS.Enabled))
	return client, nil
}

// probe pings through a throwaway client that keeps no idle connections.
//
// redis.NewClient starts one background dial per MinIdleConns as soon as it is called, and
// each of them logs its own failure after retrying. Probing first means an unreachable
// server costs one dial failure instead of MinIdleConns+1, and the real client only fills
// its idle connections once the server is known to answer.
func probe(ctx context.Context, clientOptions *redis.Options, options Options, logger *zap.Logger) error {
	probeOptions := *clientOptions
	probeOptions.MinIdleConns = 0
	probeOptions.PoolSize = 1
	client := redis.NewClient(&probeOptions)
	defer func() {
		if err := client.Close(); err != nil {
			logger.Warn("closing redis probe client failed", zap.String("addr", options.Addr), zap.Error(err))
		}
	}()

	timeout := options.DialTimeout
	if timeout <= 0 {
		timeout = defaultPingTimeout
	}
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		return fmt.Errorf("redisclient: ping %s: %w", options.Addr, err)
	}
	return nil
}

var (
	libraryLogger     atomic.Pointer[zap.Logger]
	libraryLoggerOnce sync.Once
)

// routeLibraryLogs sends go-redis's own log lines (dial failures, pool errors) through zap.
// go-redis otherwise prints them to stderr with the standard log package, where level
// filtering, JSON format and the OTel log bridge do not apply. The go-redis logger is
// process-global, so the adapter is installed once and follows the latest Build's logger.
func routeLibraryLogs(logger *zap.Logger) {
	libraryLogger.Store(logger.With(zap.String("component", "go-redis")))
	libraryLoggerOnce.Do(func() { redis.SetLogger(zapLibraryLogger{}) })
}

type zapLibraryLogger struct{}

func (zapLibraryLogger) Printf(_ context.Context, format string, args ...any) {
	if logger := libraryLogger.Load(); logger != nil {
		logger.Warn(fmt.Sprintf(format, args...))
	}
}

// buildOptions turns options into go-redis options without touching the network.
func buildOptions(options Options) (*redis.Options, error) {
	clientOptions := &redis.Options{
		Addr:         options.Addr,
		Username:     options.Username,
		Password:     options.Password,
		DB:           options.DB,
		DialTimeout:  options.DialTimeout,
		ReadTimeout:  options.ReadTimeout,
		WriteTimeout: options.WriteTimeout,
		PoolSize:     options.PoolSize,
		MinIdleConns: options.MinIdleConns,
	}
	if !options.TLS.Enabled {
		return clientOptions, nil
	}

	// ServerName stays empty on purpose: go-redis dials with tls.DialWithDialer, which takes
	// it from Addr, so verification matches the host that was configured.
	tlsConfig := &tls.Config{InsecureSkipVerify: options.TLS.InsecureSkipVerify} //nolint:gosec // opt-in per configuration
	if options.TLS.CAPEM != "" {
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(options.TLS.CAPEM)) {
			return nil, errors.New("redisclient: CA PEM contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	clientOptions.TLSConfig = tlsConfig
	return clientOptions, nil
}
