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
type Live struct{ c atomic.Pointer[redis.Client] }

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

// Module provides a *Live built from the caller's configuration and rebuilds it when the
// projected Options change. A rebuild that fails keeps the current client.
func Module[T proto.Message](project Projector[T]) fx.Option {
	return fx.Module("redisclient",
		fx.Provide(func(lc fx.Lifecycle, conf T, live *config.Live[T], logger *zap.Logger) (*Live, error) {
			return newLive(lc, project, conf, live, logger)
		}),
	)
}

func newLive[T proto.Message](lc fx.Lifecycle, project Projector[T], conf T, live *config.Live[T], logger *zap.Logger) (*Live, error) {
	applied := project(conf)
	client, err := Build(context.Background(), applied, logger)
	if err != nil {
		return nil, err
	}
	holder := NewLive(client)

	// Compare against the options actually in use, so a failed rebuild is retried by the
	// next push that differs from what is running.
	var mu sync.Mutex
	unsubscribe := live.Subscribe(func(_, cur T) {
		next := project(cur)
		mu.Lock()
		defer mu.Unlock()
		if next == applied {
			return
		}
		logger.Info("redis config changed, rebuilding client", zap.String("addr", next.Addr))
		client, err := Build(context.Background(), next, logger)
		if err != nil {
			logger.Error("rebuild redis client failed, keeping the current one", zap.Error(err))
			return
		}
		prev := holder.swap(client)
		applied = next
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
	// Metrics must be installed before the first client exists, or its pool gauges are missed.
	otelpkg.EnsureRedisInstrumentation(logger)
	client := redis.NewClient(clientOptions)

	timeout := options.DialTimeout
	if timeout <= 0 {
		timeout = defaultPingTimeout
	}
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := client.Ping(pingCtx).Err(); err != nil {
		if closeErr := client.Close(); closeErr != nil {
			logger.Warn("closing redis client after ping failure failed", zap.String("addr", options.Addr), zap.Error(closeErr))
		}
		return nil, fmt.Errorf("redisclient: ping %s: %w", options.Addr, err)
	}

	logger.Info("redis connected", zap.String("addr", options.Addr), zap.Bool("tls", options.TLS.Enabled))
	return client, nil
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
