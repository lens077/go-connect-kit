// Package pgpool builds pgx connection pools and keeps one hot-swappable pool per process.
//
// Every consumer used to carry its own copy of this code (buildPgPool + a PgPool holder);
// the copies had started to diverge before they were collected here. Consumers now only
// map their configuration message to Options.
package pgpool

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lens077/go-connect-kit/config"
	"github.com/lens077/go-connect-kit/internal/stale"
	otelpkg "github.com/lens077/go-connect-kit/otel"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// SSL modes follow libpq: https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION
const (
	SSLModeDisable    = "disable"
	SSLModeAllow      = "allow"
	SSLModePrefer     = "prefer"
	SSLModeRequire    = "require"
	SSLModeVerifyCA   = "verify-ca"
	SSLModeVerifyFull = "verify-full"
)

const (
	// drainTimeout is how long a replaced pool stays open so in-flight queries can finish.
	drainTimeout = 30 * time.Second
	// defaultPingTimeout bounds the startup ping when PingTimeout is unset. Without it a
	// zero timeout would fail the ping immediately.
	defaultPingTimeout = 5 * time.Second
)

// Options is the provider-neutral PostgreSQL pool configuration.
//
// Zero values keep pgx defaults (port 5432, pool sizes and lifetimes chosen by pgxpool)
// instead of overriding them with zero.
type Options struct {
	Host     string
	Port     uint16
	Database string
	User     string
	Password string
	// Timezone is sent as the session TimeZone runtime parameter when non-empty.
	Timezone string

	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	PingTimeout     time.Duration

	// SSLMode is one of the libpq modes above; empty means prefer, as in libpq.
	SSLMode string
	// CAPEM is the PEM-encoded CA bundle. Empty means the system roots for the verify modes.
	CAPEM string

	// TypeNames are loaded and registered on every new connection. Arrays of custom enum
	// types need this: pgx falls back to text for an unknown scalar OID but has no fallback
	// for arrays, and fails with "unable to encode ... for unknown type (OID n)".
	// Include both the type and its array type, e.g. "cart.cart_type", "cart._cart_type".
	TypeNames []string
}

func (o Options) equal(other Options) bool {
	// nil and empty TypeNames mean the same thing; DeepEqual alone would call them different.
	if len(o.TypeNames) == 0 {
		o.TypeNames = nil
	}
	if len(other.TypeNames) == 0 {
		other.TypeNames = nil
	}
	return reflect.DeepEqual(o, other)
}

// Projector maps a caller-owned configuration message into pool Options.
type Projector[T proto.Message] func(T) Options

// Live holds the current pool and lets it be replaced as a whole.
//
// It implements the Exec/Query/QueryRow triple that sqlc's DBTX interface requires, so
// Queries built on a Live keep working after the pool underneath is replaced. It also
// implements otelpgx.PoolStats, so pool metrics are registered once and follow the
// current pool; otelpgx has no way to unregister, and registering per pool would
// report duplicates.
type Live struct {
	p     atomic.Pointer[pgxpool.Pool]
	stale stale.State
}

// NewLive wraps an existing pool. Module is the normal constructor; this exists for
// tests that bring their own pool.
func NewLive(pool *pgxpool.Pool) *Live {
	live := &Live{}
	live.p.Store(pool)
	return live
}

// Pool returns the current pool for callers that need it directly (transactions, Ping).
// Do not keep the result: it is closed some time after a configuration change.
func (l *Live) Pool() *pgxpool.Pool { return l.p.Load() }

func (l *Live) swap(pool *pgxpool.Pool) *pgxpool.Pool { return l.p.Swap(pool) }

// Stale reports why the most recently pushed configuration is not in use: rebuilding with it
// failed and the previous pool is still serving. It is nil once the pool matches the latest
// push again, through a successful rebuild or a push back to the configuration in use.
//
// Report it next to health checks, not as a failed check. Every replica receives the same
// push and fails the same way, so failing readiness would drain all of them while the old
// pool still works, and failing liveness would restart them into the bad configuration.
func (l *Live) Stale() error { return l.stale.Err() }

// Exec implements sqlc's DBTX against the current pool.
func (l *Live) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return l.p.Load().Exec(ctx, sql, args...)
}

// Query implements sqlc's DBTX against the current pool.
func (l *Live) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return l.p.Load().Query(ctx, sql, args...)
}

// QueryRow implements sqlc's DBTX against the current pool.
func (l *Live) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return l.p.Load().QueryRow(ctx, sql, args...)
}

// Stat implements otelpgx.PoolStats against the current pool.
func (l *Live) Stat() *pgxpool.Stat { return l.p.Load().Stat() }

// Config implements otelpgx.PoolStats against the current pool.
func (l *Live) Config() *pgxpool.Config { return l.p.Load().Config() }

// Module provides a *Live built from the caller's configuration and rebuilds it when the
// projected Options change. A rebuild that fails keeps the current pool: one bad
// configuration push must not take down traffic that is already being served.
func Module[T proto.Message](project Projector[T]) fx.Option {
	return fx.Module("pgpool",
		fx.Provide(func(lc fx.Lifecycle, conf T, live *config.Live[T], logger *zap.Logger) (*Live, error) {
			return newLive(lc, project, conf, live, logger, Build)
		}),
	)
}

type buildFunc func(context.Context, Options, *zap.Logger) (*pgxpool.Pool, error)

func newLive[T proto.Message](lc fx.Lifecycle, project Projector[T], conf T, live *config.Live[T], logger *zap.Logger, build buildFunc) (*Live, error) {
	applied := project(conf)
	pool, err := build(context.Background(), applied, logger)
	if err != nil {
		return nil, err
	}
	holder := NewLive(pool)
	if err := otelpgx.RecordStats(holder); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgpool: record pool stats: %w", err)
	}
	if err := stale.Register("pgpool", &holder.stale); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgpool: %w", err)
	}

	// Compare against the options actually in use rather than the previous snapshot, so a
	// failed rebuild is retried by the next push that differs from what is running.
	var mu sync.Mutex
	unsubscribe := live.Subscribe(func(_, cur T) {
		next := project(cur)
		mu.Lock()
		defer mu.Unlock()
		if next.equal(applied) {
			// Also reached when a failed push is reverted: the pool already matches it.
			holder.stale.Set(nil)
			return
		}
		logger.Info("database config changed, rebuilding pool", zap.String("host", next.Host))
		pool, err := build(context.Background(), next, logger)
		if err != nil {
			logger.Error("rebuild database pool failed, keeping the current one", zap.Error(err))
			holder.stale.Set(fmt.Errorf("latest config not applied, previous pool still in use: %w", err))
			return
		}
		// Swap only after the new pool answered a ping, so the visible pool always works.
		prev := holder.swap(pool)
		applied = next
		holder.stale.Set(nil)
		logger.Info("database pool rebuilt")
		if prev != nil {
			time.AfterFunc(drainTimeout, prev.Close)
		}
	})

	lc.Append(fx.Hook{OnStop: func(context.Context) error {
		logger.Info("closing database connection")
		unsubscribe()
		holder.Pool().Close()
		return nil
	}})
	return holder, nil
}

// Build creates a pool from options and returns it only after a successful ping.
// On failure it closes whatever it created: on the rebuild path nobody else would.
func Build(ctx context.Context, options Options, logger *zap.Logger) (*pgxpool.Pool, error) {
	poolConfig, err := buildConfig(options)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("pgpool: create pool for %s: %w", options.Host, err)
	}

	timeout := options.PingTimeout
	if timeout <= 0 {
		timeout = defaultPingTimeout
	}
	pingCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("pgpool: ping %s: %w", options.Host, err)
	}

	logger.Info("database connected",
		zap.String("host", options.Host),
		zap.String("database", options.Database),
		zap.String("ssl_mode", sslMode(options)),
	)
	return pool, nil
}

// buildConfig turns options into a pool config without touching the network.
func buildConfig(options Options) (*pgxpool.Config, error) {
	// ParseConfig("") is required as the base: pgx only accepts configs it created. It also
	// derives a host, sslmode=prefer and matching Fallbacks from PG* environment defaults.
	// Every one of those is replaced below. Leaving Fallbacks in place was the bug the
	// per-service copies shared: a failed connection to the configured host retried the
	// default host (localhost or a local socket) in plaintext.
	poolConfig, err := pgxpool.ParseConfig("")
	if err != nil {
		return nil, fmt.Errorf("pgpool: parse base config: %w", err)
	}

	conn := poolConfig.ConnConfig
	conn.Host = options.Host
	if options.Port != 0 {
		conn.Port = options.Port
	}
	conn.Database = options.Database
	conn.User = options.User
	conn.Password = options.Password
	if options.Timezone != "" {
		if conn.RuntimeParams == nil {
			conn.RuntimeParams = map[string]string{}
		}
		conn.RuntimeParams["timezone"] = options.Timezone
	}

	primary, fallbacks, err := tlsConfigs(options, conn.Port)
	if err != nil {
		return nil, err
	}
	conn.TLSConfig = primary
	conn.Fallbacks = fallbacks

	if options.MaxConns > 0 {
		poolConfig.MaxConns = options.MaxConns
	}
	if options.MinConns > 0 {
		poolConfig.MinConns = options.MinConns
	}
	if options.MaxConnLifetime > 0 {
		poolConfig.MaxConnLifetime = options.MaxConnLifetime
	}
	if options.MaxConnIdleTime > 0 {
		poolConfig.MaxConnIdleTime = options.MaxConnIdleTime
	}
	if options.PingTimeout > 0 {
		poolConfig.PingTimeout = options.PingTimeout
	}

	// Span names come from the sqlc query name, never the SQL text: span names are an
	// index dimension, and full statements would explode their cardinality. The statement
	// stays on the db.statement attribute. Both options are required — otelpgx only calls
	// the span-name function when WithTrimSQLInSpanName is set.
	conn.Tracer = otelpgx.NewTracer(
		otelpgx.WithTrimSQLInSpanName(),
		otelpgx.WithSpanNameFunc(otelpkg.SQLSpanName),
	)

	if len(options.TypeNames) > 0 {
		typeNames := slices.Clone(options.TypeNames)
		poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			types, err := conn.LoadTypes(ctx, typeNames)
			if err != nil {
				return fmt.Errorf("pgpool: load types %v: %w", typeNames, err)
			}
			conn.TypeMap().RegisterTypes(types)
			return nil
		}
	}
	return poolConfig, nil
}

func sslMode(options Options) string {
	if options.SSLMode == "" {
		return SSLModePrefer
	}
	return options.SSLMode
}

// tlsConfigs returns the TLS config for the first attempt and, for allow/prefer only, one
// second attempt against the same host. Fallbacks never point at another host.
func tlsConfigs(options Options, port uint16) (*tls.Config, []*pgconn.FallbackConfig, error) {
	var roots *x509.CertPool
	if options.CAPEM != "" {
		roots = x509.NewCertPool()
		if !roots.AppendCertsFromPEM([]byte(options.CAPEM)) {
			return nil, nil, errors.New("pgpool: CA PEM contains no certificates")
		}
	}

	// SNI lets TLS passthrough proxies route by host name; libpq sends it for host names only.
	serverName := ""
	if net.ParseIP(options.Host) == nil {
		serverName = options.Host
	}
	unverified := func() *tls.Config {
		return &tls.Config{ServerName: serverName, InsecureSkipVerify: true} //nolint:gosec // libpq semantics for allow/prefer/require
	}
	sameHost := func(config *tls.Config) []*pgconn.FallbackConfig {
		return []*pgconn.FallbackConfig{{Host: options.Host, Port: port, TLSConfig: config}}
	}

	mode := sslMode(options)
	// libpq: with a root certificate available, require behaves like verify-ca.
	if mode == SSLModeRequire && roots != nil {
		mode = SSLModeVerifyCA
	}

	switch mode {
	case SSLModeDisable:
		return nil, nil, nil
	case SSLModeAllow:
		return nil, sameHost(unverified()), nil
	case SSLModePrefer:
		return unverified(), sameHost(nil), nil
	case SSLModeRequire:
		return unverified(), nil, nil
	case SSLModeVerifyCA:
		return verifyCAConfig(serverName, roots), nil, nil
	case SSLModeVerifyFull:
		return &tls.Config{ServerName: options.Host, RootCAs: roots}, nil, nil
	default:
		return nil, nil, fmt.Errorf("pgpool: unknown ssl mode %q", options.SSLMode)
	}
}

// verifyCAConfig verifies the certificate chain but not the host name. crypto/tls has no
// such mode, so the default verification is disabled and the chain is checked here.
// roots == nil verifies against the system pool.
func verifyCAConfig(serverName string, roots *x509.CertPool) *tls.Config {
	return &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: true, //nolint:gosec // chain is verified in VerifyPeerCertificate
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			if len(rawCerts) == 0 {
				return errors.New("pgpool: server presented no certificate")
			}
			leaf, err := x509.ParseCertificate(rawCerts[0])
			if err != nil {
				return err
			}
			intermediates := x509.NewCertPool()
			for _, raw := range rawCerts[1:] {
				if cert, err := x509.ParseCertificate(raw); err == nil {
					intermediates.AddCert(cert)
				}
			}
			_, err = leaf.Verify(x509.VerifyOptions{Roots: roots, Intermediates: intermediates})
			return err
		},
	}
}
