package registry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/lens077/go-connect-kit/meta"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	EnvConsulEnabled = "CONSUL_ENABLED"

	defaultTTLPingInterval = 10 * time.Second
	defaultCheckInterval   = 10 * time.Second
	defaultCheckTimeout    = 12 * time.Second
	defaultRetryBase       = time.Second
	defaultRetryMax        = 30 * time.Second
	consulScheme           = "http"
	consulTLSScheme        = "https"
	consulTagFx            = "fx"
	consulTagTTL           = "ttl"
)

// ErrInvalidOptions marks registration failures caused by configuration rather
// than by Consul. They cannot heal on their own, so Maintain does not retry them.
var ErrInvalidOptions = errors.New("registry: invalid options")

// TLSOptions configures the Consul client transport.
type TLSOptions struct {
	Enabled            bool
	InsecureSkipVerify bool
	CAPEM              string
}

// TTLCheckOptions configures the process-liveness lease.
type TTLCheckOptions struct {
	Enabled      bool
	Duration     string
	PingInterval time.Duration
}

// GRPCCheckOptions configures optional deep readiness. Unlike the TTL lease, it
// never owns automatic deregistration.
type GRPCCheckOptions struct {
	Interval               time.Duration
	Timeout                time.Duration
	SuccessBeforePassing   int
	FailuresBeforeCritical int
}

// CheckOptions configures Consul health checks.
type CheckOptions struct {
	TTL                            TTLCheckOptions
	GRPC                           *GRPCCheckOptions
	DeregisterCriticalServiceAfter string
}

// Options is the provider-neutral service-registration configuration.
type Options struct {
	Enabled       bool
	Address       string
	ServerAddress string
	TLS           TLSOptions
	Check         CheckOptions
}

// ConsulRegistry owns one Consul registration and the loop that keeps it alive.
type ConsulRegistry struct {
	Addr       string
	ID         string
	Name       string
	client     *api.Client
	logger     *zap.Logger
	cancelPing context.CancelFunc
	registered atomic.Bool
	retryBase  time.Duration
	retryMax   time.Duration
}

type Option func(*clientOptions)

type clientOptions struct {
	logger    *zap.Logger
	tlsConfig *api.TLSConfig
	scheme    string
}

// WithLogger injects the registry logger.
func WithLogger(logger *zap.Logger) Option {
	return func(options *clientOptions) {
		options.logger = logger
	}
}

// WithTLS configures the Consul client TLS transport.
func WithTLS(insecureSkipVerify bool, caPEM string) Option {
	return func(options *clientOptions) {
		options.tlsConfig = &api.TLSConfig{
			CAPem:              []byte(caPEM),
			InsecureSkipVerify: insecureSkipVerify,
		}
	}
}

// Module provides an optional Consul registry and owns its lifecycle.
var Module = fx.Module("registry",
	fx.Provide(func(
		lc fx.Lifecycle,
		logger *zap.Logger,
		options Options,
		appInfo meta.AppInfo,
	) (*ConsulRegistry, error) {
		if os.Getenv(EnvConsulEnabled) == "false" {
			logger.Info("Consul disabled by environment variable", zap.String("env", EnvConsulEnabled))
			return nil, nil
		}
		if !options.Enabled || options.Address == "" {
			logger.Info("Consul not configured, service discovery disabled")
			return nil, nil
		}

		clientOptions := []Option{WithLogger(logger)}
		if options.TLS.Enabled {
			clientOptions = append(clientOptions, WithTLS(options.TLS.InsecureSkipVerify, options.TLS.CAPEM))
		}
		registry, err := NewConsulRegistry(options.Address, appInfo.ID, appInfo.Name, clientOptions...)
		if err != nil {
			logger.Warn("failed to initialize Consul client, service discovery disabled", zap.Error(err))
			return nil, nil
		}

		lc.Append(fx.Hook{
			OnStart: func(context.Context) error {
				ctx, cancel := context.WithCancel(context.Background())
				registry.cancelPing = cancel
				go registry.Maintain(ctx, options, appInfo)
				return nil
			},
			OnStop: func(context.Context) error {
				if registry.client == nil {
					return nil
				}
				if err := registry.Deregister(); err != nil {
					logger.Warn("failed to deregister from Consul", zap.Error(err))
				}
				return nil
			},
		})
		return registry, nil
	}),
)

// NewConsulRegistry constructs a lazy Consul API client.
func NewConsulRegistry(address, id, name string, opts ...Option) (*ConsulRegistry, error) {
	options := &clientOptions{scheme: consulScheme, logger: zap.NewNop()}
	for _, option := range opts {
		option(options)
	}

	config := api.Config{
		Address: address,
		Scheme:  options.scheme,
		Transport: &http.Transport{
			DialContext:         (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
			TLSHandshakeTimeout: 5 * time.Second,
		},
		WaitTime: 10 * time.Second,
	}
	if options.tlsConfig != nil {
		config.Scheme = consulTLSScheme
		config.TLSConfig = *options.tlsConfig
	}

	client, err := api.NewClient(&config)
	if err != nil {
		return nil, err
	}
	return &ConsulRegistry{
		Addr:      address,
		ID:        id,
		Name:      name,
		client:    client,
		logger:    options.logger,
		retryBase: defaultRetryBase,
		retryMax:  defaultRetryMax,
	}, nil
}

// Register installs the TTL liveness lease and optional gRPC readiness check.
func (r *ConsulRegistry) Register(options Options, info meta.AppInfo) error {
	r.logger.Debug("registering service to Consul", zap.String("id", r.ID))

	_, portText, err := net.SplitHostPort(options.ServerAddress)
	if err != nil {
		return fmt.Errorf("%w: server address: %v", ErrInvalidOptions, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("%w: invalid server port %q", ErrInvalidOptions, portText)
	}
	if !options.Check.TTL.Enabled {
		return fmt.Errorf("%w: consul TTL check is missing", ErrInvalidOptions)
	}
	if duration, err := time.ParseDuration(options.Check.TTL.Duration); err != nil || duration <= 0 {
		return fmt.Errorf("%w: invalid consul TTL duration %q", ErrInvalidOptions, options.Check.TTL.Duration)
	}
	if configured := options.Check.DeregisterCriticalServiceAfter; configured != "" {
		if duration, err := time.ParseDuration(configured); err != nil || duration <= 0 {
			return fmt.Errorf("%w: invalid deregistration delay %q", ErrInvalidOptions, configured)
		}
	}

	registration := &api.AgentServiceRegistration{
		ID:      r.ID,
		Name:    r.Name,
		Address: info.Host,
		Port:    port,
		Tags:    []string{info.Version, consulTagFx, consulTagTTL},
		Check: newConsulTTLCheck(
			r.ID,
			options.Check.TTL.Duration,
			options.Check.DeregisterCriticalServiceAfter,
		),
	}
	if options.Check.GRPC != nil {
		registration.Checks = api.AgentServiceChecks{
			newConsulGRPCCheck(r.ID, info.Host, port, *options.Check.GRPC),
		}
	}
	if err := r.client.Agent().ServiceRegister(registration); err != nil {
		return err
	}
	r.registered.Store(true)

	r.logger.Info("service registered with Consul",
		zap.String("id", r.ID),
		zap.String("ttl", options.Check.TTL.Duration),
		zap.Bool("grpc_readiness", options.Check.GRPC != nil))
	return nil
}

func newConsulTTLCheck(serviceID, ttl, deregisterAfter string) *api.AgentServiceCheck {
	return &api.AgentServiceCheck{
		CheckID:                        consulTTLCheckID(serviceID),
		Name:                           "TTL process liveness",
		TTL:                            ttl,
		DeregisterCriticalServiceAfter: deregisterAfter,
	}
}

func newConsulGRPCCheck(serviceID, host string, port int, options GRPCCheckOptions) *api.AgentServiceCheck {
	interval := options.Interval
	if interval <= 0 {
		interval = defaultCheckInterval
	}
	timeout := options.Timeout
	if timeout <= 0 {
		timeout = defaultCheckTimeout
	}
	successBeforePassing := options.SuccessBeforePassing
	if successBeforePassing <= 0 {
		successBeforePassing = 1
	}
	failuresBeforeCritical := options.FailuresBeforeCritical
	if failuresBeforeCritical <= 0 {
		failuresBeforeCritical = 3
	}
	return &api.AgentServiceCheck{
		CheckID:                fmt.Sprintf("service:%s:grpc-readiness", serviceID),
		Name:                   "gRPC deep readiness",
		GRPC:                   net.JoinHostPort(host, strconv.Itoa(port)),
		Interval:               interval.String(),
		Timeout:                timeout.String(),
		SuccessBeforePassing:   successBeforePassing,
		FailuresBeforeCritical: failuresBeforeCritical,
	}
}

func consulTTLCheckID(serviceID string) string {
	return fmt.Sprintf("service:%s", serviceID)
}

// Maintain keeps the registration alive until ctx is cancelled.
func (r *ConsulRegistry) Maintain(ctx context.Context, options Options, info meta.AppInfo) {
	base, maxDelay := r.retryBase, r.retryMax
	if base <= 0 {
		base = defaultRetryBase
	}
	if maxDelay < base {
		maxDelay = base
	}

	attempt := 0
	delay := base
	for {
		err := r.Register(options, info)
		if err == nil {
			if attempt > 0 {
				r.logger.Info("registered with Consul after retries", zap.Int("attempts", attempt+1))
			}
			attempt, delay = 0, base

			pingErr := r.TTLCheckPinger(ctx, options)
			if pingErr == nil {
				return
			}
			r.logger.Warn("Consul TTL heartbeat failed; re-registering", zap.Error(pingErr))
			err = pingErr
		} else if errors.Is(err, ErrInvalidOptions) {
			r.logger.Error("Consul registration options are invalid; not retrying", zap.Error(err))
			return
		}

		attempt++
		r.logger.Warn("Consul registration not established; will retry",
			zap.Error(err), zap.Int("attempt", attempt), zap.Duration("retry_in", delay))
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if delay < maxDelay {
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
		}
	}
}

// TTLCheckPinger marks the TTL check as passing on every interval.
func (r *ConsulRegistry) TTLCheckPinger(ctx context.Context, options Options) error {
	interval := options.Check.TTL.PingInterval
	if interval <= 0 {
		interval = defaultTTLPingInterval
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	checkID := consulTTLCheckID(r.ID)

	ping := func() error {
		return r.client.Agent().UpdateTTL(checkID, "ttl check passing", api.HealthPassing)
	}

	r.logger.Info("starting ttl pinger", zap.Duration("interval", interval), zap.String("checkID", checkID))
	if err := ping(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			r.logger.Info("ttl pinger stopped gracefully")
			return nil
		case <-ticker.C:
			if err := ping(); err != nil {
				return err
			}
		}
	}
}

// Deregister stops the maintenance loop before removing the service registration.
func (r *ConsulRegistry) Deregister() error {
	if r.cancelPing != nil {
		r.cancelPing()
	}
	if !r.registered.Load() {
		r.logger.Info("skipping Consul deregistration; never registered", zap.String("id", r.ID))
		return nil
	}
	r.logger.Info("deregistering service from Consul", zap.String("id", r.ID))
	if err := r.client.Agent().ServiceDeregister(r.ID); err != nil {
		return err
	}
	r.registered.Store(false)
	return nil
}
