package registry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/lens077/go-connect-kit/meta"
	"go.uber.org/fx"
	"go.uber.org/zap"
)

const (
	// EnvConsulEnabled is kept for source compatibility only.
	// Deprecated: map deployment environment policy into Options.Enabled in the consumer adapter.
	EnvConsulEnabled = "CONSUL_ENABLED"

	defaultTTLPingInterval = 10 * time.Second
	defaultCheckInterval   = 10 * time.Second
	defaultCheckTimeout    = 12 * time.Second
	defaultRequestTimeout  = 15 * time.Second
	defaultDeregisterAfter = time.Minute
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
	Enabled  bool
	Duration string
	// PingInterval defaults to 10 seconds and must be shorter than Duration.
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
	TTL  TTLCheckOptions
	GRPC *GRPCCheckOptions
	// DeregisterCriticalServiceAfter defaults to one minute so an unowned TTL registration expires.
	DeregisterCriticalServiceAfter string
}

// Options is the provider-neutral service-registration configuration.
type Options struct {
	// Enabled is authoritative; deployment environment policy belongs to the caller's adapter.
	Enabled       bool
	Address       string
	ServerAddress string
	TLS           TLSOptions
	Check         CheckOptions
}

// ConsulRegistry owns one Consul registration and the loop that keeps it alive.
type ConsulRegistry struct {
	Addr   string
	ID     string
	Name   string
	client *api.Client
	logger *zap.Logger

	lifecycleMu       sync.Mutex
	cancelMaintain    context.CancelFunc
	maintainDone      <-chan struct{}
	deregisterGate    chan struct{}
	registerAttempted atomic.Bool
	registered        atomic.Bool
	retryBase         time.Duration
	retryMax          time.Duration
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

// Module initializes the optional Consul registry eagerly and owns its lifecycle.
var Module = fx.Module("registry",
	fx.Provide(func(
		lc fx.Lifecycle,
		logger *zap.Logger,
		options Options,
		appInfo meta.AppInfo,
	) (*ConsulRegistry, error) {
		if !options.Enabled {
			logger.Info("Consul service registration disabled")
			return nil, nil
		}
		if options.Address == "" {
			return nil, fmt.Errorf("%w: consul address is empty", ErrInvalidOptions)
		}
		normalized, _, err := validateAndNormalizeOptions(options)
		if err != nil {
			return nil, err
		}
		options = normalized

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
				done := make(chan struct{})
				registry.lifecycleMu.Lock()
				registry.cancelMaintain = cancel
				registry.maintainDone = done
				registry.lifecycleMu.Unlock()
				go func() {
					defer close(done)
					registry.Maintain(ctx, options, appInfo)
				}()
				return nil
			},
			OnStop: func(ctx context.Context) error {
				if err := registry.Shutdown(ctx); err != nil {
					logger.Warn("failed to shut down Consul registration", zap.Error(err))
				}
				return nil
			},
		})
		return registry, nil
	}),
	fx.Invoke(func(_ *ConsulRegistry) {}),
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
	config.HttpClient.Timeout = defaultRequestTimeout
	return &ConsulRegistry{
		Addr:           address,
		ID:             id,
		Name:           name,
		client:         client,
		logger:         options.logger,
		deregisterGate: make(chan struct{}, 1),
		retryBase:      defaultRetryBase,
		retryMax:       defaultRetryMax,
	}, nil
}

func validateAndNormalizeOptions(options Options) (Options, int, error) {
	_, portText, err := net.SplitHostPort(options.ServerAddress)
	if err != nil {
		return options, 0, fmt.Errorf("%w: server address: %v", ErrInvalidOptions, err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return options, 0, fmt.Errorf("%w: invalid server port %q", ErrInvalidOptions, portText)
	}
	if !options.Check.TTL.Enabled {
		return options, 0, fmt.Errorf("%w: consul TTL check is missing", ErrInvalidOptions)
	}
	ttlDuration, err := time.ParseDuration(options.Check.TTL.Duration)
	if err != nil || ttlDuration <= 0 {
		return options, 0, fmt.Errorf("%w: invalid consul TTL duration %q", ErrInvalidOptions, options.Check.TTL.Duration)
	}
	pingInterval := ttlPingInterval(options.Check.TTL)
	if pingInterval >= ttlDuration {
		return options, 0, fmt.Errorf("%w: consul TTL ping interval %s must be shorter than TTL %s", ErrInvalidOptions, pingInterval, ttlDuration)
	}

	if options.Check.DeregisterCriticalServiceAfter == "" {
		options.Check.DeregisterCriticalServiceAfter = defaultDeregisterAfter.String()
	}
	deregisterAfter := options.Check.DeregisterCriticalServiceAfter
	if duration, err := time.ParseDuration(deregisterAfter); err != nil || duration <= 0 {
		return options, 0, fmt.Errorf("%w: invalid deregistration delay %q", ErrInvalidOptions, deregisterAfter)
	}
	return options, port, nil
}

// Register installs the TTL liveness lease and optional gRPC readiness check.
func (r *ConsulRegistry) Register(options Options, info meta.AppInfo) error {
	return r.RegisterContext(context.Background(), options, info)
}

// RegisterContext is Register with cancellation propagated to Consul.
func (r *ConsulRegistry) RegisterContext(ctx context.Context, options Options, info meta.AppInfo) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.logger.Debug("registering service to Consul", zap.String("id", r.ID))

	normalized, port, err := validateAndNormalizeOptions(options)
	if err != nil {
		return err
	}
	options = normalized

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
	r.registerAttempted.Store(true)
	// Re-registration must also remove checks omitted by the new profile.
	registerOptions := api.ServiceRegisterOpts{ReplaceExistingChecks: true}.WithContext(ctx)
	if err := r.client.Agent().ServiceRegisterOpts(registration, registerOptions); err != nil {
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
	// A canceled HTTP registration may still commit remotely; keep it unroutable
	// until this process proves liveness with its first TTL update.
	return &api.AgentServiceCheck{
		CheckID:                        consulTTLCheckID(serviceID),
		Name:                           "TTL process liveness",
		TTL:                            ttl,
		Status:                         api.HealthCritical,
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
		err := r.RegisterContext(ctx, options, info)
		if err == nil {
			if attempt > 0 {
				r.logger.Info("registered with Consul after retries", zap.Int("attempts", attempt+1))
			}
			attempt, delay = 0, base

			pingErr := r.TTLCheckPinger(ctx, options)
			if pingErr == nil || ctx.Err() != nil {
				return
			}
			r.logger.Warn("Consul TTL heartbeat failed; re-registering", zap.Error(pingErr))
			err = pingErr
		} else if errors.Is(err, ErrInvalidOptions) {
			r.logger.Error("Consul registration options are invalid; not retrying", zap.Error(err))
			return
		}
		if ctx.Err() != nil {
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

func ttlPingInterval(options TTLCheckOptions) time.Duration {
	if options.PingInterval <= 0 {
		return defaultTTLPingInterval
	}
	return options.PingInterval
}

// TTLCheckPinger marks the TTL check as passing on every interval.
func (r *ConsulRegistry) TTLCheckPinger(ctx context.Context, options Options) error {
	interval := ttlPingInterval(options.Check.TTL)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	checkID := consulTTLCheckID(r.ID)

	ping := func() error {
		query := (&api.QueryOptions{}).WithContext(ctx)
		return r.client.Agent().UpdateTTLOpts(checkID, "ttl check passing", api.HealthPassing, query)
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

// Shutdown stops and joins the maintenance loop before removing the registration.
func (r *ConsulRegistry) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}

	r.lifecycleMu.Lock()
	cancel := r.cancelMaintain
	done := r.maintainDone
	r.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return r.deregisterContext(ctx)
}

// Deregister is the context-free compatibility wrapper around Shutdown.
func (r *ConsulRegistry) Deregister() error {
	return r.Shutdown(context.Background())
}

func (r *ConsulRegistry) deregisterContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case r.deregisterGate <- struct{}{}:
		defer func() { <-r.deregisterGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if !r.registerAttempted.Load() && !r.registered.Load() {
		r.logger.Info("skipping Consul deregistration; never attempted", zap.String("id", r.ID))
		return nil
	}

	r.logger.Info("deregistering service from Consul", zap.String("id", r.ID))
	query := (&api.QueryOptions{}).WithContext(ctx)
	if err := r.client.Agent().ServiceDeregisterOpts(r.ID, query); err != nil {
		return err
	}
	r.registerAttempted.Store(false)
	r.registered.Store(false)
	return nil
}
