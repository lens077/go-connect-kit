package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/lens077/go-connect-kit/meta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap/zaptest"
)

var testAppInfo = meta.AppInfo{
	ID:          "registry-test-id",
	Name:        "registry-test-service",
	Host:        "10.0.0.7",
	Environment: "dev",
	Version:     "v1.2.3",
}

func testOptions(serverAddress string, pingInterval time.Duration) Options {
	return Options{
		Enabled:       true,
		ServerAddress: serverAddress,
		Check: CheckOptions{
			TTL: TTLCheckOptions{
				Enabled:      true,
				Duration:     "30s",
				PingInterval: pingInterval,
			},
			GRPC:                           &GRPCCheckOptions{Interval: pingInterval},
			DeregisterCriticalServiceAfter: "1m",
		},
	}
}

type fakeConsulAgent struct {
	address string

	mu           sync.Mutex
	registered   *api.AgentServiceRegistration
	checkIDs     map[string]struct{}
	ttlCheckIDs  []string
	deregistered []string

	// rejectRegistrations makes the next N register calls answer 503,
	// simulating a Consul that is up but not yet serving (leader election,
	// CNI policy not programmed) — the failure mode seen on cluster reboot.
	rejectRegistrations   int
	registerCalls         int
	replaceExistingChecks bool
}

func (agent *fakeConsulAgent) Registered() *api.AgentServiceRegistration {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	return agent.registered
}

func (agent *fakeConsulAgent) RegisterCalls() int {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	return agent.registerCalls
}

func (agent *fakeConsulAgent) ReplaceExistingChecks() bool {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	return agent.replaceExistingChecks
}

// Forget simulates a Consul restart that lost agent-local registrations:
// the service and its checks vanish, so the next TTL update answers 404.
func (agent *fakeConsulAgent) Forget() {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	agent.registered = nil
	agent.checkIDs = nil
}

func (agent *fakeConsulAgent) TTLUpdates() []string {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	return append([]string(nil), agent.ttlCheckIDs...)
}

func (agent *fakeConsulAgent) Deregistered() []string {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	return append([]string(nil), agent.deregistered...)
}

func startFakeConsulAgent(t *testing.T) *fakeConsulAgent {
	t.Helper()
	agent := &fakeConsulAgent{}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/v1/agent/service/register":
			agent.mu.Lock()
			agent.registerCalls++
			agent.replaceExistingChecks = request.URL.Query().Get("replace-existing-checks") == "true"
			reject := agent.rejectRegistrations > 0
			if reject {
				agent.rejectRegistrations--
			}
			agent.mu.Unlock()
			if reject {
				writer.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			registration := &api.AgentServiceRegistration{}
			if err := json.NewDecoder(request.Body).Decode(registration); err != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			checks := make(api.AgentServiceChecks, 0, 1+len(registration.Checks))
			if registration.Check != nil {
				checks = append(checks, registration.Check)
			}
			checks = append(checks, registration.Checks...)
			checkIDs := make(map[string]struct{}, len(checks))
			for index, check := range checks {
				checkID := check.CheckID
				if checkID == "" {
					checkID = "service:" + registration.ID
					if len(checks) > 1 {
						checkID += fmt.Sprintf(":%d", index+1)
					}
				}
				checkIDs[checkID] = struct{}{}
			}
			agent.mu.Lock()
			agent.registered = registration
			agent.checkIDs = checkIDs
			agent.mu.Unlock()
		case strings.HasPrefix(request.URL.Path, "/v1/agent/check/update/"):
			checkID := strings.TrimPrefix(request.URL.Path, "/v1/agent/check/update/")
			agent.mu.Lock()
			_, exists := agent.checkIDs[checkID]
			if exists {
				agent.ttlCheckIDs = append(agent.ttlCheckIDs, checkID)
			}
			agent.mu.Unlock()
			if !exists {
				writer.WriteHeader(http.StatusNotFound)
			}
		case strings.HasPrefix(request.URL.Path, "/v1/agent/service/deregister/"):
			agent.mu.Lock()
			agent.deregistered = append(agent.deregistered,
				strings.TrimPrefix(request.URL.Path, "/v1/agent/service/deregister/"))
			agent.mu.Unlock()
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(server.Close)
	agent.address = strings.TrimPrefix(server.URL, "http://")
	return agent
}

func newRegistry(t *testing.T, address string) *ConsulRegistry {
	t.Helper()
	registry, err := NewConsulRegistry(address, testAppInfo.ID, testAppInfo.Name, WithLogger(zaptest.NewLogger(t)))
	require.NoError(t, err)
	return registry
}

func TestNewConsulRegistry(t *testing.T) {
	registry, err := NewConsulRegistry("localhost:8500", "test-id", "test-service", WithLogger(zaptest.NewLogger(t)))
	require.NoError(t, err)
	assert.Equal(t, "test-id", registry.ID)
	assert.Equal(t, "test-service", registry.Name)
	assert.Equal(t, "localhost:8500", registry.Addr)
}

func TestNewConsulRegistryTLS(t *testing.T) {
	registry, err := NewConsulRegistry("localhost:8500", "test-id", "test-service",
		WithLogger(zaptest.NewLogger(t)), WithTLS(true, ""))
	require.NoError(t, err)
	assert.NotNil(t, registry)

	registry, err = NewConsulRegistry("localhost:8500", "test-id", "test-service",
		WithLogger(zaptest.NewLogger(t)), WithTLS(false, "not-a-pem"))
	assert.Nil(t, registry)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PEM")
}

func moduleApp(t *testing.T, options Options, output **ConsulRegistry) *fxtest.App {
	t.Helper()
	return fxtest.New(t,
		fx.NopLogger,
		fx.Supply(options, testAppInfo, zaptest.NewLogger(t)),
		Module,
		fx.Populate(output),
	)
}

func TestModuleOwnsRegistrationLifecycle(t *testing.T) {
	agent := startFakeConsulAgent(t)
	options := testOptions("0.0.0.0:30006", 5*time.Millisecond)
	options.Address = agent.address

	var registry *ConsulRegistry
	app := moduleApp(t, options, &registry)
	app.RequireStart()
	require.NotNil(t, registry)
	// Registration is owned by a background loop now (fail-open), so it lands
	// shortly after OnStart rather than inside it.
	require.Eventually(t, func() bool { return agent.Registered() != nil }, time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return len(agent.TTLUpdates()) > 0 }, time.Second, 5*time.Millisecond)
	app.RequireStop()
	assert.Equal(t, []string{testAppInfo.ID}, agent.Deregistered())
}

func TestModuleStartsWithoutOutputConsumer(t *testing.T) {
	agent := startFakeConsulAgent(t)
	options := testOptions("0.0.0.0:30006", 5*time.Millisecond)
	options.Address = agent.address

	app := fxtest.New(t,
		fx.NopLogger,
		fx.Supply(options, testAppInfo, zaptest.NewLogger(t)),
		Module,
	)
	app.RequireStart()
	require.Eventually(t, func() bool { return agent.Registered() != nil }, time.Second, 5*time.Millisecond)
	app.RequireStop()
}

func TestModuleCanDisableRegistry(t *testing.T) {
	var registry *ConsulRegistry
	app := fxtest.New(t,
		fx.NopLogger,
		fx.Supply(Options{}, testAppInfo, zaptest.NewLogger(t)),
		Module,
		fx.Populate(&registry),
	)
	require.NoError(t, app.Err())
	assert.Nil(t, registry)
	app.RequireStart()
	app.RequireStop()
}

func TestModuleRejectsEnabledRegistryWithoutAddress(t *testing.T) {
	options := testOptions("0.0.0.0:30006", time.Second)
	app := fx.New(
		fx.NopLogger,
		fx.Supply(options, testAppInfo, zaptest.NewLogger(t)),
		Module,
	)
	require.ErrorIs(t, app.Err(), ErrInvalidOptions)
}

func TestRegisterUsesProviderNeutralOptions(t *testing.T) {
	agent := startFakeConsulAgent(t)
	registry := newRegistry(t, agent.address)
	options := testOptions("0.0.0.0:30006", time.Second)
	require.NoError(t, registry.Register(options, testAppInfo))
	assert.True(t, agent.ReplaceExistingChecks())

	registration := agent.Registered()
	require.NotNil(t, registration)
	assert.Equal(t, testAppInfo.ID, registration.ID)
	assert.Equal(t, testAppInfo.Name, registration.Name)
	assert.Equal(t, testAppInfo.Host, registration.Address)
	assert.Equal(t, 30006, registration.Port)
	assert.ElementsMatch(t, []string{testAppInfo.Version, consulTagFx, consulTagTTL}, registration.Tags)
	require.NotNil(t, registration.Check)
	assert.Equal(t, "service:"+testAppInfo.ID, registration.Check.CheckID)
	assert.Equal(t, "30s", registration.Check.TTL)
	assert.Equal(t, api.HealthCritical, registration.Check.Status)
	assert.Equal(t, "1m", registration.Check.DeregisterCriticalServiceAfter)
	require.Len(t, registration.Checks, 1)
	assert.Equal(t, "service:"+testAppInfo.ID+":grpc-readiness", registration.Checks[0].CheckID)
	assert.Equal(t, testAppInfo.Host+":30006", registration.Checks[0].GRPC)
	assert.Equal(t, "1s", registration.Checks[0].Interval)
}

func TestRegisterAllowsTTLOnly(t *testing.T) {
	agent := startFakeConsulAgent(t)
	registry := newRegistry(t, agent.address)
	options := testOptions("0.0.0.0:30010", time.Second)
	options.Check.GRPC = nil

	require.NoError(t, registry.Register(options, testAppInfo))
	registration := agent.Registered()
	require.NotNil(t, registration)
	require.NotNil(t, registration.Check)
	assert.Empty(t, registration.Checks)
}

func TestRegisterRejectsInvalidServerAndMissingTTL(t *testing.T) {
	agent := startFakeConsulAgent(t)
	registry := newRegistry(t, agent.address)

	for _, address := range []string{"", "0.0.0.0", "0.0.0.0:http"} {
		options := testOptions(address, time.Second)
		require.Error(t, registry.Register(options, testAppInfo))
	}

	options := testOptions("0.0.0.0:30006", time.Second)
	options.Check.TTL.Enabled = false
	require.ErrorIs(t, registry.Register(options, testAppInfo), ErrInvalidOptions)

	options = testOptions("0.0.0.0:30006", time.Second)
	options.Check.TTL.Duration = "not-a-duration"
	require.ErrorIs(t, registry.Register(options, testAppInfo), ErrInvalidOptions)

	options = testOptions("0.0.0.0:30006", time.Second)
	options.Check.DeregisterCriticalServiceAfter = "-1s"
	require.ErrorIs(t, registry.Register(options, testAppInfo), ErrInvalidOptions)

	options = testOptions("0.0.0.0:30006", time.Second)
	options.Check.DeregisterCriticalServiceAfter = ""
	require.ErrorIs(t, registry.Register(options, testAppInfo), ErrInvalidOptions)

	options = testOptions("0.0.0.0:30006", 0)
	options.Check.TTL.Duration = "5s"
	require.ErrorIs(t, registry.Register(options, testAppInfo), ErrInvalidOptions)
	assert.Nil(t, agent.Registered())
}

func TestTTLPingerUsesExplicitCheckIDAndStops(t *testing.T) {
	agent := startFakeConsulAgent(t)
	registry := newRegistry(t, agent.address)
	options := testOptions("0.0.0.0:30006", 5*time.Millisecond)
	require.NoError(t, registry.Register(options, testAppInfo))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		registry.TTLCheckPinger(ctx, options)
	}()
	require.Eventually(t, func() bool { return len(agent.TTLUpdates()) > 0 }, time.Second, 5*time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("TTL pinger did not stop after cancellation")
	}
	for _, checkID := range agent.TTLUpdates() {
		assert.Equal(t, "service:"+testAppInfo.ID, checkID)
	}
}

func TestTTLPingerFirstPingIsImmediate(t *testing.T) {
	agent := startFakeConsulAgent(t)
	registry := newRegistry(t, agent.address)
	options := testOptions("0.0.0.0:30006", 20*time.Second)
	require.NoError(t, registry.Register(options, testAppInfo))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := time.Now()
	go registry.TTLCheckPinger(ctx, options)
	require.Eventually(t, func() bool { return len(agent.TTLUpdates()) > 0 }, 2*time.Second, 5*time.Millisecond)
	assert.Less(t, time.Since(started), 2*time.Second,
		"the first heartbeat must not wait for the ticker interval")
}

func TestTTLPingerDefaultsMissingIntervalWithoutPanic(t *testing.T) {
	registry := newRegistry(t, "127.0.0.1:1")
	options := testOptions("0.0.0.0:30006", 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	assert.NotPanics(t, func() { registry.TTLCheckPinger(ctx, options) })
}

func TestLateRegistrationAfterShutdownStartsCriticalAndExpires(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	committed := make(chan api.AgentServiceRegistration, 1)
	var releaseOnce sync.Once
	releaseRegistration := func() { releaseOnce.Do(func() { close(release) }) }
	var deregistered atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/v1/agent/service/register":
			var registration api.AgentServiceRegistration
			_ = json.NewDecoder(request.Body).Decode(&registration)
			close(started)
			<-release // Deliberately ignore client cancellation and commit later.
			committed <- registration
		case strings.HasPrefix(request.URL.Path, "/v1/agent/service/deregister/"):
			deregistered.Store(true)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	defer releaseRegistration()

	registry := newRegistry(t, strings.TrimPrefix(server.URL, "http://"))
	options := testOptions("0.0.0.0:30006", time.Second)
	maintainCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	registry.cancelMaintain = cancel
	registry.maintainDone = done
	go func() {
		defer close(done)
		registry.Maintain(maintainCtx, options, testAppInfo)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("registration request did not start")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	require.NoError(t, registry.Shutdown(shutdownCtx))
	assert.True(t, deregistered.Load())

	releaseRegistration()
	select {
	case registration := <-committed:
		require.NotNil(t, registration.Check)
		assert.Equal(t, api.HealthCritical, registration.Check.Status,
			"a late remote commit must never become routable without a heartbeat")
		assert.Equal(t, "1m", registration.Check.DeregisterCriticalServiceAfter,
			"Consul must eventually remove an unowned late registration")
	case <-time.After(time.Second):
		t.Fatal("simulated remote registration did not commit")
	}
}

func TestShutdownCancelsAndJoinsInFlightRegistration(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	release := make(chan struct{})
	var deregistered atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch {
		case request.URL.Path == "/v1/agent/service/register":
			var registration api.AgentServiceRegistration
			_ = json.NewDecoder(request.Body).Decode(&registration)
			close(started)
			select {
			case <-request.Context().Done():
				close(cancelled)
			case <-release:
			}
		case strings.HasPrefix(request.URL.Path, "/v1/agent/service/deregister/"):
			deregistered.Store(true)
		default:
			writer.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	defer close(release)

	registry := newRegistry(t, strings.TrimPrefix(server.URL, "http://"))
	options := testOptions("0.0.0.0:30006", time.Second)
	maintainCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	registry.cancelMaintain = cancel
	registry.maintainDone = done
	go func() {
		defer close(done)
		registry.Maintain(maintainCtx, options, testAppInfo)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("registration request did not start")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Second)
	defer shutdownCancel()
	require.NoError(t, registry.Shutdown(shutdownCtx))
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("registration request did not observe shutdown cancellation")
	}
	assert.True(t, deregistered.Load())
}

func TestDeregisterStopsPingerFirst(t *testing.T) {
	agent := startFakeConsulAgent(t)
	registry := newRegistry(t, agent.address)
	options := testOptions("0.0.0.0:30006", 5*time.Millisecond)
	require.NoError(t, registry.Register(options, testAppInfo))

	pingCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	registry.cancelMaintain = cancel
	registry.maintainDone = done
	go func() {
		defer close(done)
		registry.TTLCheckPinger(pingCtx, options)
	}()
	require.Eventually(t, func() bool { return len(agent.TTLUpdates()) > 0 }, time.Second, 5*time.Millisecond)
	require.NoError(t, registry.Deregister())
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Deregister did not stop the TTL pinger")
	}
	assert.Equal(t, []string{testAppInfo.ID}, agent.Deregistered())
}

// fastRetry makes the backoff sub-millisecond so retry tests finish quickly
// without changing the production defaults.
func fastRetry(registry *ConsulRegistry) {
	registry.retryBase = time.Millisecond
	registry.retryMax = 4 * time.Millisecond
}

// Incident 2026-09-02: every service booted while Consul was unreachable for
// ~5s after a cluster reboot, the single registration attempt timed out, and
// all ten services stayed out of discovery for the rest of their lifetime.
func TestMaintainRetriesUntilConsulAccepts(t *testing.T) {
	agent := startFakeConsulAgent(t)
	agent.rejectRegistrations = 3
	registry := newRegistry(t, agent.address)
	fastRetry(registry)
	options := testOptions("0.0.0.0:30006", 5*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go registry.Maintain(ctx, options, testAppInfo)

	require.Eventually(t, func() bool { return agent.Registered() != nil }, 2*time.Second, time.Millisecond)
	assert.GreaterOrEqual(t, agent.RegisterCalls(), 4, "three rejections must be followed by a successful retry")
	require.Eventually(t, func() bool { return len(agent.TTLUpdates()) > 0 }, time.Second, time.Millisecond)
}

// A Consul restart drops agent-local registrations. The TTL heartbeat is the
// only signal (404 on the check ID); it must trigger re-registration, not an
// endless stream of "failed to update TTL" while the service stays invisible.
func TestMaintainReRegistersAfterConsulForgets(t *testing.T) {
	agent := startFakeConsulAgent(t)
	registry := newRegistry(t, agent.address)
	fastRetry(registry)
	options := testOptions("0.0.0.0:30006", 5*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go registry.Maintain(ctx, options, testAppInfo)
	require.Eventually(t, func() bool { return len(agent.TTLUpdates()) > 0 }, time.Second, time.Millisecond)
	callsBefore := agent.RegisterCalls()

	agent.Forget()

	require.Eventually(t, func() bool { return agent.Registered() != nil }, 2*time.Second, time.Millisecond)
	assert.Greater(t, agent.RegisterCalls(), callsBefore, "heartbeat failure must lead to a new registration")
	ttlBefore := len(agent.TTLUpdates())
	require.Eventually(t, func() bool { return len(agent.TTLUpdates()) > ttlBefore }, time.Second, time.Millisecond,
		"heartbeats must resume against the new registration")
}

// Invalid options are a deploy bug, not a Consul outage: retrying forever would
// only bury the real error under backoff noise.
func TestMaintainDoesNotRetryInvalidOptions(t *testing.T) {
	agent := startFakeConsulAgent(t)
	registry := newRegistry(t, agent.address)
	fastRetry(registry)
	options := testOptions("0.0.0.0", 5*time.Millisecond) // no port

	done := make(chan struct{})
	go func() {
		defer close(done)
		registry.Maintain(context.Background(), options, testAppInfo)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Maintain must return on invalid options instead of retrying")
	}
	assert.Equal(t, 0, agent.RegisterCalls())
}

// A process that never reached Consul must not fail its own shutdown trying to
// deregister something that does not exist.
func TestDeregisterIsNoopWhenNeverRegistered(t *testing.T) {
	agent := startFakeConsulAgent(t)
	registry := newRegistry(t, agent.address)
	require.NoError(t, registry.Deregister())
	assert.Empty(t, agent.Deregistered())
}
