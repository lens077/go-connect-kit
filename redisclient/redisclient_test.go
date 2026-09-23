package redisclient

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"

	"errors"
	"strings"

	"github.com/lens077/go-connect-kit/config"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func testCAPEM(t *testing.T) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
}

func TestBuildOptionsMapsFields(t *testing.T) {
	got, err := buildOptions(Options{
		Addr: "cache:6380", Username: "default", Password: "secret", DB: 3,
		DialTimeout: time.Second, ReadTimeout: 2 * time.Second, WriteTimeout: 3 * time.Second,
		PoolSize: 20, MinIdleConns: 4,
	})
	require.NoError(t, err)
	assert.Equal(t, "cache:6380", got.Addr)
	assert.Equal(t, "default", got.Username)
	assert.Equal(t, "secret", got.Password)
	assert.Equal(t, 3, got.DB)
	assert.Equal(t, time.Second, got.DialTimeout)
	assert.Equal(t, 2*time.Second, got.ReadTimeout)
	assert.Equal(t, 3*time.Second, got.WriteTimeout)
	assert.Equal(t, 20, got.PoolSize)
	assert.Equal(t, 4, got.MinIdleConns)
	assert.Nil(t, got.TLSConfig, "TLS stays off unless enabled")
}

func TestBuildOptionsTLS(t *testing.T) {
	got, err := buildOptions(Options{Addr: "cache:6379", TLS: TLSOptions{Enabled: true, CAPEM: testCAPEM(t)}})
	require.NoError(t, err)
	require.NotNil(t, got.TLSConfig)
	assert.False(t, got.TLSConfig.InsecureSkipVerify)
	assert.NotNil(t, got.TLSConfig.RootCAs)
	assert.Empty(t, got.TLSConfig.ServerName, "go-redis derives ServerName from Addr")

	_, err = buildOptions(Options{Addr: "cache:6379", TLS: TLSOptions{Enabled: true, CAPEM: "not a pem"}})
	assert.ErrorContains(t, err, "CA PEM")
}

// A failed build used to print one "failed to dial" line per MinIdleConns background dial,
// straight to stderr: 12 lines for one unreachable address in a 2026-09-24 hot-reload test.
func TestFailedBuildLogsOnceThroughZap(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	logger := zap.New(core)

	_, err := Build(context.Background(), Options{
		Addr:         "127.0.0.1:1", // connection refused, no network needed
		DialTimeout:  time.Second,
		PoolSize:     10,
		MinIdleConns: 5,
	}, logger)
	require.Error(t, err)

	// Background dials, if any were started, finish their retries and log after Build returns.
	time.Sleep(1500 * time.Millisecond)

	redisLines := logs.FilterField(zap.String("component", "go-redis")).All()
	assert.LessOrEqual(t, len(redisLines), 1, "only the probe's own dial failure may be logged")
	for _, entry := range redisLines {
		assert.Equal(t, zap.WarnLevel, entry.Level)
	}
}

func TestLiveSwap(t *testing.T) {
	first := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	second := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6380"})
	t.Cleanup(func() { _ = first.Close(); _ = second.Close() })

	live := NewLive(first)
	assert.Same(t, first, live.Client())
	assert.Same(t, first, live.swap(second), "swap hands the old client back for delayed close")
	assert.Same(t, second, live.Client())
}

func TestStaleReportsFailedRebuildUntilResolved(t *testing.T) {
	unreachable := errors.New("no such host")
	build := func(_ context.Context, options Options, _ *zap.Logger) (*redis.Client, error) {
		if strings.HasPrefix(options.Addr, "down") {
			return nil, unreachable
		}
		client := redis.NewClient(&redis.Options{Addr: options.Addr})
		t.Cleanup(func() { _ = client.Close() })
		return client, nil
	}
	project := func(c *wrapperspb.StringValue) Options { return Options{Addr: c.GetValue()} }
	source := config.NewLive(wrapperspb.String("127.0.0.1:6379"))

	live, err := newLive(fxtest.NewLifecycle(t), project, source.Get(), source, zap.NewNop(), build)
	require.NoError(t, err)
	first := live.Client()

	source.Set(wrapperspb.String("down:6379"))
	assert.ErrorIs(t, live.Stale(), unreachable)
	assert.Same(t, first, live.Client())

	source.Set(wrapperspb.String("127.0.0.1:6379"))
	assert.NoError(t, live.Stale())

	source.Set(wrapperspb.String("down:6379"))
	source.Set(wrapperspb.String("127.0.0.1:6380"))
	assert.NoError(t, live.Stale())
	assert.Equal(t, "127.0.0.1:6380", live.Client().Options().Addr)
}
