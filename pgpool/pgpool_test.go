package pgpool

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

	"github.com/exaring/otelpgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dbtx mirrors the interface sqlc generates for every consumer's models package.
type dbtx interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Hot reload depends on both: Queries built on a Live survive a swap, and pool metrics
// registered on a Live follow the current pool.
var (
	_ dbtx              = (*Live)(nil)
	_ otelpgx.PoolStats = (*Live)(nil)
)

// The per-service copies started from ParseConfig("") and replaced only the host, so the
// generated fallbacks still pointed at the default host in plaintext. Any fallback must
// target the configured host.
func TestBuildConfigNeverFallsBackToAnotherHost(t *testing.T) {
	t.Setenv("PGHOST", "default-host.invalid")
	for _, mode := range []string{"", SSLModeDisable, SSLModeAllow, SSLModePrefer, SSLModeRequire, SSLModeVerifyCA, SSLModeVerifyFull} {
		t.Run(mode, func(t *testing.T) {
			config, err := buildConfig(Options{Host: "db.example.com", Port: 6432, SSLMode: mode})
			require.NoError(t, err)
			assert.Equal(t, "db.example.com", config.ConnConfig.Host)
			for _, fallback := range config.ConnConfig.Fallbacks {
				assert.Equal(t, "db.example.com", fallback.Host)
				assert.Equal(t, uint16(6432), fallback.Port)
			}
		})
	}
}

func TestTLSModes(t *testing.T) {
	caPEM, _, _ := newCA(t)

	tests := []struct {
		mode, ca          string
		primaryTLS        bool
		primaryVerifies   bool // any certificate verification at all
		chainOnly         bool // verify-ca: chain checked, host name not
		fallbacks         int
		fallbackPlaintext bool
	}{
		{mode: SSLModeDisable},
		{mode: SSLModeAllow, fallbacks: 1},
		{mode: "", primaryTLS: true, fallbacks: 1, fallbackPlaintext: true},
		{mode: SSLModePrefer, primaryTLS: true, fallbacks: 1, fallbackPlaintext: true},
		{mode: SSLModeRequire, primaryTLS: true},
		{mode: SSLModeRequire, ca: caPEM, primaryTLS: true, primaryVerifies: true, chainOnly: true},
		{mode: SSLModeVerifyCA, ca: caPEM, primaryTLS: true, primaryVerifies: true, chainOnly: true},
		{mode: SSLModeVerifyFull, ca: caPEM, primaryTLS: true, primaryVerifies: true},
		{mode: SSLModeVerifyFull, primaryTLS: true, primaryVerifies: true}, // system roots, never a silent downgrade
	}
	for _, tt := range tests {
		t.Run(tt.mode+"/ca="+boolString(tt.ca != ""), func(t *testing.T) {
			primary, fallbacks, err := tlsConfigs(Options{Host: "db.example.com", SSLMode: tt.mode, CAPEM: tt.ca}, 5432)
			require.NoError(t, err)

			require.Equal(t, tt.primaryTLS, primary != nil)
			if primary != nil {
				assert.Equal(t, "db.example.com", primary.ServerName)
				verifies := !primary.InsecureSkipVerify || primary.VerifyPeerCertificate != nil
				assert.Equal(t, tt.primaryVerifies, verifies)
				assert.Equal(t, tt.chainOnly, primary.VerifyPeerCertificate != nil)
				if tt.mode == SSLModeVerifyFull && tt.ca != "" {
					assert.NotNil(t, primary.RootCAs)
				}
			}

			require.Len(t, fallbacks, tt.fallbacks)
			if tt.fallbacks == 1 {
				assert.Equal(t, tt.fallbackPlaintext, fallbacks[0].TLSConfig == nil)
			}
		})
	}
}

func TestTLSModesRejectBadInput(t *testing.T) {
	_, _, err := tlsConfigs(Options{Host: "db", SSLMode: "verify-everything"}, 5432)
	assert.ErrorContains(t, err, "unknown ssl mode")

	_, _, err = tlsConfigs(Options{Host: "db", SSLMode: SSLModeVerifyFull, CAPEM: "not a pem"}, 5432)
	assert.ErrorContains(t, err, "CA PEM")
}

// verify-ca is hand-rolled on top of crypto/tls, so prove it rejects what it must reject.
func TestVerifyCAChecksChainButNotHostName(t *testing.T) {
	caPEM, caCert, caKey := newCA(t)
	_, otherCert, otherKey := newCA(t)

	config := verifyCAConfig("db.example.com", poolFromPEM(t, caPEM))

	trusted := newLeaf(t, caCert, caKey, "not-the-host.example.org")
	assert.NoError(t, config.VerifyPeerCertificate([][]byte{trusted}, nil), "host name must not be checked")

	untrusted := newLeaf(t, otherCert, otherKey, "db.example.com")
	assert.Error(t, config.VerifyPeerCertificate([][]byte{untrusted}, nil), "an unknown CA must be rejected")

	assert.Error(t, config.VerifyPeerCertificate(nil, nil))
}

func TestBuildConfigMapsOptionsAndKeepsPgxDefaults(t *testing.T) {
	config, err := buildConfig(Options{
		Host: "db", Port: 6432, Database: "shop", User: "app", Password: "secret", Timezone: "UTC",
		MaxConns: 7, MinConns: 2, MaxConnLifetime: time.Hour, MaxConnIdleTime: time.Minute, PingTimeout: 3 * time.Second,
		SSLMode: SSLModeDisable, TypeNames: []string{"cart.cart_type", "cart._cart_type"},
	})
	require.NoError(t, err)
	assert.Equal(t, uint16(6432), config.ConnConfig.Port)
	assert.Equal(t, "shop", config.ConnConfig.Database)
	assert.Equal(t, "app", config.ConnConfig.User)
	assert.Equal(t, "secret", config.ConnConfig.Password)
	assert.Equal(t, "UTC", config.ConnConfig.RuntimeParams["timezone"])
	assert.Equal(t, int32(7), config.MaxConns)
	assert.Equal(t, int32(2), config.MinConns)
	assert.Equal(t, time.Hour, config.MaxConnLifetime)
	assert.Equal(t, time.Minute, config.MaxConnIdleTime)
	assert.Equal(t, 3*time.Second, config.PingTimeout)
	assert.NotNil(t, config.ConnConfig.Tracer)
	assert.NotNil(t, config.AfterConnect, "TypeNames must be registered on every connection")

	// Zero values keep pgx defaults instead of forcing zero (MaxConns 0 fails pool creation;
	// an empty timezone would be sent to the server as an invalid TimeZone).
	defaults, err := buildConfig(Options{Host: "db", SSLMode: SSLModeDisable})
	require.NoError(t, err)
	assert.Equal(t, uint16(5432), defaults.ConnConfig.Port)
	assert.Positive(t, defaults.MaxConns)
	assert.NotContains(t, defaults.ConnConfig.RuntimeParams, "timezone")
	assert.Nil(t, defaults.AfterConnect)
}

func TestOptionsEqualComparesTypeNames(t *testing.T) {
	base := Options{Host: "db", TypeNames: []string{"a"}}
	assert.True(t, base.equal(Options{Host: "db", TypeNames: []string{"a"}}))
	assert.False(t, base.equal(Options{Host: "db", TypeNames: []string{"a", "_a"}}))
	assert.False(t, base.equal(Options{Host: "other", TypeNames: []string{"a"}}))
}

func TestLiveSwapRedirectsQueries(t *testing.T) {
	first := lazyPool(t, "postgres://u:p@127.0.0.1:5432/a")
	second := lazyPool(t, "postgres://u:p@127.0.0.1:5432/b")

	live := NewLive(first)
	assert.Same(t, first, live.Pool())
	assert.Equal(t, "a", live.Config().ConnConfig.Database)

	assert.Same(t, first, live.swap(second), "swap hands the old pool back for delayed close")
	assert.Same(t, second, live.Pool())
	assert.Equal(t, "b", live.Config().ConnConfig.Database)
	assert.NotNil(t, live.Stat())
}

// lazyPool parses the DSN without dialing; pgxpool connects on first use.
func lazyPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func newCA(t *testing.T) (string, *x509.Certificate, *ecdsa.PrivateKey) {
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
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})), cert, key
}

func newLeaf(t *testing.T, ca *x509.Certificate, caKey *ecdsa.PrivateKey, dnsName string) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: dnsName},
		DNSNames:     []string{dnsName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, ca, &key.PublicKey, caKey)
	require.NoError(t, err)
	return der
}

func poolFromPEM(t *testing.T, caPEM string) *x509.CertPool {
	t.Helper()
	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM([]byte(caPEM)))
	return pool
}

func boolString(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}
