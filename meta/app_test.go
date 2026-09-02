package meta

import (
	"net"
	"testing"
)

func TestAppInfoFields(t *testing.T) {
	info := AppInfo{
		ID:          "test-id",
		Name:        "test-service",
		Host:        "127.0.0.1",
		Environment: "dev",
		Version:     "v1",
	}

	if info.ID != "test-id" || info.Name != "test-service" || info.Host != "127.0.0.1" || info.Environment != "dev" || info.Version != "v1" {
		t.Fatalf("AppInfo fields were not preserved: %+v", info)
	}
}

func TestGetOutboundIP(t *testing.T) {
	ip, err := GetOutboundIP()
	if err != nil {
		t.Skipf("outbound network unavailable: %v", err)
	}
	if parsed := net.ParseIP(ip); parsed == nil {
		t.Fatalf("GetOutboundIP() = %q, want an IP address", ip)
	}
}

func TestBuildVersionIsNotEmpty(t *testing.T) {
	if Version == "" {
		t.Fatal("Version must have a local-build fallback")
	}
}
