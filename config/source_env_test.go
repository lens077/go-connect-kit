package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func clearSourceEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{EnvSource, EnvFile, EnvSourceFile} {
		t.Setenv(key, "")
	}
}

func TestFromEnvironmentUsesSelectorFactory(t *testing.T) {
	clearSourceEnvironment(t)
	t.Setenv(EnvSourceFile, "/etc/service/source.yaml")

	var selectedPath string
	source, err := FromEnvironment(func(path string) (Source, error) {
		selectedPath = path
		return staticSource{name: "remote", raw: map[string]any{"value": "loaded"}}, nil
	})
	if err != nil {
		t.Fatalf("FromEnvironment() error = %v", err)
	}
	if selectedPath != "/etc/service/source.yaml" {
		t.Fatalf("selector path = %q", selectedPath)
	}
	if source.Name() != "remote" {
		t.Fatalf("source name = %q", source.Name())
	}
}

func TestFromEnvironmentUsesExplicitLocalFile(t *testing.T) {
	clearSourceEnvironment(t)
	path := filepath.Join(t.TempDir(), "bootstrap.yaml")
	if err := os.WriteFile(path, []byte("value: local\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(EnvSource, FileSourceName)
	t.Setenv(EnvFile, path)

	source, err := FromEnvironment(nil)
	if err != nil {
		t.Fatalf("FromEnvironment() error = %v", err)
	}
	raw, err := source.Load(context.Background())
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if raw["value"] != "local" {
		t.Fatalf("value = %#v", raw["value"])
	}
}

func TestFromEnvironmentRequiresSelector(t *testing.T) {
	clearSourceEnvironment(t)
	if _, err := FromEnvironment(nil); err == nil {
		t.Fatal("FromEnvironment() error = nil")
	}
}
