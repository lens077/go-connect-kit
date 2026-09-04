package config

import (
	"bytes"
	"context"
	"fmt"

	"github.com/lens077/go-connect-kit/env"
	"github.com/spf13/viper"
)

const (
	// EnvSource selects the legacy local-file configuration path.
	EnvSource = "CONFIG_SOURCE"
	// EnvFile points at a complete local configuration document.
	EnvFile = "CONFIG_FILE"
	// EnvSourceFile points at the small provider selector consumed by SelectorFactory.
	EnvSourceFile = "CONFIG_SOURCE_FILE"

	FileSourceName = "file"

	legacyConfigCenterSource = "configcenter"
)

// Source fetches one complete configuration document.
type Source interface {
	// Name returns the selected source identifier.
	Name() string
	// Load returns the parsed YAML document as a provider-neutral map.
	Load(ctx context.Context) (map[string]any, error)
}

// WatchEvent describes one configuration update.
type WatchEvent struct {
	Raw     map[string]any
	Deleted bool
	Err     error
}

// Watcher is the optional change-stream capability implemented by watchable sources.
type Watcher interface {
	Watch(ctx context.Context, onEvent func(WatchEvent)) error
}

// SelectorFactory builds the provider selected by CONFIG_SOURCE_FILE.
type SelectorFactory func(path string) (Source, error)

// FromEnvironment selects the configured source. Normal startup uses a local
// provider selector; CONFIG_SOURCE=file is an explicit local-only escape hatch.
func FromEnvironment(selector SelectorFactory) (Source, error) {
	if sourceConfigFile := env.GetEnvString(EnvSourceFile, ""); sourceConfigFile != "" {
		if selector == nil {
			return nil, fmt.Errorf("%s is set but no selector factory was provided", EnvSourceFile)
		}
		return selector(sourceConfigFile)
	}

	name := env.GetEnvString(EnvSource, "")
	switch name {
	case FileSourceName:
		path := env.GetEnvString(EnvFile, "")
		if path == "" {
			return nil, fmt.Errorf("required env %s is missing when %s=%s", EnvFile, EnvSource, FileSourceName)
		}
		return NewFileSource(path)
	case legacyConfigCenterSource:
		return nil, fmt.Errorf("%s=%s is deprecated; set %s to a local source selector instead",
			EnvSource, legacyConfigCenterSource, EnvSourceFile)
	case "":
		return nil, fmt.Errorf("required env %s is missing", EnvSourceFile)
	default:
		return nil, fmt.Errorf("unknown %s=%q, expect %q or set %s",
			EnvSource, name, FileSourceName, EnvSourceFile)
	}
}

// ParseYAML parses one YAML document for Source adapters.
func ParseYAML(data []byte) (map[string]any, error) {
	v := viper.New()
	v.SetConfigType("yaml")
	if err := v.ReadConfig(bytes.NewBuffer(data)); err != nil {
		return nil, err
	}
	return v.AllSettings(), nil
}
