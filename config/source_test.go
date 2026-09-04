package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileSourceLoadsYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bootstrap.yaml")
	require.NoError(t, os.WriteFile(path, []byte("value: local\n"), 0o600))

	source, err := NewFileSource(path)
	require.NoError(t, err)
	assert.Equal(t, FileSourceName, source.Name())
	_, watchable := source.(Watcher)
	assert.False(t, watchable, "file source must remain startup-only")

	raw, err := source.Load(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "local", raw["value"])
}

func TestNewFileSourceRejectsEmptyPath(t *testing.T) {
	source, err := NewFileSource("")
	assert.Nil(t, source)
	require.Error(t, err)
}

func TestParseYAML(t *testing.T) {
	raw, err := ParseYAML([]byte("server:\n  addr: 0.0.0.0:30006\n"))
	require.NoError(t, err)
	server, ok := raw["server"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "0.0.0.0:30006", server["addr"])

	_, err = ParseYAML([]byte("server:\n\taddr: invalid"))
	require.Error(t, err)
}
