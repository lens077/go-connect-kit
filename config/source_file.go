package config

import (
	"context"
	"fmt"
	"os"
)

var _ Source = (*fileSource)(nil)

type fileSource struct {
	path string
}

// NewFileSource creates a startup-only local file source. It deliberately does
// not implement Watcher.
func NewFileSource(path string) (Source, error) {
	if path == "" {
		return nil, fmt.Errorf("config file path is empty")
	}
	return &fileSource{path: path}, nil
}

func (s *fileSource) Name() string { return FileSourceName }

func (s *fileSource) Load(context.Context) (map[string]any, error) {
	contents, err := os.ReadFile(s.path)
	if err != nil {
		return nil, fmt.Errorf("read local config %q: %w", s.path, err)
	}
	if len(contents) == 0 {
		return nil, fmt.Errorf("local config %q is empty", s.path)
	}
	return ParseYAML(contents)
}
