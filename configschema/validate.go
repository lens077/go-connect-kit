// Package configschema validates YAML documents against generated JSON Schema.
package configschema

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v5"
	"gopkg.in/yaml.v3"
)

const schemaURL = "inmemory://bootstrap.schema.json"

// Validate checks YAML configuration against a self-contained JSON Schema.
func Validate(schemaData, configData []byte) error {
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource(schemaURL, bytes.NewReader(schemaData)); err != nil {
		return fmt.Errorf("add schema resource: %w", err)
	}
	schema, err := compiler.Compile(schemaURL)
	if err != nil {
		return fmt.Errorf("compile schema: %w", err)
	}

	var yamlValue any
	if err := yaml.Unmarshal(configData, &yamlValue); err != nil {
		return fmt.Errorf("parse YAML: %w", err)
	}

	// Normalize YAML-only map/value types before schema validation.
	encoded, err := json.Marshal(yamlValue)
	if err != nil {
		return fmt.Errorf("normalize YAML: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("decode normalized YAML: %w", err)
	}
	if err := schema.Validate(value); err != nil {
		return err
	}
	return nil
}

// Locations returns redacted instance paths for validation errors. It omits
// values and messages because Bootstrap documents may contain credentials.
func Locations(err error) []string {
	var validationErr *jsonschema.ValidationError
	if !errors.As(err, &validationErr) {
		return []string{"(schema or YAML parse error)"}
	}

	seen := make(map[string]struct{})
	var visit func(*jsonschema.ValidationError)
	visit = func(current *jsonschema.ValidationError) {
		if len(current.Causes) == 0 {
			location := current.InstanceLocation
			if location == "" {
				location = "/"
			}
			seen[location] = struct{}{}
			return
		}
		for _, cause := range current.Causes {
			visit(cause)
		}
	}
	visit(validationErr)

	locations := make([]string, 0, len(seen))
	for location := range seen {
		locations = append(locations, location)
	}
	sort.Strings(locations)
	return locations
}

// RedactedError formats a validation failure without leaking config values.
func RedactedError(err error) string {
	return "invalid configuration at " + strings.Join(Locations(err), ", ")
}
