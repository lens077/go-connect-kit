package configschema

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRedactedErrorReportsPathWithoutConfigValue(t *testing.T) {
	schema := []byte(`{
		"type": "object",
		"additionalProperties": false,
		"properties": {"token": {"type": "string", "minLength": 20}},
		"required": ["token"]
	}`)
	const secret = "short-secret"
	err := Validate(schema, []byte("token: "+secret))
	require.Error(t, err)

	message := RedactedError(err)
	assert.Contains(t, message, "/token")
	assert.NotContains(t, message, secret)
}
