package env

import (
	"os"
	"testing"
)

func TestGetEnvString(t *testing.T) {
	const key = "ECOMMERCE_TEST_ENV_STRING"

	t.Run("existing", func(t *testing.T) {
		t.Setenv(key, "configured")
		if got := GetEnvString(key, "fallback"); got != "configured" {
			t.Fatalf("GetEnvString() = %q, want configured", got)
		}
	})

	t.Run("missing", func(t *testing.T) {
		t.Setenv(key, "temporary")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		if got := GetEnvString(key, "fallback"); got != "fallback" {
			t.Fatalf("GetEnvString() = %q, want fallback", got)
		}
	})

	t.Run("empty", func(t *testing.T) {
		t.Setenv(key, "")
		if got := GetEnvString(key, "fallback"); got != "fallback" {
			t.Fatalf("GetEnvString() = %q, want fallback", got)
		}
	})
}

func TestGetEnvBool(t *testing.T) {
	const key = "ECOMMERCE_TEST_ENV_BOOL"

	tests := []struct {
		name     string
		value    string
		fallback bool
		want     bool
	}{
		{name: "true", value: "true", want: true},
		{name: "uppercase true", value: "TRUE", want: true},
		{name: "one", value: "1", want: true},
		{name: "false", value: "false", fallback: true, want: false},
		{name: "uppercase false", value: "FALSE", fallback: true, want: false},
		{name: "zero", value: "0", fallback: true, want: false},
		{name: "invalid", value: "invalid", fallback: true, want: true},
		{name: "empty", value: "", fallback: true, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(key, tt.value)
			if got := GetEnvBool(key, tt.fallback); got != tt.want {
				t.Fatalf("GetEnvBool() = %t, want %t", got, tt.want)
			}
		})
	}

	t.Run("missing", func(t *testing.T) {
		t.Setenv(key, "temporary")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		if got := GetEnvBool(key, true); !got {
			t.Fatal("GetEnvBool() = false, want fallback true")
		}
	})
}
