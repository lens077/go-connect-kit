package log

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestElasticsearchLoggerOptions(t *testing.T) {
	logger := NewElasticsearchLogger(nil, ElasticsearchOptions{RequestBody: true})
	if !logger.RequestBodyEnabled() {
		t.Fatal("RequestBodyEnabled() = false, want true")
	}
	if logger.ResponseBodyEnabled() {
		t.Fatal("ResponseBodyEnabled() = true, want false")
	}
}

func TestElasticsearchLoggerHandlesMissingResponse(t *testing.T) {
	core, observed := observer.New(zapcore.WarnLevel)
	logger := NewElasticsearchLogger(zap.New(core), ElasticsearchOptions{})
	request, err := http.NewRequest(http.MethodGet, "http://search.test/products", nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := logger.LogRoundTrip(request, nil, errors.New("connection refused"), time.Now(), time.Millisecond); err != nil {
		t.Fatalf("LogRoundTrip() error = %v", err)
	}
	entries := observed.All()
	if len(entries) != 1 || entries[0].Message != "elasticsearch request failed" {
		t.Fatalf("observed entries = %#v", entries)
	}
}
