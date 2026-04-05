package pinger

import (
	"testing"

	"pingtower/ping-service/internal/models"
)

func TestBuildHTTPURL_FromHostPortAndQuery(t *testing.T) {
	t.Parallel()

	port := 8080
	target := models.ServerEventServer{
		Host:     "example.com",
		Port:     &port,
		Query:    stringPtr("/health?probe=1"),
		Protocol: models.ProtocolHTTP,
	}

	got, err := buildHTTPURL(target)
	if err != nil {
		t.Fatalf("buildHTTPURL returned error: %v", err)
	}

	want := "http://example.com:8080/health?probe=1"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}

func TestBuildHTTPURL_WithAbsoluteURLDefaultsPath(t *testing.T) {
	t.Parallel()

	target := models.ServerEventServer{
		Host:     "https://example.com",
		Protocol: models.ProtocolHTTPS,
	}

	got, err := buildHTTPURL(target)
	if err != nil {
		t.Fatalf("buildHTTPURL returned error: %v", err)
	}

	want := "https://example.com/"
	if got != want {
		t.Fatalf("expected %q, got %q", want, got)
	}
}
