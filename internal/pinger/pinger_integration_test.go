package pinger

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"pingtower/ping-service/internal/models"
)

func TestPingTarget_HTTPAgainstLocalServer(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			t.Fatalf("expected request path /ready, got %s", r.URL.Path)
		}
		if r.URL.Query().Get("probe") != "1" {
			t.Fatalf("expected query probe=1, got %s", r.URL.RawQuery)
		}

		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	parsedURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse test server url: %v", err)
	}

	port, err := strconv.Atoi(parsedURL.Port())
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}

	result := PingTarget(context.Background(), models.ServerEventServer{
		ID:       "server-http",
		Host:     parsedURL.Hostname(),
		Port:     &port,
		Query:    stringPtr("/ready?probe=1"),
		Protocol: models.ProtocolHTTP,
	})

	if !result.IsSuccess {
		t.Fatalf("expected successful ping, got failure with error %v", result.ErrorMessage)
	}
	if result.StatusCode == nil || *result.StatusCode != http.StatusNoContent {
		t.Fatalf("expected status code %d, got %#v", http.StatusNoContent, result.StatusCode)
	}
	if result.LatencyMs == nil || *result.LatencyMs < 0 {
		t.Fatalf("expected non-negative latency, got %#v", result.LatencyMs)
	}
}

func TestPingTarget_TCPAgainstLocalListener(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	defer listener.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)

		conn, acceptErr := listener.Accept()
		if acceptErr == nil {
			_ = conn.Close()
		}
	}()

	address := listener.Addr().(*net.TCPAddr)

	result := PingTarget(context.Background(), models.ServerEventServer{
		ID:       "server-tcp",
		Host:     address.IP.String(),
		Port:     &address.Port,
		Protocol: models.ProtocolTCP,
	})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("accept loop did not complete")
	}

	if !result.IsSuccess {
		t.Fatalf("expected successful tcp ping, got failure with error %v", result.ErrorMessage)
	}
	if result.LatencyMs == nil || *result.LatencyMs < 0 {
		t.Fatalf("expected non-negative latency, got %#v", result.LatencyMs)
	}
}
