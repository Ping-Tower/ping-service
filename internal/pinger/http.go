package pinger

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"pingtower/ping-service/internal/models"
)

var sharedHTTPTransport = &http.Transport{
	Proxy:                 http.ProxyFromEnvironment,
	ForceAttemptHTTP2:     true,
	MaxIdleConns:          256,
	MaxIdleConnsPerHost:   32,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 1 * time.Second,
}

var sharedHTTPClient = &http.Client{
	Transport: sharedHTTPTransport,
}

func pingHTTP(parentCtx context.Context, result models.PingRecordedPayload, target models.ServerEventServer) models.PingRecordedPayload {
	targetURL, err := buildHTTPURL(target)
	if err != nil {
		result.ErrorMessage = stringPtr(err.Error())
		return result
	}

	timeout := networkProbeTimeout()
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	traceMetrics := newHTTPTraceMetrics()
	trace := &httptrace.ClientTrace{
		DNSStart:          func(httptrace.DNSStartInfo) { traceMetrics.onDNSStart() },
		DNSDone:           func(httptrace.DNSDoneInfo) { traceMetrics.onDNSDone() },
		ConnectStart:      func(_, _ string) { traceMetrics.onConnectStart() },
		ConnectDone:       func(_, _ string, _ error) { traceMetrics.onConnectDone() },
		TLSHandshakeStart: func() { traceMetrics.onTLSHandshakeStart() },
		TLSHandshakeDone:  func(_ tls.ConnectionState, _ error) { traceMetrics.onTLSHandshakeDone() },
		GotConn:           func(info httptrace.GotConnInfo) { traceMetrics.onGotConn(info) },
	}
	ctx = httptrace.WithClientTrace(ctx, trace)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
	if err != nil {
		result.ErrorMessage = stringPtr(fmt.Sprintf("build http request: %v", err))
		return result
	}

	if dumpedRequest, dumpErr := httputil.DumpRequestOut(req, false); dumpErr == nil {
		result.SentBytes = int64Ptr(int64(len(dumpedRequest)))
	}

	start := time.Now()
	resp, err := sharedHTTPClient.Do(req)
	elapsedMs := durationMilliseconds(time.Since(start))
	result.LatencyMs = float64Ptr(elapsedMs)
	if err != nil {
		result.ErrorMessage = stringPtr(fmt.Sprintf("http probe failed: %v", err))
		return result
	}
	defer resp.Body.Close()

	bodyBytes, copyErr := io.Copy(io.Discard, resp.Body)
	if copyErr != nil {
		result.ErrorMessage = stringPtr(fmt.Sprintf("http response read failed: %v", copyErr))
		return result
	}

	result.StatusCode = intPtr(resp.StatusCode)
	result.IsSuccess = resp.StatusCode >= 200 && resp.StatusCode <= 299
	result.RTTMinMs = float64Ptr(elapsedMs)
	result.RTTMaxMs = float64Ptr(elapsedMs)
	result.ReceivedBytes = int64Ptr(httpResponseBytes(resp, bodyBytes))
	if traceMetrics.DNSLookupMs != nil {
		result.DNSLookupMs = traceMetrics.DNSLookupMs
	}

	if resp.TLS != nil {
		result.TLSVersion = stringPtr(tlsVersionString(resp.TLS.Version))
		if len(resp.TLS.PeerCertificates) > 0 {
			expiresAt := resp.TLS.PeerCertificates[0].NotAfter.UTC()
			result.CertExpiresAt = &expiresAt
		}
	}

	return result
}

func buildHTTPURL(target models.ServerEventServer) (string, error) {
	host := strings.TrimSpace(target.Host)
	if host == "" {
		return "", fmt.Errorf("http probe failed: empty host")
	}

	scheme := "http"
	if target.Protocol == models.ProtocolHTTPS {
		scheme = "https"
	}

	if strings.HasPrefix(host, "http://") || strings.HasPrefix(host, "https://") {
		parsed, err := url.Parse(host)
		if err != nil {
			return "", fmt.Errorf("http probe failed: parse host url: %w", err)
		}
		if parsed.Scheme == "" {
			parsed.Scheme = scheme
		}
		if parsed.Path == "" {
			parsed.Path = "/"
		}
		applyHTTPQuery(parsed, target.Query)
		return parsed.String(), nil
	}

	u := &url.URL{
		Scheme: scheme,
		Host:   host,
		Path:   "/",
	}
	if target.Port != nil {
		u.Host = net.JoinHostPort(host, strconv.Itoa(*target.Port))
	}

	applyHTTPQuery(u, target.Query)
	return u.String(), nil
}

func applyHTTPQuery(u *url.URL, query *string) {
	if query == nil {
		return
	}
	raw := strings.TrimSpace(*query)
	if raw == "" {
		return
	}
	if strings.HasPrefix(raw, "?") {
		u.RawQuery = strings.TrimPrefix(raw, "?")
		return
	}
	if strings.Contains(raw, "?") {
		parts := strings.SplitN(raw, "?", 2)
		u.Path = normalizeHTTPPath(parts[0])
		u.RawQuery = parts[1]
		return
	}
	u.Path = normalizeHTTPPath(raw)
}

func normalizeHTTPPath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "/"
	}
	if strings.HasPrefix(trimmed, "/") {
		return trimmed
	}
	return "/" + trimmed
}

type httpTraceMetrics struct {
	DNSLookupMs    *float64
	TCPConnectMs   *float64
	TLSHandshakeMs *float64

	dnsStart         time.Time
	connectStart     time.Time
	tlsStart         time.Time
	connectionReused bool
}

func newHTTPTraceMetrics() *httpTraceMetrics {
	return &httpTraceMetrics{}
}

func (m *httpTraceMetrics) onDNSStart() { m.dnsStart = time.Now() }
func (m *httpTraceMetrics) onDNSDone() {
	if !m.dnsStart.IsZero() {
		m.DNSLookupMs = float64Ptr(durationMilliseconds(time.Since(m.dnsStart)))
	}
}
func (m *httpTraceMetrics) onConnectStart() { m.connectStart = time.Now() }
func (m *httpTraceMetrics) onConnectDone() {
	if !m.connectStart.IsZero() {
		m.TCPConnectMs = float64Ptr(durationMilliseconds(time.Since(m.connectStart)))
	}
}
func (m *httpTraceMetrics) onTLSHandshakeStart() { m.tlsStart = time.Now() }
func (m *httpTraceMetrics) onTLSHandshakeDone() {
	if !m.tlsStart.IsZero() {
		m.TLSHandshakeMs = float64Ptr(durationMilliseconds(time.Since(m.tlsStart)))
	}
}
func (m *httpTraceMetrics) onGotConn(info httptrace.GotConnInfo) {
	m.connectionReused = info.Reused
	if info.Reused && m.DNSLookupMs == nil {
		zero := 0.0
		m.DNSLookupMs = &zero
	}
}
