package pinger

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"
)

const defaultNetworkProbeTimeout = 5 * time.Second

func networkProbeTimeout() time.Duration { return defaultNetworkProbeTimeout }

func durationMilliseconds(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func measureDNSLookup(ctx context.Context, host string) *float64 {
	host = strings.TrimSpace(host)
	host = strings.Trim(host, "[]")
	if host == "" {
		return nil
	}

	if net.ParseIP(host) != nil {
		zero := 0.0
		return &zero
	}

	start := time.Now()
	_, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil
	}

	value := durationMilliseconds(time.Since(start))
	return &value
}

func httpResponseBytes(resp *http.Response, bodyBytes int64) int64 {
	var headers bytes.Buffer
	_ = resp.Header.Write(&headers)

	statusLine := int64(len(resp.Proto) + 1 + len(resp.Status) + 2)
	return statusLine + int64(headers.Len()) + bodyBytes
}

func tlsVersionString(version uint16) string {
	switch version {
	case 0x0301:
		return "TLS1.0"
	case 0x0302:
		return "TLS1.1"
	case 0x0303:
		return "TLS1.2"
	case 0x0304:
		return "TLS1.3"
	default:
		return fmt.Sprintf("0x%04x", version)
	}
}

func stringPtr(value string) *string    { return &value }
func intPtr(value int) *int             { return &value }
func int64Ptr(value int64) *int64       { return &value }
func float64Ptr(value float64) *float64 { return &value }
