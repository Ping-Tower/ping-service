package pinger

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"pingtower/ping-service/internal/models"
)

func pingTCP(parentCtx context.Context, result models.PingRecordedPayload, target models.ServerEventServer) models.PingRecordedPayload {
	address, err := tcpAddress(target)
	if err != nil {
		result.ErrorMessage = stringPtr(err.Error())
		return result
	}

	timeout := networkProbeTimeout()
	ctx, cancel := context.WithTimeout(parentCtx, timeout)
	defer cancel()

	result.DNSLookupMs = measureDNSLookup(ctx, target.Host)

	dialer := &net.Dialer{Timeout: timeout}

	start := time.Now()
	conn, err := dialer.DialContext(ctx, "tcp", address)
	elapsedMs := durationMilliseconds(time.Since(start))
	result.LatencyMs = float64Ptr(elapsedMs)
	if err != nil {
		result.ErrorMessage = stringPtr(fmt.Sprintf("tcp probe failed: %v", err))
		return result
	}
	defer conn.Close()

	result.IsSuccess = true
	result.RTTMinMs = float64Ptr(elapsedMs)
	result.RTTMaxMs = float64Ptr(elapsedMs)
	return result
}

func tcpAddress(target models.ServerEventServer) (string, error) {
	host := strings.TrimSpace(target.Host)
	if host == "" {
		return "", fmt.Errorf("tcp probe failed: empty host")
	}
	if target.Port == nil {
		return "", fmt.Errorf("tcp probe failed: port is required")
	}
	return net.JoinHostPort(host, strconv.Itoa(*target.Port)), nil
}
