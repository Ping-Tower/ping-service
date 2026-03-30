package pinger

import (
	"context"
	"fmt"
	"strings"

	probing "github.com/prometheus-community/pro-bing"
	"pingtower/ping-service/internal/models"
)

func pingICMP(result models.PingRecordedPayload, target models.ServerEventServer, timeoutMs int) models.PingRecordedPayload {
	if strings.TrimSpace(target.Host) == "" {
		result.ErrorMessage = stringPtr("icmp probe failed: empty host")
		return result
	}

	timeout := normalizeTimeout(timeoutMs)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	result.DNSLookupMs = measureDNSLookup(ctx, target.Host)

	pinger, err := probing.NewPinger(target.Host)
	if err != nil {
		result.ErrorMessage = stringPtr(fmt.Sprintf("create icmp pinger: %v", err))
		return result
	}

	pinger.Count = 1
	pinger.Timeout = timeout
	pinger.Interval = timeout
	pinger.RecordTTLs = true
	pinger.SetPrivileged(false)

	var receivedBytes int64
	var receivedTTL int
	pinger.OnRecv = func(pkt *probing.Packet) {
		receivedBytes = int64(pkt.Nbytes)
		receivedTTL = pkt.TTL
	}

	err = pinger.RunWithContext(ctx)
	stats := pinger.Statistics()

	if stats != nil {
		result.PacketLossPercent = float64Ptr(stats.PacketLoss)
		if stats.PacketsSent > 0 {
			result.SentBytes = int64Ptr(int64((pinger.Size + 8) * stats.PacketsSent))
		}
		if stats.PacketsRecv > 0 {
			result.LatencyMs = float64Ptr(durationMilliseconds(stats.AvgRtt))
			result.RTTMinMs = float64Ptr(durationMilliseconds(stats.MinRtt))
			result.RTTMaxMs = float64Ptr(durationMilliseconds(stats.MaxRtt))

			if receivedBytes > 0 {
				result.ReceivedBytes = int64Ptr(receivedBytes)
			}

			if receivedTTL > 0 {
				ttl := receivedTTL
				result.TTL = &ttl
			} else if len(stats.TTLs) > 0 {
				ttl := int(stats.TTLs[0])
				result.TTL = &ttl
			}
		}

		result.IsSuccess = stats.PacketsRecv > 0 && stats.PacketLoss == 0
	}

	if err != nil {
		result.ErrorMessage = stringPtr(fmt.Sprintf("icmp probe failed: %v", err))
	}

	return result
}