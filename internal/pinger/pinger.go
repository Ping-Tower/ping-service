package pinger

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"pingtower/ping-service/internal/models"
)

func PingTarget(target models.ServerEventServer, timeoutMs int) models.PingRecordedPayload {
	result := newPingRecordedPayload(target)

	switch target.Protocol {
	case models.ProtocolHTTP, models.ProtocolHTTPS:
		return pingHTTP(result, target, timeoutMs)
	case models.ProtocolTCP:
		return pingTCP(result, target, timeoutMs)
	case models.ProtocolICMP:
		return pingICMP(result, target, timeoutMs)
	default:
		result.ErrorMessage = stringPtr(fmt.Sprintf("unsupported protocol: %s", target.Protocol))
		return result
	}
}

func newPingRecordedPayload(target models.ServerEventServer) models.PingRecordedPayload {
	return models.PingRecordedPayload{
		ID:        uuid.New().String(),
		ServerID:  target.ID,
		Protocol:  target.Protocol,
		Timestamp: time.Now().UTC(),
		IsSuccess: false,
	}
}