package pinger

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"pingtower/ping-service/internal/models"
)

func PingTarget(ctx context.Context, target models.ServerEventServer) models.PingRecordedPayload {
	result := newPingRecordedPayload(target)

	switch target.Protocol {
	case models.ProtocolHTTP, models.ProtocolHTTPS:
		return pingHTTP(ctx, result, target)
	case models.ProtocolTCP:
		return pingTCP(ctx, result, target)
	case models.ProtocolICMP:
		return pingICMP(ctx, result, target)
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
