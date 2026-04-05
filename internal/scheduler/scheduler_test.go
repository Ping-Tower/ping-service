package scheduler

import (
	"context"
	"sync"
	"testing"

	"pingtower/ping-service/internal/models"
)

func TestRunPingAttempts_RetriesUntilSuccess(t *testing.T) {
	t.Parallel()

	server := models.ServerEventServer{ID: "server-1"}
	callCount := 0
	s := &Scheduler{
		pingTarget: func(context.Context, models.ServerEventServer) models.PingRecordedPayload {
			callCount++
			return models.PingRecordedPayload{
				ServerID:  server.ID,
				IsSuccess: callCount == 3,
			}
		},
	}

	result := s.runPingAttempts(context.Background(), server, 2)

	if callCount != 3 {
		t.Fatalf("expected 3 ping attempts, got %d", callCount)
	}
	if !result.IsSuccess {
		t.Fatal("expected final attempt to succeed")
	}
}

func TestRunPingAttempts_ReturnsLastFailureWhenRetriesExhausted(t *testing.T) {
	t.Parallel()

	server := models.ServerEventServer{ID: "server-2"}
	callCount := 0
	s := &Scheduler{
		pingTarget: func(context.Context, models.ServerEventServer) models.PingRecordedPayload {
			callCount++
			return models.PingRecordedPayload{
				ServerID:  server.ID,
				IsSuccess: false,
			}
		},
	}

	result := s.runPingAttempts(context.Background(), server, 1)

	if callCount != 2 {
		t.Fatalf("expected 2 ping attempts, got %d", callCount)
	}
	if result.IsSuccess {
		t.Fatal("expected final result to remain unsuccessful")
	}
}

func TestRunPingCycle_PublishesResultAfterRetries(t *testing.T) {
	t.Parallel()

	publisher := &stubPublisher{}
	server := models.ServerEventServer{ID: "server-3"}
	callCount := 0

	s := &Scheduler{
		publisher: publisher,
		pingTarget: func(context.Context, models.ServerEventServer) models.PingRecordedPayload {
			callCount++
			return models.PingRecordedPayload{
				ServerID:  server.ID,
				IsSuccess: callCount == 2,
			}
		},
	}

	retries := 1
	s.runPingCycle(context.Background(), models.ServerEventPayload{
		Server: server,
		PingSettings: models.ServerEventPingSettings{
			Retries: &retries,
		},
	})

	if callCount != 2 {
		t.Fatalf("expected 2 ping attempts, got %d", callCount)
	}
	if publisher.publishCount() != 1 {
		t.Fatalf("expected 1 published result, got %d", publisher.publishCount())
	}
	if !publisher.lastPayload().IsSuccess {
		t.Fatal("expected published payload to be successful")
	}
}

func TestStartTarget_InactiveTargetCancelsExistingScheduler(t *testing.T) {
	t.Parallel()

	cancelled := make(chan struct{})
	s := &Scheduler{
		targets: map[string]context.CancelFunc{
			"server-4": func() { close(cancelled) },
		},
	}

	s.StartTarget(models.ServerEventPayload{
		Server: models.ServerEventServer{
			ID:       "server-4",
			IsActive: false,
		},
	})

	select {
	case <-cancelled:
	default:
		t.Fatal("expected existing scheduler to be cancelled")
	}

	if _, exists := s.targets["server-4"]; exists {
		t.Fatal("expected target entry to be removed")
	}
}

type stubPublisher struct {
	mu       sync.Mutex
	payloads []models.PingRecordedPayload
}

func (s *stubPublisher) Publish(_ context.Context, payload models.PingRecordedPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.payloads = append(s.payloads, payload)
	return nil
}

func (s *stubPublisher) publishCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.payloads)
}

func (s *stubPublisher) lastPayload() models.PingRecordedPayload {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.payloads[len(s.payloads)-1]
}
