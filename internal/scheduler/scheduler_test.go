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
		store: &stubTargetStore{},
		targets: map[string]context.CancelFunc{
			"server-4": func() { close(cancelled) },
		},
	}

	if err := s.StartTarget(context.Background(), models.ServerEventPayload{
		Server: models.ServerEventServer{
			ID:       "server-4",
			IsActive: false,
		},
	}); err != nil {
		t.Fatalf("start target: %v", err)
	}

	select {
	case <-cancelled:
	default:
		t.Fatal("expected existing scheduler to be cancelled")
	}

	if _, exists := s.targets["server-4"]; exists {
		t.Fatal("expected target entry to be removed")
	}
}

func TestStartTarget_PersistsActiveTarget(t *testing.T) {
	t.Parallel()

	store := &stubTargetStore{}
	s := &Scheduler{
		store:   store,
		targets: make(map[string]context.CancelFunc),
	}

	retries := 1
	target := models.ServerEventPayload{
		Server: models.ServerEventServer{
			ID:       "server-5",
			IsActive: true,
		},
		PingSettings: models.ServerEventPingSettings{
			Retries: &retries,
		},
	}

	if err := s.StartTarget(context.Background(), target); err != nil {
		t.Fatalf("start target: %v", err)
	}
	defer s.StopAll()

	if len(store.saved) != 1 {
		t.Fatalf("expected 1 saved target, got %d", len(store.saved))
	}
	if store.saved[0].Server.ID != "server-5" {
		t.Fatalf("expected saved target server-5, got %s", store.saved[0].Server.ID)
	}
}

func TestRestoreTargets_StartsSchedulersFromStore(t *testing.T) {
	t.Parallel()

	retries := 1
	store := &stubTargetStore{
		loaded: []models.ServerEventPayload{
			{
				Server: models.ServerEventServer{
					ID:       "server-6",
					IsActive: true,
				},
				PingSettings: models.ServerEventPingSettings{
					Retries: &retries,
				},
			},
		},
	}

	s := New(&stubPublisher{}, store)
	defer s.StopAll()

	if err := s.RestoreTargets(context.Background()); err != nil {
		t.Fatalf("restore targets: %v", err)
	}

	if _, exists := s.targets["server-6"]; !exists {
		t.Fatal("expected restored scheduler for server-6")
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

type stubTargetStore struct {
	saved   []models.ServerEventPayload
	deleted []string
	loaded  []models.ServerEventPayload
}

func (s *stubTargetStore) SaveTarget(_ context.Context, target models.ServerEventPayload) error {
	s.saved = append(s.saved, target)
	return nil
}

func (s *stubTargetStore) DeleteTarget(_ context.Context, serverID string) error {
	s.deleted = append(s.deleted, serverID)
	return nil
}

func (s *stubTargetStore) LoadTargets(_ context.Context) ([]models.ServerEventPayload, error) {
	return append([]models.ServerEventPayload(nil), s.loaded...), nil
}
