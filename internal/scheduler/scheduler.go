package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"pingtower/ping-service/internal/models"
	"pingtower/ping-service/internal/pinger"
)

// PingResultPublisher publishes ping results to a message broker.
type PingResultPublisher interface {
	Publish(context.Context, models.PingRecordedPayload) error
}

type TargetStore interface {
	SaveTarget(context.Context, models.ServerEventPayload) error
	DeleteTarget(context.Context, string) error
	LoadTargets(context.Context) ([]models.ServerEventPayload, error)
}

type PingFunc func(context.Context, models.ServerEventServer) models.PingRecordedPayload

// Scheduler manages the lifecycle of periodic ping targets.
type Scheduler struct {
	publisher  PingResultPublisher
	pingTarget PingFunc
	store      TargetStore

	targets   map[string]context.CancelFunc
	targetsMu sync.RWMutex
	wg        sync.WaitGroup
}

func New(publisher PingResultPublisher, store TargetStore) *Scheduler {
	return &Scheduler{
		publisher:  publisher,
		pingTarget: pinger.PingTarget,
		store:      store,
		targets:    make(map[string]context.CancelFunc),
	}
}

// StartTarget registers a target for periodic probing and starts its scheduler goroutine.
// If a scheduler for this target already exists, it is replaced.
func (s *Scheduler) StartTarget(ctx context.Context, target models.ServerEventPayload) error {
	serverID := target.Server.ID
	if serverID == "" {
		return nil
	}

	if target.Server.IsDeleted || !target.Server.IsActive || target.PingSettings.IsDeleted {
		return s.StopTarget(ctx, serverID)
	}

	if s.store != nil {
		if err := s.store.SaveTarget(ctx, target); err != nil {
			return err
		}
	}

	s.startTargetInMemory(target)
	return nil
}

func (s *Scheduler) RestoreTargets(ctx context.Context) error {
	if s.store == nil {
		return nil
	}

	targets, err := s.store.LoadTargets(ctx)
	if err != nil {
		return err
	}

	for _, target := range targets {
		s.startTargetInMemory(target)
	}

	return nil
}

// StopTarget cancels the scheduler for the given server ID.
func (s *Scheduler) StopTarget(ctx context.Context, serverID string) error {
	if s.store != nil {
		if err := s.store.DeleteTarget(ctx, serverID); err != nil {
			return err
		}
	}

	s.stopTargetInMemory(serverID)
	return nil
}

func (s *Scheduler) startTargetInMemory(target models.ServerEventPayload) {
	serverID := target.Server.ID
	intervalSec := intValueOrDefault(target.PingSettings.IntervalSec, 60)
	s.stopTargetInMemory(serverID)

	ctx, cancel := context.WithCancel(context.Background())

	s.targetsMu.Lock()
	s.targets[serverID] = cancel
	s.targetsMu.Unlock()

	s.wg.Add(1)
	go s.runTargetScheduler(ctx, target, intervalSec)
}

func (s *Scheduler) stopTargetInMemory(serverID string) {
	s.targetsMu.Lock()
	cancel, exists := s.targets[serverID]
	if exists {
		delete(s.targets, serverID)
	}
	s.targetsMu.Unlock()

	if exists {
		cancel()
	}
}

// StopAll cancels all active schedulers and waits for them to finish.
func (s *Scheduler) StopAll() {
	s.targetsMu.Lock()
	cancels := make([]context.CancelFunc, 0, len(s.targets))
	for serverID, cancel := range s.targets {
		cancels = append(cancels, cancel)
		delete(s.targets, serverID)
	}
	s.targetsMu.Unlock()

	for _, cancel := range cancels {
		cancel()
	}

	s.wg.Wait()
}

func (s *Scheduler) runTargetScheduler(ctx context.Context, target models.ServerEventPayload, intervalSec int) {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer ticker.Stop()

	s.runPingCycle(ctx, target)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runPingCycle(ctx, target)
		}
	}
}

func (s *Scheduler) runPingCycle(ctx context.Context, target models.ServerEventPayload) {
	if ctx.Err() != nil {
		return
	}

	retries := intValueOrDefault(target.PingSettings.Retries, 0)
	result := s.runPingAttempts(ctx, target.Server, retries)
	if ctx.Err() != nil {
		return
	}

	if err := s.publisher.Publish(ctx, result); err != nil {
		log.Printf("error publishing ping result for %s: %v", result.ServerID, err)
	}
}

func (s *Scheduler) runPingAttempts(ctx context.Context, server models.ServerEventServer, retries int) models.PingRecordedPayload {
	attempts := retries + 1
	if attempts <= 0 {
		attempts = 1
	}

	var result models.PingRecordedPayload
	for attempt := 1; attempt <= attempts; attempt++ {
		result = s.pingTarget(ctx, server)
		if result.IsSuccess || attempt == attempts {
			return result
		}
		if ctx.Err() != nil {
			return result
		}

		log.Printf(
			"ping attempt failed for %s on attempt %d/%d, retrying",
			server.ID,
			attempt,
			attempts,
		)
	}

	return result
}

func intValueOrDefault(value *int, fallback int) int {
	if value == nil || *value <= 0 {
		return fallback
	}
	return *value
}
