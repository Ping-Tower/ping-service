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

// Scheduler manages the lifecycle of periodic ping targets.
type Scheduler struct {
	publisher PingResultPublisher

	targets   map[string]context.CancelFunc
	targetsMu sync.RWMutex
	wg        sync.WaitGroup
}

func New(publisher PingResultPublisher) *Scheduler {
	return &Scheduler{
		publisher: publisher,
		targets:   make(map[string]context.CancelFunc),
	}
}

// StartTarget registers a target for periodic probing and starts its scheduler goroutine.
// If a scheduler for this target already exists, it is replaced.
func (s *Scheduler) StartTarget(target models.ServerEventPayload) {
	serverID := target.Server.ID
	if serverID == "" {
		return
	}

	if target.Server.IsDeleted || !target.Server.IsActive || target.PingSettings.IsDeleted {
		s.StopTarget(serverID)
		return
	}

	intervalSec := intValueOrDefault(target.PingSettings.IntervalSec, 60)
	timeoutMs := intValueOrDefault(target.PingSettings.LatencyThresholdMs, 400)

	s.StopTarget(serverID)

	ctx, cancel := context.WithCancel(context.Background())

	s.targetsMu.Lock()
	s.targets[serverID] = cancel
	s.targetsMu.Unlock()

	s.wg.Add(1)
	go s.runTargetScheduler(ctx, target, intervalSec, timeoutMs)
}

// StopTarget cancels the scheduler for the given server ID.
func (s *Scheduler) StopTarget(serverID string) {
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

func (s *Scheduler) runTargetScheduler(ctx context.Context, target models.ServerEventPayload, intervalSec, timeoutMs int) {
	defer s.wg.Done()

	ticker := time.NewTicker(time.Duration(intervalSec) * time.Second)
	defer ticker.Stop()

	s.runPingCycle(ctx, target, timeoutMs)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.runPingCycle(ctx, target, timeoutMs)
		}
	}
}

func (s *Scheduler) runPingCycle(ctx context.Context, target models.ServerEventPayload, timeoutMs int) {
	result := pinger.PingTarget(target.Server, timeoutMs)
	if err := s.publisher.Publish(ctx, result); err != nil {
		log.Printf("error publishing ping result for %s: %v", result.ServerID, err)
	}
}

func intValueOrDefault(value *int, fallback int) int {
	if value == nil || *value <= 0 {
		return fallback
	}
	return *value
}