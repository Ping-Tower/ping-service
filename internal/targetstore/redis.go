package targetstore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9"
	"pingtower/ping-service/internal/config"
	"pingtower/ping-service/internal/models"
)

type RedisStore struct {
	client    *redis.Client
	keyPrefix string
}

func NewRedisStore(ctx context.Context, cfg config.RedisConfig) (*RedisStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Password: cfg.Password,
		DB:       cfg.Database,
	})

	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("ping redis: %w", err)
	}

	return &RedisStore{
		client:    client,
		keyPrefix: cfg.KeyPrefix,
	}, nil
}

func (s *RedisStore) SaveTarget(ctx context.Context, target models.ServerEventPayload) error {
	payload, err := json.Marshal(target)
	if err != nil {
		return fmt.Errorf("marshal target %s: %w", target.Server.ID, err)
	}

	if err := s.client.Set(ctx, s.targetKey(target.Server.ID), payload, 0).Err(); err != nil {
		return fmt.Errorf("save target %s: %w", target.Server.ID, err)
	}

	return nil
}

func (s *RedisStore) DeleteTarget(ctx context.Context, serverID string) error {
	if serverID == "" {
		return nil
	}

	if err := s.client.Del(ctx, s.targetKey(serverID)).Err(); err != nil {
		return fmt.Errorf("delete target %s: %w", serverID, err)
	}

	return nil
}

func (s *RedisStore) LoadTargets(ctx context.Context) ([]models.ServerEventPayload, error) {
	keys, err := s.scanKeys(ctx)
	if err != nil {
		return nil, err
	}

	targets := make([]models.ServerEventPayload, 0, len(keys))
	for _, key := range keys {
		raw, err := s.client.Get(ctx, key).Bytes()
		if err != nil {
			return nil, fmt.Errorf("read target %s: %w", key, err)
		}

		var target models.ServerEventPayload
		if err := json.Unmarshal(raw, &target); err != nil {
			return nil, fmt.Errorf("decode target %s: %w", key, err)
		}

		targets = append(targets, target)
	}

	return targets, nil
}

func (s *RedisStore) Close() error {
	return s.client.Close()
}

func (s *RedisStore) scanKeys(ctx context.Context) ([]string, error) {
	keys := make([]string, 0)
	var cursor uint64

	for {
		page, nextCursor, err := s.client.Scan(ctx, cursor, s.targetPattern(), 100).Result()
		if err != nil {
			return nil, fmt.Errorf("scan redis targets: %w", err)
		}

		keys = append(keys, page...)
		cursor = nextCursor
		if cursor == 0 {
			return keys, nil
		}
	}
}

func (s *RedisStore) targetKey(serverID string) string {
	return fmt.Sprintf("%s:target:%s", s.keyPrefix, serverID)
}

func (s *RedisStore) targetPattern() string {
	return fmt.Sprintf("%s:target:*", s.keyPrefix)
}
