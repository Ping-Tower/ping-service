package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoad_FromConfigFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, ".env")
	content := "RABBITMQ_PASSWORD=super-secret\nRABBITMQ_HOST=broker.internal\nRABBITMQ_PORT=5673\nRABBITMQ_PUBLISH_WORKERS=8\n"

	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write config file: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.RabbitMQ.Password != "super-secret" {
		t.Fatalf("expected password from file, got %q", cfg.RabbitMQ.Password)
	}
	if cfg.RabbitMQ.Host != "broker.internal" {
		t.Fatalf("expected host from file, got %q", cfg.RabbitMQ.Host)
	}
	if cfg.RabbitMQ.Port != 5673 {
		t.Fatalf("expected port 5673, got %d", cfg.RabbitMQ.Port)
	}
	if cfg.RabbitMQ.PublishWorkers != 8 {
		t.Fatalf("expected 8 publish workers, got %d", cfg.RabbitMQ.PublishWorkers)
	}
}

func TestLoad_FromEnvironmentWhenFileMissing(t *testing.T) {
	t.Setenv("RABBITMQ_PASSWORD", "env-secret")
	t.Setenv("RABBITMQ_HOST", "env-broker")
	t.Setenv("RABBITMQ_SERVER_EVENTS_QUEUE", "q.custom.server-events")

	cfg, err := Load(filepath.Join(t.TempDir(), "missing.env"))
	if err != nil {
		t.Fatalf("load config from environment: %v", err)
	}

	if cfg.RabbitMQ.Password != "env-secret" {
		t.Fatalf("expected password from environment, got %q", cfg.RabbitMQ.Password)
	}
	if cfg.RabbitMQ.Host != "env-broker" {
		t.Fatalf("expected host from environment, got %q", cfg.RabbitMQ.Host)
	}
	if cfg.RabbitMQ.ServerEventsQueue != "q.custom.server-events" {
		t.Fatalf("expected server events queue override, got %q", cfg.RabbitMQ.ServerEventsQueue)
	}
}
