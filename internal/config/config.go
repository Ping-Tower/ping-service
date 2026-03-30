package config

import (
	"errors"
	"fmt"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	RabbitMQ RabbitMQConfig `env-prefix:"RABBITMQ_"`
}

type RabbitMQConfig struct {
	Host                   string `env:"HOST" env-default:"localhost"`
	Port                   int    `env:"PORT" env-default:"5672"`
	User                   string `env:"USER" env-default:"admin"`
	Password               string `env:"PASSWORD" env-required:"true"`
	VHost                  string `env:"VHOST" env-default:"/"`
	ServerEventsQueue      string `env:"SERVER_EVENTS_QUEUE" env-default:"q.ping-service.server-events"`
	PingEventsExchange     string `env:"PING_EVENTS_EXCHANGE" env-default:"pingEventsExchange"`
	PingRecordedRoutingKey string `env:"PING_RECORDED_ROUTING_KEY" env-default:"server.ping.recorded"`
	ReconnectDelayMs       int    `env:"RECONNECT_DELAY_MS" env-default:"5000"`
	PublishWorkers         int    `env:"PUBLISH_WORKERS" env-default:"4"`
	PublishQueueBuffer     int    `env:"PUBLISH_QUEUE_BUFFER" env-default:"256"`
}

func Load(path string) (Config, error) {
	if path == "" {
		path = ".env"
	}

	var cfg Config

	_, err := os.Stat(path)
	switch {
	case err == nil:
		if err := cleanenv.ReadConfig(path, &cfg); err != nil {
			return Config{}, fmt.Errorf("read config from %s: %w", path, err)
		}
	case errors.Is(err, os.ErrNotExist):
		if err := cleanenv.ReadEnv(&cfg); err != nil {
			return Config{}, fmt.Errorf("read config from environment: %w", err)
		}
	default:
		return Config{}, fmt.Errorf("stat config file %s: %w", path, err)
	}

	return cfg, nil
}