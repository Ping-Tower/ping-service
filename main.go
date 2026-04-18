package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
	"pingtower/ping-service/internal/config"
	"pingtower/ping-service/internal/messaging"
	"pingtower/ping-service/internal/models"
	"pingtower/ping-service/internal/scheduler"
	"pingtower/ping-service/internal/targetstore"
)

func main() {
	if err := run(); err != nil {
		log.Printf("ping-service stopped with error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	log.Printf("ping-service starting")

	cfg, err := config.Load(".env")
	if err != nil {
		return err
	}

	rmq, err := messaging.NewRabbitMQService(cfg.RabbitMQ)
	if err != nil {
		return fmt.Errorf("create RabbitMQ service: %w", err)
	}
	defer rmq.Close()

	store, err := targetstore.NewRedisStore(context.Background(), cfg.Redis)
	if err != nil {
		return fmt.Errorf("create redis target store: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Printf("error closing redis target store: %v", err)
		}
	}()

	sched := scheduler.New(rmq, store)
	log.Printf("scheduler configured")

	if err := sched.RestoreTargets(context.Background()); err != nil {
		return fmt.Errorf("restore targets from redis: %w", err)
	}
	log.Printf("restored targets from redis")

	deliveries := rmq.Deliveries()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("shutdown signal received")
			sched.StopAll()
			log.Printf("ping-service stopped")
			return nil
		case d, ok := <-deliveries:
			if !ok {
				log.Printf("deliveries channel closed")
				sched.StopAll()
				return nil
			}

			var payload models.ServerEventPayload
			if err := json.Unmarshal(d.Body, &payload); err != nil {
				log.Printf("invalid server event payload for routing key %s: %v", d.RoutingKey, err)
				ackDelivery(d)
				continue
			}

			switch d.RoutingKey {
			case "server.target.added", "server.target.updated":
				log.Printf(
					"starting target scheduler: routing_key=%s server_id=%s host=%s protocol=%s",
					d.RoutingKey,
					payload.Server.ID,
					payload.Server.Host,
					payload.Server.Protocol,
				)
				if err := sched.StartTarget(ctx, payload); err != nil {
					log.Printf("error starting target scheduler for %s: %v", payload.Server.ID, err)
					nackDelivery(d, true)
					continue
				}
			case "server.target.deleted":
				log.Printf(
					"stopping target scheduler: routing_key=%s server_id=%s host=%s",
					d.RoutingKey,
					payload.Server.ID,
					payload.Server.Host,
				)
				if err := sched.StopTarget(ctx, payload.Server.ID); err != nil {
					log.Printf("error stopping target scheduler for %s: %v", payload.Server.ID, err)
					nackDelivery(d, true)
					continue
				}
			default:
				log.Printf("ignoring unsupported routing key %s for server_id=%s", d.RoutingKey, payload.Server.ID)
			}

			ackDelivery(d)
		}
	}
}

func ackDelivery(d amqp.Delivery) {
	if err := d.Ack(false); err != nil {
		log.Printf("error acking delivery: %v", err)
	}
}

func nackDelivery(d amqp.Delivery, requeue bool) {
	if err := d.Nack(false, requeue); err != nil {
		log.Printf("error nacking delivery: %v", err)
	}
}
