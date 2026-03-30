package messaging

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"pingtower/ping-service/internal/config"
	"pingtower/ping-service/internal/models"
)

var errServiceClosed = errors.New("rabbitmq service closed")

type publishRequest struct {
	ctx     context.Context
	payload models.PingRecordedPayload
	result  chan error
}

// RabbitMQService manages a RabbitMQ connection with automatic reconnection,
// a dedicated consume channel, and a pool of publish workers.
type RabbitMQService struct {
	cfg config.RabbitMQConfig

	ctx    context.Context
	cancel context.CancelFunc

	stateMu         sync.RWMutex
	reconnectMu     sync.Mutex
	conn            *amqp.Connection
	consumeChannel  *amqp.Channel
	publishChannels []*amqp.Channel

	deliveries    chan amqp.Delivery
	publishQueues []chan publishRequest

	rr          atomic.Uint64
	lifecycleWg sync.WaitGroup
}

func NewRabbitMQService(cfg config.RabbitMQConfig) (*RabbitMQService, error) {
	ctx, cancel := context.WithCancel(context.Background())

	workerCount := cfg.PublishWorkers
	if workerCount <= 0 {
		workerCount = 1
	}

	queueBuffer := cfg.PublishQueueBuffer
	if queueBuffer <= 0 {
		queueBuffer = 1
	}

	service := &RabbitMQService{
		cfg:           cfg,
		ctx:           ctx,
		cancel:        cancel,
		deliveries:    make(chan amqp.Delivery, queueBuffer),
		publishQueues: make([]chan publishRequest, workerCount),
	}

	for i := range service.publishQueues {
		service.publishQueues[i] = make(chan publishRequest, queueBuffer)
	}

	if err := service.reconnect(); err != nil {
		cancel()
		return nil, err
	}

	service.lifecycleWg.Add(1)
	go service.consumeLoop()

	for i := range service.publishQueues {
		service.lifecycleWg.Add(1)
		go service.publishLoop(i)
	}

	return service, nil
}

// Deliveries returns a channel of incoming server event messages.
func (s *RabbitMQService) Deliveries() <-chan amqp.Delivery {
	return s.deliveries
}

// Publish sends a ping result to the configured exchange using round-robin worker distribution.
func (s *RabbitMQService) Publish(ctx context.Context, payload models.PingRecordedPayload) error {
	if s.ctx.Err() != nil {
		return errServiceClosed
	}

	request := publishRequest{
		ctx:     ctx,
		payload: payload,
		result:  make(chan error, 1),
	}

	idx := int(s.rr.Add(1)-1) % len(s.publishQueues)

	select {
	case <-s.ctx.Done():
		return errServiceClosed
	case <-ctx.Done():
		return ctx.Err()
	case s.publishQueues[idx] <- request:
	}

	select {
	case <-s.ctx.Done():
		return errServiceClosed
	case <-ctx.Done():
		return ctx.Err()
	case err := <-request.result:
		return err
	}
}

// Close shuts down the service, waits for all workers to stop, and closes the deliveries channel.
func (s *RabbitMQService) Close() {
	s.cancel()
	s.closeResources()
	s.lifecycleWg.Wait()
	close(s.deliveries)
}

func (s *RabbitMQService) consumeLoop() {
	defer s.lifecycleWg.Done()

	for {
		if s.ctx.Err() != nil {
			return
		}

		channel, err := s.currentConsumeChannel()
		if err != nil {
			if !s.retryReconnect("consume loop") {
				return
			}
			continue
		}

		deliveries, err := consumeServerEvents(channel, s.cfg)
		if err != nil {
			log.Printf("error starting consumer: %v", err)
			if !s.retryReconnect("consume start") {
				return
			}
			continue
		}

		log.Printf("consuming server events from queue %s", s.cfg.ServerEventsQueue)

	forwardLoop:
		for {
			select {
			case <-s.ctx.Done():
				return
			case delivery, ok := <-deliveries:
				if !ok {
					log.Printf("server events consumer channel closed, reconnecting")
					if !s.retryReconnect("consumer closed") {
						return
					}
					break forwardLoop
				}
				select {
				case <-s.ctx.Done():
					return
				case s.deliveries <- delivery:
				}
			}
		}
	}
}

func (s *RabbitMQService) publishLoop(workerIndex int) {
	defer s.lifecycleWg.Done()

	queue := s.publishQueues[workerIndex]

	for {
		select {
		case <-s.ctx.Done():
			return
		case request := <-queue:
			request.result <- s.publishWithRetry(workerIndex, request.ctx, request.payload)
		}
	}
}

func (s *RabbitMQService) publishWithRetry(workerIndex int, ctx context.Context, payload models.PingRecordedPayload) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.ctx.Err() != nil {
			return errServiceClosed
		}

		channel, err := s.currentPublishChannel(workerIndex)
		if err != nil {
			if !s.retryReconnect("publish acquire channel") {
				return errServiceClosed
			}
			continue
		}

		if err := publishPingRecorded(ctx, channel, s.cfg, payload); err != nil {
			log.Printf("publish failed for %s, reconnecting: %v", payload.ServerID, err)
			if !s.retryReconnect("publish failure") {
				return errServiceClosed
			}
			continue
		}

		return nil
	}
}

func (s *RabbitMQService) currentConsumeChannel() (*amqp.Channel, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()

	if s.consumeChannel == nil || s.consumeChannel.IsClosed() {
		return nil, fmt.Errorf("consume channel is not available")
	}
	return s.consumeChannel, nil
}

func (s *RabbitMQService) currentPublishChannel(index int) (*amqp.Channel, error) {
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()

	if index < 0 || index >= len(s.publishChannels) {
		return nil, fmt.Errorf("publish channel index %d out of range", index)
	}

	channel := s.publishChannels[index]
	if channel == nil || channel.IsClosed() {
		return nil, fmt.Errorf("publish channel %d is not available", index)
	}
	return channel, nil
}

func (s *RabbitMQService) reconnect() error {
	s.reconnectMu.Lock()
	defer s.reconnectMu.Unlock()

	if s.ctx.Err() != nil {
		return errServiceClosed
	}

	log.Printf("connecting to RabbitMQ at %s:%d vhost=%s", s.cfg.Host, s.cfg.Port, s.cfg.VHost)

	conn, err := connectRabbitMQ(s.cfg)
	if err != nil {
		return err
	}

	consumeChannel, err := openChannel(conn)
	if err != nil {
		_ = conn.Close()
		return err
	}

	publishChannels := make([]*amqp.Channel, len(s.publishQueues))
	for i := range publishChannels {
		channel, openErr := openChannel(conn)
		if openErr != nil {
			for _, existing := range publishChannels {
				if existing != nil {
					_ = existing.Close()
				}
			}
			_ = consumeChannel.Close()
			_ = conn.Close()
			return openErr
		}
		publishChannels[i] = channel
	}

	s.stateMu.Lock()
	oldConn := s.conn
	oldConsumeChannel := s.consumeChannel
	oldPublishChannels := s.publishChannels

	s.conn = conn
	s.consumeChannel = consumeChannel
	s.publishChannels = publishChannels
	s.stateMu.Unlock()

	closeChannel(oldConsumeChannel)
	closeChannels(oldPublishChannels)
	closeConnection(oldConn)

	log.Printf("RabbitMQ connection ready")
	return nil
}

func (s *RabbitMQService) retryReconnect(reason string) bool {
	if s.ctx.Err() != nil {
		return false
	}

	for {
		if err := s.reconnect(); err != nil {
			if s.ctx.Err() != nil {
				return false
			}
			log.Printf("RabbitMQ reconnect failed after %s: %v", reason, err)
			select {
			case <-s.ctx.Done():
				return false
			case <-time.After(s.reconnectDelay()):
			}
			continue
		}
		log.Printf("RabbitMQ reconnected after %s", reason)
		return true
	}
}

func (s *RabbitMQService) reconnectDelay() time.Duration {
	delayMs := s.cfg.ReconnectDelayMs
	if delayMs <= 0 {
		delayMs = 5000
	}
	return time.Duration(delayMs) * time.Millisecond
}

func (s *RabbitMQService) closeResources() {
	s.stateMu.Lock()
	conn := s.conn
	consumeChannel := s.consumeChannel
	publishChannels := s.publishChannels

	s.conn = nil
	s.consumeChannel = nil
	s.publishChannels = nil
	s.stateMu.Unlock()

	closeChannel(consumeChannel)
	closeChannels(publishChannels)
	closeConnection(conn)
}

func rabbitMQURL(cfg config.RabbitMQConfig) string {
	vhost := cfg.VHost
	if vhost == "" {
		vhost = "/"
	}
	return fmt.Sprintf(
		"amqp://%s:%s@%s:%d/%s",
		url.QueryEscape(cfg.User),
		url.QueryEscape(cfg.Password),
		cfg.Host,
		cfg.Port,
		url.PathEscape(vhost),
	)
}

func connectRabbitMQ(cfg config.RabbitMQConfig) (*amqp.Connection, error) {
	conn, err := amqp.Dial(rabbitMQURL(cfg))
	if err != nil {
		return nil, fmt.Errorf("connect to rabbitmq: %w", err)
	}
	return conn, nil
}

func openChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open rabbitmq channel: %w", err)
	}
	return channel, nil
}

func consumeServerEvents(channel *amqp.Channel, cfg config.RabbitMQConfig) (<-chan amqp.Delivery, error) {
	deliveries, err := channel.Consume(
		cfg.ServerEventsQueue,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("consume server events from queue %s: %w", cfg.ServerEventsQueue, err)
	}
	return deliveries, nil
}

func publishPingRecorded(ctx context.Context, channel *amqp.Channel, cfg config.RabbitMQConfig, payload models.PingRecordedPayload) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal ping recorded payload: %w", err)
	}
	if err := channel.PublishWithContext(
		ctx,
		cfg.PingEventsExchange,
		cfg.PingRecordedRoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Body:         body,
		},
	); err != nil {
		return fmt.Errorf("publish ping recorded payload: %w", err)
	}
	return nil
}

func closeChannels(channels []*amqp.Channel) {
	for _, ch := range channels {
		closeChannel(ch)
	}
}

func closeChannel(channel *amqp.Channel) {
	if channel == nil || channel.IsClosed() {
		return
	}
	_ = channel.Close()
}

func closeConnection(conn *amqp.Connection) {
	if conn == nil || conn.IsClosed() {
		return
	}
	_ = conn.Close()
}