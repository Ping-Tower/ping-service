# Ping Service Agent Guide

## Current State

- `ping-service` is currently empty in this workspace: no source files, configs, or build files were found.
- This document is therefore based on the verified system architecture diagram and RabbitMQ contracts in:
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/asyncapi.yaml`
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/config/definitions.json`

## Service Role

- `ping-service` is the active probing service in PingTower.
- According to the architecture diagram, it is a Go service that:
  - consumes monitoring target changes from RabbitMQ
  - executes health checks for active targets
  - publishes raw ping results back to RabbitMQ
- It is not the owner of:
  - target CRUD, auth, or user settings
  - final status aggregation
  - notifications
  - ClickHouse writes
  - Redis hot state

## System Boundaries

- API owns target definitions and publishes `server.target.*` events.
- `ping-service` reacts to those events and updates its in-memory or local execution state.
- `ping-service` publishes `server.ping.recorded`.
- `metrics-writer` persists raw ping samples to ClickHouse.
- `state-elevator` derives server status and publishes `server.status.changed`.
- `ping-service` should not publish `server.status.changed` unless the contract is explicitly changed everywhere.

## RabbitMQ Contract

- Broker topology source of truth:
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/asyncapi.yaml`
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/config/definitions.json`
- `ping-service` consumer queue:
  - `q.ping-service.server-events`
- `ping-service` subscribes to:
  - `serverEventsExchange` with routing key pattern `server.target.*`
- `ping-service` must handle these events:
  - `server.target.added`
  - `server.target.updated`
  - `server.target.deleted`
- `ping-service` publishes:
  - exchange: `pingEventsExchange`
  - routing key: `server.ping.recorded`

## Incoming Message Shape

- All target change events use `ServerEventPayload`.
- Payload shape:
  - `server`
  - `pingSettings`
- `server` fields relevant to the worker:
  - `id`
  - `name`
  - `host`
  - `query`
  - `userId`
  - `port`
  - `isActive`
  - `protocol`
  - `status`
  - `isDeleted`
- `pingSettings` fields relevant to scheduling/probing:
  - `id`
  - `serverId`
  - `intervalSec`
  - `latencyThresholdMs`
  - `retries`
  - `failureThreshold`
  - `isDeleted`

## Outgoing Message Shape

- `ping-service` publishes `PingRecordedPayload`.
- Required fields:
  - `id`
  - `serverId`
  - `protocol`
  - `timestamp`
  - `isSuccess`
- Optional fields supported by the contract:
  - `latencyMs`
  - `errorMessage`
  - `statusCode`
  - `certExpiresAt`
  - `tlsVersion`
  - `dnsLookupMs`
  - `sentBytes`
  - `receivedBytes`
  - `packetLossPercent`
  - `rttMinMs`
  - `rttMaxMs`
  - `ttl`

## Protocol Expectations

- Supported protocols from the contract:
  - `ICMP`
  - `TCP`
  - `HTTP`
  - `HTTPS`
- Any implementation must keep protocol handling aligned with the enum in `asyncapi.yaml`.
- Do not introduce protocol aliases or lowercase wire values unless the contract is updated first.

## Behavioral Expectations

- On `server.target.added`:
  - register the target for probing
  - schedule probes using `pingSettings.intervalSec`
- On `server.target.updated`:
  - replace existing target configuration atomically
  - update schedule and protocol-specific settings
- For `HTTP` and `HTTPS` targets:
  - treat `server.query` as the request path/query component
  - default to `/` only when `server.query` is absent or empty
- On `server.target.deleted`:
  - stop probing the target
  - remove it from active state even if the payload is a soft delete
- Respect `server.isActive` and `isDeleted`.
- Treat `pingSettings` as part of the target contract, not as optional local metadata.

## Ownership Rules

- Keep `ping-service` stateless with respect to system-of-record data unless a local cache is clearly ephemeral.
- Do not move business ownership from other services into `ping-service`.
- Do not add direct writes to PostgreSQL, ClickHouse, or Redis unless the architecture is intentionally being changed.
- If the service starts persisting local state, document why and how recovery works after restart.

## Change Rules

- If you change queue names, exchanges, or routing keys, update both:
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/asyncapi.yaml`
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/config/definitions.json`
- If you change `ServerEventPayload` or `PingRecordedPayload`, update all producers and consumers, not only `ping-service`.
- Do not change the meaning of `server.ping.recorded` to carry aggregated status. Aggregated status belongs to `state-elevator`.
- Preserve JSON field names from the contract exactly.

## Execution Rules

- Commands that depend on network access, external package registries, remote module proxies, broker reachability, or other sandbox-restricted resources should not be retried repeatedly inside sandbox.
- For such commands, prefer running outside sandbox immediately or request escalation right away.
- Typical examples:
  - `go get`
  - `go mod tidy`
  - `go build` if it needs to download modules
  - integration checks against RabbitMQ or other external services
- Pure local actions should still stay inside sandbox:
  - editing files
  - `gofmt`
  - local static inspection
  - builds that already have all dependencies available locally

## Implementation Guidance

- If this service is scaffolded from scratch, prefer a layout that makes these responsibilities explicit:
  - RabbitMQ consumer for target events
  - target registry / scheduler
  - protocol-specific probe runners
  - RabbitMQ publisher for ping results
- Keep contract DTOs separate from internal execution models.
- Make target updates idempotent. Re-delivered RabbitMQ messages must not create duplicate active schedules.
- Prefer explicit ack/nack behavior and dead-letter-friendly handling for malformed messages.
- Generate ping result IDs in the service before publish.
- Emit timestamps in UTC and use RFC 3339 / JSON date-time compatible values.

## Practical Starting Point

- Before adding code, read:
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/asyncapi.yaml`
  - `/home/semao0/Projects/PingTower/infra/rabbitmq/config/definitions.json`
  - `/home/semao0/Projects/PingTower/gitlab-profile/images/schema.png`
- Treat those files as the current source of truth for `ping-service` behavior until real code appears in this directory.
