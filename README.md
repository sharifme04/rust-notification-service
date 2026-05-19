# rust-notification-service

A distributed notification platform built in Rust to demonstrate gRPC (Tonic),
Kafka event-driven messaging, PostgreSQL, Kubernetes orchestration, and a
real-time Angular dashboard.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design.

## Architecture at a glance

```
Client ──REST/WS──► api-gateway ──Kafka──► notification-worker ──gRPC──► delivery-service
                        │                          │                          │
                        └─── PostgreSQL ◄──────────┴──── delivery_jobs ──────┘
```

Three Rust services:
- **api-gateway** (Axum) — REST + WebSocket, publishes to Kafka, exposes `/metrics`
- **notification-worker** — Kafka consumer, routing logic, gRPC client
- **delivery-service** (Tonic) — gRPC server with Email / Webhook / In-app channels

Plus: Angular 17 dashboard, PostgreSQL, Redpanda (Kafka-compatible), Redis, MailHog.

## Quick start (Docker Compose)

```bash
# Bring up infra + all services
docker compose up --build

# Create a notification
curl -X POST http://localhost:8080/api/v1/notifications \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "00000000-0000-0000-0000-000000000001",
    "event_type": "order.shipped",
    "title": "Your order has shipped",
    "body": "Order #1234 is on the way"
  }'

# List recent notifications
curl http://localhost:8080/api/v1/notifications | jq

# Prometheus metrics
curl http://localhost:9090/metrics

# Captured emails (MailHog UI)
open http://localhost:8025
```

## Local development (without Docker)

Requires Rust stable, PostgreSQL, and a Kafka-compatible broker on `localhost:9092`.

```bash
# 1. Run migrations against your local Postgres
psql $DATABASE_URL -f migrations/20260519000000_extensions.sql
psql $DATABASE_URL -f migrations/20260519000001_notifications.sql
psql $DATABASE_URL -f migrations/20260519000002_delivery_jobs.sql
psql $DATABASE_URL -f migrations/20260519000003_user_preferences.sql
psql $DATABASE_URL -f migrations/20260519000004_delivery_logs.sql

# 2. Build the workspace
cargo build

# 3. Start each service in its own terminal
cargo run -p delivery-service
cargo run -p notification-worker
cargo run -p api-gateway
```

## Kubernetes (minikube)

```bash
minikube start
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmaps/
cp k8s/secrets/secrets.yaml.example k8s/secrets/secrets.yaml  # then edit
kubectl apply -f k8s/secrets/secrets.yaml
kubectl apply -R -f k8s/
```

The HPA scales the api-gateway between 2–5 replicas at >70% CPU.

## Project layout

```
proto/                  # Protobuf service definitions
services/
  api-gateway/          # REST + WebSocket (Axum)
  notification-worker/  # Kafka consumer + gRPC client
  delivery-service/     # gRPC server (Tonic)
dashboard/              # Angular 17 frontend
migrations/             # SQLx-style timestamped SQL files
k8s/                    # Deployments, Services, HPA, Ingress
.github/workflows/      # CI: fmt, clippy, test, build
```

## Status

This repository is the working scaffold described in
[ARCHITECTURE.md](ARCHITECTURE.md). Phase 1 (infrastructure + skeletons) is
complete, and Phase 2 (core flow: REST → Kafka → routing → gRPC → channels) is
substantially wired up. Remaining work tracked in the architecture doc:
WebSocket bridge from Kafka → connected clients, dashboard hookup against a
running stack, K8s smoke testing on minikube, and load-test scripting.

## License

MIT — see [LICENSE](LICENSE).
