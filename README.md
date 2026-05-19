# rust-notification-service

A distributed notification platform built in async Rust, demonstrating gRPC (Tonic),
Kafka event-driven messaging, PostgreSQL, Kubernetes orchestration, and a real-time
Angular dashboard.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full system design and technical decisions.

---

## Architecture

```
Angular Dashboard
      │ HTTP REST + WebSocket
      ▼
 api-gateway (Axum)          ←── JWT auth, per-IP rate limiting
      │ Kafka: notification.created
      ▼
 notification-worker          ←── routing logic, delivery_jobs DB writes
      │ gRPC (Tonic)
      ▼
 delivery-service             ←── Email / Webhook / In-app, exponential backoff
      │ Kafka: delivery.completed / delivery.failed
      └──────────────────────► api-gateway WebSocket hub → browser
```

Three independently deployable Rust services:

- **api-gateway** — Axum REST + WebSocket, JWT middleware, per-IP rate limiter, Kafka producer, Prometheus metrics
- **notification-worker** — Kafka consumer, channel routing rules, gRPC client, delivery_jobs tracking
- **delivery-service** — Tonic gRPC server, Email/Webhook/In-app channels, retry with exponential backoff

Infrastructure: PostgreSQL 16, Redpanda (Kafka-compatible), Redis, MailHog.

---

## Quick Start (Docker Compose)

```bash
# Start all services and infrastructure
docker compose up --build

# Open the dashboard
open http://localhost:4200

# Create a notification (requires a signed JWT — see requests.http for a ready-made token)
curl -X POST http://localhost:8080/api/v1/notifications \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{
    "user_id":    "11111111-1111-1111-1111-111111111111",
    "event_type": "order.shipped",
    "title":      "Your order has shipped",
    "body":       "Order #1234 is on the way",
    "metadata":   { "order_id": "1234" }
  }'

# List notifications
curl http://localhost:8080/api/v1/notifications \
  -H "Authorization: Bearer <token>"

# Prometheus metrics
curl http://localhost:9090/metrics

# Captured emails (MailHog)
open http://localhost:8025
```

---

## Local Development (without Docker)

Requires: Rust stable, PostgreSQL, a Kafka-compatible broker on `localhost:9092`, Node 18+.

```bash
# 1. Start infra (Postgres + Redpanda + Redis)
docker compose up -d postgres redis redpanda mailhog

# 2. Start each service
DATABASE_URL=postgresql://notif_user:notif_pass@localhost/notifications \
KAFKA_BROKERS=localhost:9092 \
JWT_SECRET=local-dev-secret \
cargo run --bin delivery-service

DATABASE_URL=postgresql://notif_user:notif_pass@localhost/notifications \
KAFKA_BROKERS=localhost:9092 \
DELIVERY_SERVICE_URL=http://localhost:50051 \
cargo run --bin notification-worker

DATABASE_URL=postgresql://notif_user:notif_pass@localhost/notifications \
KAFKA_BROKERS=localhost:9092 \
JWT_SECRET=local-dev-secret \
cargo run --bin api-gateway

# 3. Run the Angular dashboard (proxies /api to localhost:8080)
cd dashboard
npm install && npm start -- --proxy-config proxy.conf.json
```

Dashboard: **<http://localhost:4200>** — API: **<http://localhost:8080>**

---

## API Reference

| Method | Path | Description |
| ------ | ---- | ----------- |
| `GET` | `/health` | Liveness check |
| `GET` | `/ready` | Readiness check (pings DB) |
| `POST` | `/api/v1/notifications` | Create and dispatch a notification |
| `GET` | `/api/v1/notifications` | List notifications (paginated, filterable by user_id) |
| `GET` | `/api/v1/notifications/:id` | Get single notification |
| `GET` | `/api/v1/preferences` | Get user channel preferences |
| `PUT` | `/api/v1/preferences` | Update user channel preferences |
| `GET` | `/ws` | WebSocket connection for real-time delivery events |
| `GET` | `/metrics` | Prometheus metrics (port 9090) |

All `/api/v1/*` endpoints require `Authorization: Bearer <HS256-JWT>`.

See [`requests.http`](requests.http) for ready-to-run examples.

---

## Kubernetes (minikube)

```bash
minikube start
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmaps/
cp k8s/secrets/secrets.yaml.example k8s/secrets/secrets.yaml  # fill in values
kubectl apply -f k8s/secrets/secrets.yaml
kubectl apply -R -f k8s/
```

The HPA scales the api-gateway between 2–5 replicas at >70% CPU.

---

## Load Testing

```bash
# Requires k6 (https://k6.io)
k6 run k6/load-test.js

# Targets 100 notifications/sec
# Thresholds: p95 < 500 ms, error rate < 1%
```

---

## Project Layout

```text
proto/                  # Protobuf service definitions
services/
  api-gateway/          # REST + WebSocket (Axum)
  notification-worker/  # Kafka consumer + gRPC client
  delivery-service/     # gRPC server (Tonic)
dashboard/              # Angular 17 frontend
migrations/             # Timestamped SQL files (auto-run via Docker)
k8s/                    # Deployments, Services, HPA, Ingress
k6/                     # Load test scripts
requests.http           # REST client examples
.github/workflows/      # CI: fmt → clippy → test → docker build
```

---

## License

MIT — see [LICENSE](LICENSE).
