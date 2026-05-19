# rust-notification-service

## Distributed Notification Platform — Architecture

**GitHub:** [sharifme04/rust-notification-service](https://github.com/sharifme04/rust-notification-service)

> A production-quality distributed notification service built in async Rust,
> demonstrating gRPC (Tonic), Kafka event-driven messaging, PostgreSQL,
> Kubernetes orchestration, and a real-time Angular dashboard.

---

## Skills & Patterns Demonstrated

| Capability | Implementation |
|---|---|
| Async Rust backend | Axum 0.8 + Tonic gRPC + Tokio async runtime |
| Distributed systems | Three independently deployable services communicating over Kafka and gRPC |
| gRPC + Protobuf | Tonic gRPC server/client between internal services, compiled via tonic-build |
| Event-driven messaging | Kafka topics spanning the full notification lifecycle |
| Kubernetes + Docker | Multi-stage Dockerfiles, full K8s manifests, HPA, minikube-ready |
| PostgreSQL | SQLx compile-time query checking, schema migrations, indexed lookups |
| Real-time frontend | Angular 17 dashboard with WebSocket-driven live feed |
| CI/CD | Docker Compose local dev → K8s prod → GitHub Actions pipeline |
| Observability | Prometheus metrics on every service, Grafana-ready dashboards |

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                        CLIENT LAYER                                  │
│                                                                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │   Angular Dashboard (TypeScript)                              │   │
│  │   - Real-time notification feed (WebSocket)                   │   │
│  │   - Notification history table + filters                      │   │
│  │   - Channel management (email / webhook / in-app)             │   │
│  │   - Prometheus metrics viewer                                 │   │
│  └──────────────┬───────────────────────────────────────────────┘   │
└─────────────────┼───────────────────────────────────────────────────┘
                  │ HTTP REST + WebSocket
┌─────────────────▼───────────────────────────────────────────────────┐
│                    API GATEWAY SERVICE (Rust / Axum)                 │
│                                                                      │
│   • REST endpoints: POST /notifications, GET /notifications,         │
│                     GET /notifications/:id, GET/PUT /preferences     │
│   • WebSocket hub: broadcasts delivery events to connected clients   │
│   • JWT auth middleware (HS256, FromRequestParts extractor)         │
│   • Per-IP rate limiting (DashMap sliding-window, 200 rps default)  │
│   • Publishes NotificationCreated events → Kafka                     │
│   • Consumes delivery.completed / delivery.failed → WS bridge       │
│   • Exposes /metrics (Prometheus)                                    │
│   • Port: 8080 (REST + WebSocket) / 9090 (metrics)                  │
└─────────────────┬───────────────────────────────────────────────────┘
                  │ Kafka: topic=notification.created
┌─────────────────▼───────────────────────────────────────────────────┐
│               NOTIFICATION WORKER SERVICE (Rust)                     │
│                                                                      │
│   • Consumes Kafka topic: notification.created                       │
│   • Applies routing rules: which channels to use per user/event      │
│   • Creates delivery_jobs rows in PostgreSQL                         │
│   • Calls Delivery Service via gRPC (Tonic)                         │
│   • Publishes notification.routed events → Kafka                     │
│   • Updates delivery_jobs after gRPC response                        │
│   • Exposes /metrics (Prometheus)                                    │
└──────────┬──────────────────────────┬───────────────────────────────┘
           │ gRPC (Tonic)             │
┌──────────▼──────────┐   ┌──────────▼──────────────────────────────┐
│  DELIVERY SERVICE   │   │  PostgreSQL                              │
│  (Rust / Tonic)     │   │                                          │
│                     │   │  • notifications table                   │
│  Proto: DeliverRPC  │   │  • delivery_jobs table                   │
│  Channels:          │   │  • user_preferences table                │
│  • Email (SMTP)     │   │  • delivery_logs table                   │
│  • Webhook (HTTP)   │   │                                          │
│  • In-app (WS)      │   └──────────────────────────────────────────┘
│                     │
│  Exponential        │   ┌──────────────────────────────────────────┐
│  backoff retries    │   │  Redis                                   │
│                     │   │  • WebSocket session store               │
│  Publishes:         │   │  • Rate limit counters                   │
│  delivery.completed │   │  • Notification dedup cache              │
│  delivery.failed    │   └──────────────────────────────────────────┘
│  → Kafka            │
└─────────────────────┘   ┌──────────────────────────────────────────┐
                           │  Kafka (Redpanda-compatible)             │
                           │  Topics:                                 │
                           │  • notification.created                  │
                           │  • notification.routed                   │
                           │  • delivery.completed                    │
                           │  • delivery.failed                       │
                           └──────────────────────────────────────────┘
```

---

## Repository Structure

```
rust-notification-service/
├── proto/
│   └── notification/v1/
│       ├── delivery.proto          # DeliveryService gRPC definition
│       └── types.proto             # Shared message types
│
├── services/
│   ├── api-gateway/                # Rust (Axum) — REST + WebSocket
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── routes/
│   │   │   │   ├── notifications.rs
│   │   │   │   └── health.rs
│   │   │   ├── websocket/
│   │   │   │   ├── hub.rs          # WebSocket connection manager
│   │   │   │   └── handler.rs
│   │   │   ├── kafka/
│   │   │   │   ├── producer.rs
│   │   │   │   └── consumer.rs     # Kafka → WebSocket bridge
│   │   │   ├── middleware/
│   │   │   │   ├── auth.rs         # JWT extractor
│   │   │   │   └── rate_limit.rs   # Per-IP sliding window
│   │   │   └── metrics.rs
│   │   ├── Cargo.toml
│   │   └── Dockerfile
│   │
│   ├── notification-worker/        # Rust — Kafka consumer + gRPC client
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── consumer.rs         # Kafka consumer loop
│   │   │   ├── router.rs           # Channel routing logic
│   │   │   ├── grpc_client.rs      # Tonic client for delivery service
│   │   │   ├── db/
│   │   │   │   ├── mod.rs
│   │   │   │   └── jobs.rs         # delivery_jobs CRUD
│   │   │   └── metrics.rs
│   │   ├── Cargo.toml
│   │   └── Dockerfile
│   │
│   └── delivery-service/           # Rust (Tonic gRPC server)
│       ├── src/
│       │   ├── main.rs
│       │   ├── grpc/
│       │   │   └── delivery_impl.rs # DeliveryService trait impl
│       │   ├── channels/
│       │   │   ├── email.rs         # SMTP delivery
│       │   │   ├── webhook.rs       # HTTP webhook delivery
│       │   │   └── inapp.rs         # In-app via Kafka event
│       │   ├── retry.rs             # Exponential backoff
│       │   └── metrics.rs
│       ├── Cargo.toml
│       └── Dockerfile
│
├── dashboard/                      # Angular 17 frontend
│   ├── src/app/
│   │   ├── components/
│   │   │   ├── notification-feed/  # Real-time WebSocket feed
│   │   │   ├── notification-list/  # History table + filters
│   │   │   ├── metrics-panel/      # Prometheus metrics viewer
│   │   │   └── channel-config/     # User preferences (GET/PUT)
│   │   ├── services/
│   │   │   ├── api.service.ts
│   │   │   └── websocket.service.ts
│   │   └── models/
│   │       └── notification.model.ts
│   ├── angular.json
│   └── Dockerfile
│
├── migrations/                     # Timestamped SQL migrations
│   ├── 20260519000000_extensions.sql
│   ├── 20260519000001_notifications.sql
│   ├── 20260519000002_delivery_jobs.sql
│   ├── 20260519000003_user_preferences.sql
│   └── 20260519000004_delivery_logs.sql
│
├── k8s/                            # Kubernetes manifests
│   ├── namespace.yaml
│   ├── api-gateway/
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   └── hpa.yaml                # Horizontal Pod Autoscaler
│   ├── notification-worker/
│   ├── delivery-service/
│   ├── dashboard/
│   ├── configmaps/
│   ├── secrets/
│   └── ingress.yaml
│
├── k6/
│   └── load-test.js                # 100 rps load test script
│
├── requests.http                   # REST client examples (all endpoints)
├── docker-compose.yml              # Local dev: all services + infra
├── Cargo.toml                      # Workspace root
└── .github/workflows/ci.yml        # CI: fmt + clippy + test + docker build
```

---

## gRPC Proto Definition

```protobuf
// proto/notification/v1/delivery.proto

syntax = "proto3";
package notification.v1;

service DeliveryService {
  rpc Deliver(DeliverRequest) returns (DeliverResponse);
  rpc DeliverBatch(DeliverBatchRequest) returns (stream DeliverBatchResponse);
  rpc GetDeliveryStatus(DeliveryStatusRequest) returns (DeliveryStatusResponse);
}

message DeliverRequest {
  string notification_id = 1;
  string user_id         = 2;
  Channel channel        = 3;
  Payload payload        = 4;
  DeliveryOptions options = 5;
  string delivery_job_id = 6;
}

message Payload {
  string title = 1;
  string body  = 2;
  map<string, string> metadata = 3;
}

message DeliveryOptions {
  uint32 max_retries    = 1;
  uint32 retry_delay_ms = 2;
  uint32 timeout_ms     = 3;
}

enum Channel {
  CHANNEL_UNSPECIFIED = 0;
  CHANNEL_EMAIL       = 1;
  CHANNEL_WEBHOOK     = 2;
  CHANNEL_IN_APP      = 3;
}

message DeliverResponse {
  string         delivery_id   = 1;
  DeliveryStatus status        = 2;
  string         error_message = 3;
  uint32         attempt_count = 4;
}

enum DeliveryStatus {
  STATUS_UNSPECIFIED = 0;
  STATUS_QUEUED      = 1;
  STATUS_DELIVERED   = 2;
  STATUS_FAILED      = 3;
  STATUS_RETRYING    = 4;
}
```

---

## PostgreSQL Schema

```sql
-- extensions
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- notifications
CREATE TABLE notifications (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    title      TEXT NOT NULL,
    body       TEXT NOT NULL,
    metadata   JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_notifications_user_id   ON notifications(user_id);
CREATE INDEX idx_notifications_created_at ON notifications(created_at DESC);

-- delivery_jobs
CREATE TYPE delivery_channel AS ENUM ('email', 'webhook', 'in_app');
CREATE TYPE delivery_status  AS ENUM ('queued', 'delivered', 'failed', 'retrying');

CREATE TABLE delivery_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id UUID NOT NULL REFERENCES notifications(id),
    channel         delivery_channel NOT NULL,
    status          delivery_status NOT NULL DEFAULT 'queued',
    attempt_count   INT NOT NULL DEFAULT 0,
    max_retries     INT NOT NULL DEFAULT 3,
    next_attempt_at TIMESTAMPTZ,
    delivered_at    TIMESTAMPTZ,
    error_message   TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_delivery_jobs_notification_id ON delivery_jobs(notification_id);
CREATE INDEX idx_delivery_jobs_status          ON delivery_jobs(status);
CREATE INDEX idx_delivery_jobs_next_attempt    ON delivery_jobs(next_attempt_at)
    WHERE status IN ('queued', 'retrying');

-- user_preferences
CREATE TABLE user_preferences (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     UUID NOT NULL UNIQUE,
    email       VARCHAR(320),
    webhook_url TEXT,
    channels    delivery_channel[] NOT NULL DEFAULT '{in_app}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- delivery_logs
CREATE TABLE delivery_logs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_job_id UUID NOT NULL REFERENCES delivery_jobs(id),
    attempt_number  INT NOT NULL,
    status          delivery_status NOT NULL,
    response_code   INT,
    error_detail    TEXT,
    duration_ms     INT,
    attempted_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## Kafka Event Schemas

```json
// topic: notification.created
{
  "event_id":        "uuid",
  "notification_id": "uuid",
  "user_id":         "uuid",
  "event_type":      "order.shipped",
  "title":           "Your order has shipped",
  "body":            "Order #1234 is on the way",
  "metadata":        { "order_id": "1234" },
  "timestamp":       "2026-05-19T10:00:00Z"
}

// topic: delivery.completed
{
  "event_id":        "uuid",
  "delivery_job_id": "uuid",
  "notification_id": "uuid",
  "user_id":         "uuid",
  "channel":         "email",
  "status":          "delivered",
  "attempt_count":   1,
  "timestamp":       "2026-05-19T10:00:01Z"
}

// topic: delivery.failed
{
  "event_id":        "uuid",
  "delivery_job_id": "uuid",
  "notification_id": "uuid",
  "channel":         "webhook",
  "error":           "connection timeout",
  "attempt_count":   3,
  "timestamp":       "2026-05-19T10:00:05Z"
}
```

---

## Key Rust Crates

```toml
[workspace]
resolver = "2"
members = [
    "services/api-gateway",
    "services/notification-worker",
    "services/delivery-service",
]

[workspace.dependencies]
tokio            = { version = "1",    features = ["full"] }
serde            = { version = "1",    features = ["derive"] }
serde_json       = "1"
tracing          = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
uuid             = { version = "1",    features = ["v4", "serde"] }
thiserror        = "2"
anyhow           = "1"
chrono           = { version = "0.4",  features = ["serde"] }

# api-gateway
axum             = { version = "0.8",  features = ["ws"] }
tower-http       = { version = "0.6",  features = ["cors", "trace"] }
sqlx             = { version = "0.8",  features = ["postgres", "uuid", "chrono", "runtime-tokio-rustls"] }
rdkafka          = { version = "0.37", features = ["cmake-build"] }
jsonwebtoken     = "9"
dashmap          = "6"
prometheus       = "0.13"

# delivery-service / notification-worker
tonic            = "0.12"
prost            = "0.13"
tonic-build      = "0.12"   # build.rs
```

---

## Prometheus Metrics

```
# api-gateway  (:9090/metrics)
notifications_created_total        counter  labels: event_type
http_requests_total                counter  labels: method, path, status
http_request_duration_seconds      histogram
active_ws_connections              gauge
kafka_publish_errors_total         counter

# notification-worker  (:9093/metrics)
notifications_routed_total         counter  labels: channel
grpc_calls_total                   counter  labels: service, method, status
routing_duration_seconds           histogram

# delivery-service  (:9091/metrics)
deliveries_succeeded_total         counter  labels: channel
deliveries_failed_total            counter  labels: channel, reason
retry_attempts_total               counter  labels: channel
delivery_duration_seconds          histogram labels: channel
```

---

## Docker Compose (Local Dev)

```yaml
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: notifications
      POSTGRES_USER: notif_user
      POSTGRES_PASSWORD: notif_pass
    ports: ["5432:5432"]
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./migrations:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U notif_user -d notifications"]
      interval: 5s

  redpanda:
    image: redpandadata/redpanda:latest
    command:
      - redpanda
      - start
      - --smp=1
      - --memory=512M
      - --kafka-addr=PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr=PLAINTEXT://redpanda:9092
    ports: ["9092:9092", "9644:9644"]

  redis:
    image: redis:7-alpine
    ports: ["6379:6379"]

  mailhog:
    image: mailhog/mailhog
    ports: ["1025:1025", "8025:8025"]

  delivery-service:
    build: { context: ., dockerfile: services/delivery-service/Dockerfile }
    environment:
      GRPC_PORT: 50051
      DATABASE_URL: postgresql://notif_user:notif_pass@postgres/notifications
      KAFKA_BROKERS: redpanda:9092
    ports: ["50051:50051", "9091:9090"]
    depends_on:
      postgres: { condition: service_healthy }

  notification-worker:
    build: { context: ., dockerfile: services/notification-worker/Dockerfile }
    environment:
      DATABASE_URL: postgresql://notif_user:notif_pass@postgres/notifications
      KAFKA_BROKERS: redpanda:9092
      DELIVERY_SERVICE_URL: http://delivery-service:50051
    ports: ["9093:9090"]
    depends_on:
      postgres: { condition: service_healthy }

  api-gateway:
    build: { context: ., dockerfile: services/api-gateway/Dockerfile }
    environment:
      PORT: 8080
      DATABASE_URL: postgresql://notif_user:notif_pass@postgres/notifications
      KAFKA_BROKERS: redpanda:9092
      REDIS_URL: redis://redis:6379
      JWT_SECRET: local-dev-secret
    ports: ["8080:8080", "9090:9090"]
    depends_on:
      postgres: { condition: service_healthy }

volumes:
  postgres_data:
```

---

## Kubernetes Deployment

```yaml
# k8s/api-gateway/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-gateway
  namespace: notification-service
spec:
  replicas: 2
  selector:
    matchLabels: { app: api-gateway }
  template:
    metadata:
      labels: { app: api-gateway }
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      containers:
        - name: api-gateway
          image: sharifme04/api-gateway:latest
          ports:
            - { containerPort: 8080, name: http }
            - { containerPort: 9090, name: metrics }
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef: { name: notification-secrets, key: database-url }
            - name: JWT_SECRET
              valueFrom:
                secretKeyRef: { name: notification-secrets, key: jwt-secret }
            - name: KAFKA_BROKERS
              valueFrom:
                configMapKeyRef: { name: notification-config, key: kafka-brokers }
          resources:
            requests: { cpu: "100m", memory: "128Mi" }
            limits:   { cpu: "500m", memory: "512Mi" }
          livenessProbe:
            httpGet: { path: /health, port: 8080 }
            initialDelaySeconds: 10
          readinessProbe:
            httpGet: { path: /ready, port: 8080 }
            initialDelaySeconds: 5
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: api-gateway-hpa
  namespace: notification-service
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: api-gateway
  minReplicas: 2
  maxReplicas: 5
  metrics:
    - type: Resource
      resource:
        name: cpu
        target: { type: Utilization, averageUtilization: 70 }
```

---

## CI/CD Pipeline

```yaml
# .github/workflows/ci.yml
name: CI
on:
  push:    { branches: [main] }
  pull_request: { branches: [main] }

jobs:
  rust:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with: { components: "clippy, rustfmt" }
      - name: Install build deps
        run: sudo apt-get install -y cmake protobuf-compiler libssl-dev pkg-config
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --all -- --check
      - run: cargo clippy --all-targets -- -D warnings
      - run: cargo test --all

  docker:
    runs-on: ubuntu-latest
    needs: rust
    strategy:
      matrix:
        service: [api-gateway, notification-worker, delivery-service]
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/build-push-action@v5
        with:
          context: .
          file: services/${{ matrix.service }}/Dockerfile
          push: false
          cache-from: type=gha
          cache-to: type=gha,mode=max
```

---

## Design Decisions

**Redpanda instead of Kafka for local development** — Kafka-API-compatible but runs
without Zookeeper or KRaft setup, so `docker compose up` is a single command.
In production the services connect to any standard Kafka cluster unchanged.

**Synchronous gRPC alongside asynchronous Kafka** — Kafka decouples ingest from
routing at high throughput, but the worker calls the delivery service over gRPC
where it needs a confirmed result per in-flight request. A pure fire-and-forget
Kafka approach would lose the ability to surface delivery status synchronously.

**Retry logic in the delivery service, not at the Kafka layer** — exponential
backoff lives in application code so it is unit-testable and observable via
Prometheus counters, instead of being hidden inside consumer redelivery semantics.

**SQLx compile-time query checking over an ORM** — schema drift becomes a compile
error, and the exact SQL is visible at the call site without abstraction overhead.

**Custom per-IP DashMap rate limiter over Tower's `RateLimitLayer`** — Tower's built-in
rate limiter applies a global limit and produces a non-Clone service that breaks
Axum's router. The custom middleware tracks per-IP sliding windows in a DashMap,
is Clone-safe, and integrates cleanly as an Axum `from_fn_with_state` layer.

**JWT auth as an Axum `FromRequestParts` extractor** — handlers declare auth as a
typed parameter (`_auth: AuthUser`) rather than relying on middleware ordering.
The compiler rejects handlers that forget authentication.

---

## License

MIT — see [LICENSE](LICENSE).

---

*Author: Md. Sharif Hossain — [github.com/sharifme04](https://github.com/sharifme04)*
