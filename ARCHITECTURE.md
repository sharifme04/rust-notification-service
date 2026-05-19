# rust-notification-service
## Distributed Notification Platform — Architecture & Implementation Plan
**GitHub:** [sharifme04/rust-notification-service](https://github.com/sharifme04/rust-notification-service)

> **Goal:** A production-quality distributed notification service demonstrating
> idiomatic async Rust, gRPC (Tonic), Kafka event-driven messaging, PostgreSQL,
> Kubernetes orchestration, and a real-time Angular dashboard.

---

## Skills & Patterns Demonstrated

| Capability | Implementation |
|---|---|
| Async Rust backend | Rust (Axum + Tonic gRPC + Tokio async runtime) |
| Distributed systems | Three independently deployable services: API gateway, notification worker, delivery service |
| gRPC + Protobuf | Tonic gRPC server/client between internal services |
| Event-driven messaging | Kafka topics for notification lifecycle events |
| Kubernetes + Docker | Multi-stage Dockerfiles, full K8s manifests, HPA, minikube-ready |
| PostgreSQL / SQL | Schema migrations via SQLx, compile-time query checking, indexed lookups |
| Real-time frontend | Angular 17+ dashboard with WebSocket-driven live feed |
| Cloud & CI/CD | Docker Compose local → K8s → GitHub Actions pipeline |
| Observability | Prometheus metrics exposed on every service, Grafana dashboards |

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
│  │   - Channel management (email / push / webhook)               │   │
│  │   - Prometheus metrics viewer                                 │   │
│  └──────────────┬───────────────────────────────────────────────┘   │
└─────────────────┼───────────────────────────────────────────────────┘
                  │ HTTP REST + WebSocket
┌─────────────────▼───────────────────────────────────────────────────┐
│                    API GATEWAY SERVICE (Rust/Axum)                   │
│                                                                      │
│   • REST endpoints: POST /notify, GET /notifications, GET /status    │
│   • WebSocket hub: broadcasts delivery events to connected clients   │
│   • Auth middleware (JWT validation)                                 │
│   • Rate limiting (tower middleware)                                 │
│   • Publishes NotificationCreated events → Kafka                     │
│   • Exposes /metrics (Prometheus)                                    │
│   • Port: 8080 (REST) / 8081 (WebSocket)                            │
└─────────────────┬───────────────────────────────────────────────────┘
                  │ Kafka: topic=notification.created
┌─────────────────▼───────────────────────────────────────────────────┐
│               NOTIFICATION WORKER SERVICE (Rust)                     │
│                                                                      │
│   • Consumes Kafka topic: notification.created                       │
│   • Applies routing rules: which channels to use per user/event      │
│   • Calls Delivery Service via gRPC (Tonic)                         │
│   • Writes delivery jobs to PostgreSQL                               │
│   • Publishes NotificationRouted events → Kafka                      │
│   • Exposes /metrics (Prometheus)                                    │
└──────────┬──────────────────────────┬───────────────────────────────┘
           │ gRPC (Tonic)             │ gRPC (Tonic)
┌──────────▼──────────┐   ┌──────────▼──────────────────────────────┐
│  DELIVERY SERVICE   │   │  PostgreSQL                              │
│  (Rust/Tonic)       │   │                                          │
│                     │   │  • notifications table                   │
│  Proto: DeliverRPC  │   │  • delivery_jobs table                   │
│  Channels:          │   │  • user_preferences table                │
│  • Email (SMTP)     │   │  • delivery_logs table                   │
│  • Webhook (HTTP)   │   │                                          │
│  • In-app (WS)      │   └──────────────────────────────────────────┘
│                     │
│  Retries with       │   ┌──────────────────────────────────────────┐
│  exponential        │   │  Redis                                   │
│  backoff            │   │  • WebSocket session store               │
│                     │   │  • Rate limit counters                   │
│  Publishes:         │   │  • Notification dedup cache              │
│  DeliveryCompleted  │   └──────────────────────────────────────────┘
│  → Kafka            │
└─────────────────────┘   ┌──────────────────────────────────────────┐
                           │  Kafka (Redpanda or Kafka)               │
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
│   └── notification/
│       └── v1/
│           ├── delivery.proto          # DeliveryService gRPC definition
│           └── types.proto             # Shared message types
│   # Each service that consumes the proto has its own build.rs invoking tonic-build
│
├── services/
│   ├── api-gateway/                    # Rust (Axum) — REST + WebSocket
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── routes/
│   │   │   │   ├── notifications.rs
│   │   │   │   └── health.rs
│   │   │   ├── websocket/
│   │   │   │   ├── hub.rs              # WebSocket connection manager
│   │   │   │   └── handler.rs
│   │   │   ├── kafka/
│   │   │   │   └── producer.rs
│   │   │   ├── middleware/
│   │   │   │   ├── auth.rs
│   │   │   │   └── rate_limit.rs
│   │   │   └── metrics.rs
│   │   ├── Cargo.toml
│   │   └── Dockerfile
│   │
│   ├── notification-worker/            # Rust — Kafka consumer + gRPC client
│   │   ├── src/
│   │   │   ├── main.rs
│   │   │   ├── consumer.rs             # Kafka consumer loop
│   │   │   ├── router.rs               # Routing logic
│   │   │   ├── grpc_client.rs          # Tonic client for delivery service
│   │   │   ├── db/
│   │   │   │   ├── mod.rs
│   │   │   │   └── jobs.rs
│   │   │   └── metrics.rs
│   │   ├── Cargo.toml
│   │   └── Dockerfile
│   │
│   └── delivery-service/               # Rust (Tonic gRPC server)
│       ├── src/
│       │   ├── main.rs
│       │   ├── grpc/
│       │   │   └── delivery_impl.rs    # DeliveryService trait impl
│       │   ├── channels/
│       │   │   ├── email.rs            # SMTP delivery
│       │   │   ├── webhook.rs          # HTTP webhook delivery
│       │   │   └── inapp.rs            # In-app via Kafka event
│       │   ├── retry.rs                # Exponential backoff
│       │   └── metrics.rs
│       ├── Cargo.toml
│       └── Dockerfile
│
├── dashboard/                          # Angular frontend
│   ├── src/
│   │   ├── app/
│   │   │   ├── components/
│   │   │   │   ├── notification-feed/  # Real-time WebSocket feed
│   │   │   │   ├── notification-list/  # History + filters
│   │   │   │   ├── metrics-panel/      # Prometheus charts
│   │   │   │   └── channel-config/     # User preferences
│   │   │   ├── services/
│   │   │   │   ├── notification.service.ts
│   │   │   │   ├── websocket.service.ts
│   │   │   │   └── api.service.ts
│   │   │   └── models/
│   │   │       └── notification.model.ts
│   │   └── environments/
│   ├── angular.json
│   └── Dockerfile
│
├── migrations/                         # SQLx migrations
│   ├── 001_create_notifications.sql
│   ├── 002_create_delivery_jobs.sql
│   ├── 003_create_user_preferences.sql
│   └── 004_create_delivery_logs.sql
│
├── k8s/                                # Kubernetes manifests
│   ├── namespace.yaml
│   ├── api-gateway/
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   └── hpa.yaml                   # Horizontal Pod Autoscaler
│   ├── notification-worker/
│   │   ├── deployment.yaml
│   │   └── service.yaml
│   ├── delivery-service/
│   │   ├── deployment.yaml
│   │   └── service.yaml
│   ├── dashboard/
│   │   ├── deployment.yaml
│   │   └── service.yaml
│   ├── configmaps/
│   │   └── app-config.yaml
│   ├── secrets/
│   │   └── secrets.yaml.example
│   └── ingress.yaml
│
├── helm/                               # Helm chart (optional phase 4)
│   └── notification-service/
│       ├── Chart.yaml
│       ├── values.yaml
│       └── templates/
│
├── docker-compose.yml                  # Local dev: all services + infra
├── docker-compose.infra.yml            # Kafka, PostgreSQL, Redis only
├── Cargo.toml                          # Workspace root
├── Cargo.lock
├── .github/
│   └── workflows/
│       ├── ci.yml                      # Test + lint + build
│       └── docker-build.yml            # Build + push images
└── README.md
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
  string user_id = 2;
  Channel channel = 3;
  Payload payload = 4;
  DeliveryOptions options = 5;
}

message Payload {
  string title = 1;
  string body = 2;
  map<string, string> metadata = 3;
}

message DeliveryOptions {
  uint32 max_retries = 1;
  uint32 retry_delay_ms = 2;
  uint32 timeout_ms = 3;
}

enum Channel {
  CHANNEL_UNSPECIFIED = 0;
  CHANNEL_EMAIL = 1;
  CHANNEL_WEBHOOK = 2;
  CHANNEL_IN_APP = 3;
}

message DeliverResponse {
  string delivery_id = 1;
  DeliveryStatus status = 2;
  string error_message = 3;
}

enum DeliveryStatus {
  STATUS_UNSPECIFIED = 0;
  STATUS_QUEUED = 1;
  STATUS_DELIVERED = 2;
  STATUS_FAILED = 3;
  STATUS_RETRYING = 4;
}

message DeliverBatchRequest {
  repeated DeliverRequest requests = 1;
}

message DeliverBatchResponse {
  string notification_id = 1;
  DeliveryStatus status = 2;
}

message DeliveryStatusRequest {
  string delivery_id = 1;
}

message DeliveryStatusResponse {
  string delivery_id = 1;
  DeliveryStatus status = 2;
  uint32 attempt_count = 3;
  string last_error = 4;
}
```

---

## PostgreSQL Schema

```sql
-- migrations/000_extensions.sql
-- gen_random_uuid() ships in PG 13+ core, but the extension makes intent explicit
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- migrations/001_create_notifications.sql
CREATE TABLE notifications (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_notifications_user_id ON notifications(user_id);
CREATE INDEX idx_notifications_created_at ON notifications(created_at DESC);

-- migrations/002_create_delivery_jobs.sql
CREATE TYPE delivery_channel AS ENUM ('email', 'webhook', 'in_app');
CREATE TYPE delivery_status AS ENUM ('queued', 'delivered', 'failed', 'retrying');

CREATE TABLE delivery_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    notification_id UUID NOT NULL REFERENCES notifications(id),
    channel delivery_channel NOT NULL,
    status delivery_status NOT NULL DEFAULT 'queued',
    attempt_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    next_attempt_at TIMESTAMPTZ,
    delivered_at TIMESTAMPTZ,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX idx_delivery_jobs_notification_id ON delivery_jobs(notification_id);
CREATE INDEX idx_delivery_jobs_status ON delivery_jobs(status);
CREATE INDEX idx_delivery_jobs_next_attempt ON delivery_jobs(next_attempt_at)
    WHERE status IN ('queued', 'retrying');

-- migrations/003_create_user_preferences.sql
CREATE TABLE user_preferences (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id UUID NOT NULL UNIQUE,
    email VARCHAR(320),
    webhook_url TEXT,
    channels delivery_channel[] NOT NULL DEFAULT '{in_app}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- migrations/004_create_delivery_logs.sql
CREATE TABLE delivery_logs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_job_id UUID NOT NULL REFERENCES delivery_jobs(id),
    attempt_number INT NOT NULL,
    status delivery_status NOT NULL,
    response_code INT,
    error_detail TEXT,
    duration_ms INT,
    attempted_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

---

## Kafka Events (JSON Schema)

```json
// topic: notification.created
{
  "event_id": "uuid",
  "notification_id": "uuid",
  "user_id": "uuid",
  "event_type": "order.shipped",
  "title": "Your order has shipped",
  "body": "Order #1234 is on the way",
  "metadata": { "order_id": "1234" },
  "timestamp": "2026-05-11T10:00:00Z"
}

// topic: delivery.completed
{
  "event_id": "uuid",
  "delivery_job_id": "uuid",
  "notification_id": "uuid",
  "user_id": "uuid",
  "channel": "email",
  "status": "delivered",
  "attempt_count": 1,
  "timestamp": "2026-05-11T10:00:01Z"
}

// topic: delivery.failed
{
  "event_id": "uuid",
  "delivery_job_id": "uuid",
  "notification_id": "uuid",
  "channel": "webhook",
  "error": "connection timeout",
  "attempt_count": 3,
  "timestamp": "2026-05-11T10:00:05Z"
}
```

---

## Key Rust Crates

```toml
# Workspace Cargo.toml
[workspace]
resolver = "2"
members = [
    "services/api-gateway",
    "services/notification-worker",
    "services/delivery-service",
]

# Shared across all services
[workspace.dependencies]
tokio = { version = "1", features = ["full"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter", "json"] }
uuid = { version = "1", features = ["v4", "serde"] }
thiserror = "2"
anyhow = "1"
chrono = { version = "0.4", features = ["serde"] }
config = "0.14"

# Per-service additions:
# api-gateway:
axum = { version = "0.8", features = ["ws"] }
tower = "0.5"
tower-http = { version = "0.6", features = ["cors", "trace"] }
sqlx = { version = "0.8", features = ["postgres", "uuid", "chrono", "runtime-tokio-rustls"] }
rdkafka = { version = "0.37", features = ["cmake-build"] }
redis = { version = "0.27", features = ["tokio-comp"] }
jsonwebtoken = "9"
prometheus = "0.13"
axum-prometheus = "0.7"

# delivery-service:
tonic = "0.12"
prost = "0.13"
tonic-build = "0.12"  # build.rs

# notification-worker:
tonic = "0.12"         # gRPC client
rdkafka = "0.37"
```

---

## Implementation Phases

### Phase 1 — Infrastructure + Skeleton (Days 1–3)

**Goal:** Everything running locally, services talk to each other.

```
Tasks:
□ docker-compose.yml: Kafka (Redpanda), PostgreSQL, Redis, Zookeeper
□ Cargo workspace: three service crates
□ SQLx migrations: run and verify all 4 tables
□ Proto definitions written and compiled (build.rs with tonic-build)
□ api-gateway: basic Axum server, health endpoint, /metrics
□ delivery-service: Tonic server skeleton, DeliverRPC returns mock response
□ notification-worker: Kafka consumer loop (print received messages)
□ All three services start and connect to infra without errors
□ README: local dev setup instructions

Quality bar:
- clippy --deny warnings passes
- All services have structured logging (tracing + JSON format)
- Config loaded from environment variables (config crate)
```

### Phase 2 — Core Flow (Days 4–8)

**Goal:** End-to-end notification creation → Kafka → routing → gRPC delivery → DB write.

```
Tasks:
□ api-gateway:
  - POST /api/v1/notifications: validate, write to DB, publish to Kafka
  - GET /api/v1/notifications/:id: fetch from DB
  - GET /api/v1/notifications?user_id=&limit=&offset=: paginated list
  - JWT middleware (HS256, configurable secret)
  - Rate limiting middleware (tower, per-IP)

□ notification-worker:
  - Consume notification.created topic (rdkafka consumer group)
  - Load user preferences from DB
  - Determine delivery channels per preference
  - Call DeliveryService via Tonic gRPC for each channel
  - Write delivery_jobs to DB
  - Publish notification.routed to Kafka

□ delivery-service:
  - Implement DeliverRPC fully
  - Email channel: use lettre crate (SMTP to Mailhog in docker-compose)
  - Webhook channel: reqwest HTTP POST with timeout
  - In-app channel: publish delivery.completed to Kafka
  - Write delivery_logs on each attempt
  - Exponential backoff retry (tokio::time::sleep)

□ Integration test: POST /notify → verify delivery_jobs row in DB

Quality bar:
- Unit tests for routing logic
- Unit tests for retry backoff calculation
- sqlx compile-time query checks (query!)
- Error types with thiserror, no unwrap() in production paths
```

### Phase 3 — Real-Time + Dashboard (Days 9–13)

**Goal:** Angular dashboard with live WebSocket feed + full CRUD.

```
Tasks:
□ api-gateway WebSocket:
  - WS endpoint: /ws?user_id=
  - Hub: HashMap<UserId, Vec<Sender>>
  - Consume delivery.completed from Kafka → broadcast to relevant WS clients
  - Redis session store for WS connection tracking

□ Angular dashboard (standalone components, Angular 17+):
  - notification-feed component: connects to WS, appends real-time items
  - notification-list component: paginated table, filter by status/channel
  - channel-config component: GET/PUT user preferences
  - metrics-panel: iframe or chart.js pulling Prometheus /metrics
  - HttpClient service, WebSocket service
  - Angular Material or TailwindCSS for styling

□ CORS configured on api-gateway (tower-http)
□ Docker build for Angular (nginx:alpine, multi-stage)

Quality bar:
- Angular unit tests (Jasmine) for services
- E2E happy path manually verified
- No console errors in browser
```

### Phase 4 — Kubernetes + Observability (Days 14–18)

**Goal:** Everything deployed to minikube, Prometheus scraping all services.

```
Tasks:
□ Dockerfiles: multi-stage (builder → runtime), minimal final images
□ k8s/namespace.yaml: notification-service namespace
□ Deployments: 3 backend services + Angular frontend
□ Services: ClusterIP for internal, LoadBalancer/NodePort for gateway
□ ConfigMap: non-secret config (Kafka brokers, DB host)
□ Secret: DB password, JWT secret, SMTP credentials
□ Ingress: nginx ingress controller, route / to dashboard, /api to gateway
□ HPA: api-gateway scales 2–5 replicas on CPU > 70%
□ Prometheus ServiceMonitor or scrape annotations on all pods
□ Grafana dashboard: request rate, delivery success rate, Kafka lag
□ Resource requests/limits on all containers
□ Liveness + readiness probes on all services
□ GitHub Actions CI:
  - cargo test --all
  - cargo clippy -- -D warnings
  - cargo fmt --check
  - docker build all images

Quality bar:
- kubectl get pods: all Running
- kubectl logs: no ERROR lines at startup
- Prometheus targets: all UP
- HPA verified with load test (k6 or hey)
```

### Phase 5 — Polish + Documentation (Days 19–21)

```
Tasks:
□ README.md:
  - Architecture diagram (ASCII + Mermaid)
  - Local quickstart: docker-compose up → curl POST /notify → see in dashboard
  - K8s quickstart: minikube start → kubectl apply -k k8s/ → open dashboard
  - Design decisions section (why Redpanda vs Kafka, why Tonic, why this schema)
  - Known limitations / future improvements

□ ARCHITECTURE.md: this document, trimmed for public repo
□ Postman collection or .http file for all REST endpoints
□ Load test script (k6): 100 notifications/sec, verify no message loss
□ Code comments on non-obvious parts (retry logic, WS hub, gRPC streaming)

Quality bar:
- Someone unfamiliar can run the project in 5 minutes from README
- All public types and functions have doc comments
- cargo doc builds without warnings
```

---

## Prometheus Metrics to Expose

```rust
// Each service exposes these on :9090/metrics

// api-gateway
notifications_created_total (counter, labels: event_type)
http_requests_total (counter, labels: method, path, status)
http_request_duration_seconds (histogram)
websocket_connections_active (gauge)
kafka_publish_errors_total (counter)

// notification-worker
notifications_routed_total (counter, labels: channel)
kafka_consumer_lag (gauge, labels: topic, partition)
grpc_calls_total (counter, labels: service, method, status)
routing_duration_seconds (histogram)

// delivery-service
deliveries_attempted_total (counter, labels: channel, status)
deliveries_succeeded_total (counter, labels: channel)
deliveries_failed_total (counter, labels: channel, reason)
retry_attempts_total (counter, labels: channel)
delivery_duration_seconds (histogram, labels: channel)
```

---

## Docker Compose (Local Dev)

```yaml
# docker-compose.yml
# (the obsolete `version:` field is intentionally omitted — Compose v2+ ignores it)

services:
  # Infrastructure
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_DB: notifications
      POSTGRES_USER: notif_user
      POSTGRES_PASSWORD: notif_pass
    ports: ["5432:5432"]
    volumes: [postgres_data:/var/lib/postgresql/data]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U notif_user"]
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

  # Application services
  delivery-service:
    build: ./services/delivery-service
    environment:
      GRPC_PORT: 50051
      DATABASE_URL: postgresql://notif_user:notif_pass@postgres/notifications
      KAFKA_BROKERS: redpanda:9092
      SMTP_HOST: mailhog
      SMTP_PORT: 1025
    ports: ["50051:50051", "9091:9090"]
    depends_on: [postgres, redpanda, mailhog]

  notification-worker:
    build: ./services/notification-worker
    environment:
      DATABASE_URL: postgresql://notif_user:notif_pass@postgres/notifications
      KAFKA_BROKERS: redpanda:9092
      KAFKA_GROUP_ID: notification-worker
      DELIVERY_SERVICE_URL: http://delivery-service:50051
    ports: ["9093:9090"]   # metrics — host 9092 is taken by Redpanda
    depends_on: [postgres, redpanda, delivery-service]

  api-gateway:
    build: ./services/api-gateway
    environment:
      PORT: 8080
      WS_PORT: 8081
      DATABASE_URL: postgresql://notif_user:notif_pass@postgres/notifications
      KAFKA_BROKERS: redpanda:9092
      REDIS_URL: redis://redis:6379
      JWT_SECRET: local-dev-secret
    ports: ["8080:8080", "8081:8081", "9090:9090"]   # 9090 = metrics
    depends_on: [postgres, redpanda, redis]

  dashboard:
    build: ./dashboard
    ports: ["4200:80"]
    depends_on: [api-gateway]

volumes:
  postgres_data:
```

---

## Kubernetes Deployment Example

```yaml
# k8s/api-gateway/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: api-gateway
  namespace: notification-service
  labels:
    app: api-gateway
spec:
  replicas: 2
  selector:
    matchLabels:
      app: api-gateway
  template:
    metadata:
      labels:
        app: api-gateway
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: api-gateway
          image: sharifme04/api-gateway:latest
          ports:
            - containerPort: 8080
              name: http
            - containerPort: 8081
              name: websocket
            - containerPort: 9090
              name: metrics
          env:
            - name: DATABASE_URL
              valueFrom:
                secretKeyRef:
                  name: notification-secrets
                  key: database-url
            - name: JWT_SECRET
              valueFrom:
                secretKeyRef:
                  name: notification-secrets
                  key: jwt-secret
            - name: KAFKA_BROKERS
              valueFrom:
                configMapKeyRef:
                  name: notification-config
                  key: kafka-brokers
          resources:
            requests:
              cpu: "100m"
              memory: "128Mi"
            limits:
              cpu: "500m"
              memory: "512Mi"
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /ready
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 10
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
        target:
          type: Utilization
          averageUtilization: 70
```

---

## GitHub Actions CI

```yaml
# .github/workflows/ci.yml
name: CI

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_PASSWORD: test
          POSTGRES_DB: notifications_test
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports: ["5432:5432"]

    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: clippy, rustfmt

      - name: Cache cargo
        uses: Swatinem/rust-cache@v2

      - name: Check formatting
        run: cargo fmt --all -- --check

      - name: Clippy
        run: cargo clippy --all-targets -- -D warnings

      - name: Run tests
        run: cargo test --all
        env:
          DATABASE_URL: postgresql://postgres:test@localhost/notifications_test

      - name: Build all images
        run: |
          docker build -t api-gateway ./services/api-gateway
          docker build -t notification-worker ./services/notification-worker
          docker build -t delivery-service ./services/delivery-service
          docker build -t dashboard ./dashboard
```

---

## Design Decisions

A few choices in this architecture are non-obvious and worth calling out:

- **Redpanda instead of full Kafka for local dev** — Kafka-API-compatible but runs
  without Zookeeper/KRaft setup, so `docker-compose up` is a single step.
- **Sync gRPC alongside async Kafka** — Kafka decouples ingest from routing, but the
  worker calls the delivery service over gRPC where it needs a confirmed result for
  the same in-flight request. Using only Kafka would force everything through
  fire-and-forget, losing the ability to surface delivery status synchronously.
- **Retry logic in the delivery service, not at the Kafka layer** — exponential
  backoff lives in application code so it's unit-testable and observable via metrics,
  instead of being hidden inside consumer redelivery semantics.
- **`sqlx` with compile-time query checking rather than an ORM** — schema drift
  becomes a compile error, and the exact SQL is visible at the call site.
- **Separate WebSocket port (8081)** — keeps WS upgrades isolated from REST middleware
  (rate limiting, JWT) which apply differently to long-lived connections.

---

## Recommended Build Order (Summary)

```
Week 1: Phase 1 + Phase 2
  Day 1-3: Infrastructure + skeletons
  Day 4-8: Full notification flow works end-to-end

Week 2: Phase 3 + Phase 4
  Day 9-13: Angular dashboard + WebSocket
  Day 14-18: Kubernetes deployment + Prometheus

Week 3: Phase 5
  Day 19-21: Polish, load test, documentation
```

---

## Notes on Code Quality Standards

- **No `unwrap()` or `expect()` in production code paths** — use `?` with proper error types
- **`thiserror` for domain errors**, `anyhow` only at application entry points
- **All async I/O non-blocking** — no `std::thread::sleep`, only `tokio::time::sleep`
- **Structured logging everywhere** — `tracing::info!(notification_id = %id, "created")`
- **Config from environment** — no hardcoded values, use the `config` crate
- **Compile-time SQL** — `sqlx::query!` macro wherever possible
- **Clippy deny warnings** — `#![deny(clippy::all)]` at crate root
- **Tests for all business logic** — routing rules, retry calculation, event parsing
- **Integration test** — at minimum one happy-path test per service using testcontainers or docker-compose

---

## License

MIT — see [LICENSE](LICENSE).

---

*Architecture version 1.0 — May 2026*
*Author: Md. Sharif Hossain — [github.com/sharifme04](https://github.com/sharifme04)*
