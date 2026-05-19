FROM rust:1.83-slim AS builder
WORKDIR /build

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev cmake protobuf-compiler ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY proto ./proto
COPY services ./services

RUN cargo build --release --bin notification-worker

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/target/release/notification-worker /usr/local/bin/notification-worker
EXPOSE 9090
ENTRYPOINT ["/usr/local/bin/notification-worker"]
