# Helix Relay — Docker build for interop testing
# Build context: pbs-stack/
# Build: docker compose -f docker-compose.test.yml build
FROM rust:1.91-bookworm AS builder
WORKDIR /src

# Copy helix source and ws-wire
COPY helix/ ./helix/
COPY ws-wire/ ./ws-wire/

# Fix ws-wire path — inside Docker, ws-wire is at ../../../ws-wire (sibling to helix/)
RUN sed -i 's|path = "../../../pbs-stack/ws-wire"|path = "../../../ws-wire"|g' helix/crates/relay/Cargo.toml

# Build in helix workspace
WORKDIR /src/helix
RUN cargo build --release -p helix-relay

# Runtime
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/helix/target/release/helix-relay /usr/local/bin/helix-relay
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/helix-relay"]
