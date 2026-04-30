# Helix Relay — Multi-stage Docker build for interop testing
# Build: docker build -t local/helix-relay:ws-dev .
FROM rust:1.91-bookworm AS builder
WORKDIR /src

# Copy workspace Cargo.toml + crate stubs first for layer caching
COPY Cargo.toml Cargo.lock ./
COPY crates/common/Cargo.toml crates/common/
COPY crates/types/Cargo.toml crates/types/
COPY crates/database/Cargo.toml crates/database/
COPY crates/data-api/Cargo.toml crates/data-api/
COPY crates/tcp-types/Cargo.toml crates/tcp-types/
COPY crates/simulator/Cargo.toml crates/simulator/
COPY crates/relay/Cargo.toml crates/relay/

# Create stub lib.rs files for dependency resolution
RUN mkdir -p crates/common/src crates/types/src crates/database/src crates/data-api/src crates/tcp-types/src crates/simulator/src crates/relay/src && \
    echo "pub fn dummy() {}" > crates/common/src/lib.rs && \
    echo "pub fn dummy() {}" > crates/types/src/lib.rs && \
    echo "pub fn dummy() {}" > crates/database/src/lib.rs && \
    echo "pub fn dummy() {}" > crates/data-api/src/lib.rs && \
    echo "pub fn dummy() {}" > crates/tcp-types/src/lib.rs && \
    echo "pub fn dummy() {}" > crates/simulator/src/lib.rs && \
    echo "fn main() {}" > crates/relay/src/main.rs

# Fetch dependencies (cached layer)
RUN cargo build --release -p helix-relay 2>/dev/null || true

# Now copy real source
COPY . .

# Build the relay binary
RUN cargo build --release -p helix-relay

# Runtime stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 curl && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/target/release/helix-relay /usr/local/bin/helix-relay
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/helix-relay"]
