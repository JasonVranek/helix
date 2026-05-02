# Helix Relay — Docker build for Kurtosis
# Build context: pbs-stack/
#
# The Kurtosis helix_relay_launcher mounts a config at /config/config.yaml
# and runs: helix-relay --config /config/config.yaml
# Ports: 4040 (API), 9060 (website)
# Env: RELAY_KEY (set by launcher)

FROM rust:1.91-bookworm AS builder
WORKDIR /src
COPY helix/ ./helix/
COPY ws-wire/ ./ws-wire/
WORKDIR /src/helix
RUN cargo build --release -p helix-relay

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /src/helix/target/release/helix-relay /usr/local/bin/helix-relay
EXPOSE 4040 9060
ENTRYPOINT ["/usr/local/bin/helix-relay"]
