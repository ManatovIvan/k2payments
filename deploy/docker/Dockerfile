# ── Stage 1: Build ────────────────────────────────────────────────
FROM rust:1-bookworm AS builder

WORKDIR /build

# Install system dependencies required by workspace crates
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    clang \
    cmake \
    liblz4-dev \
    libpq-dev \
    libsnappy-dev \
    libsqlite3-dev \
    libssl-dev \
    libzstd-dev \
    pkg-config \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace manifests first for better layer caching
COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY proto/ proto/

# Build the runtime binary in release profile
RUN cargo build --release --bin mxruntime

# ── Stage 2: Runtime ──────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    libpq5 \
    libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

# Non-root service account
RUN useradd --system --no-create-home --shell /usr/sbin/nologin mxruntime

# Copy the compiled binary from the builder stage
COPY --from=builder /build/target/release/mxruntime /usr/local/bin/mxruntime

# Config mount point
RUN mkdir -p /etc/mx && chown mxruntime:mxruntime /etc/mx

# Default port layout:
#   8443 – message channel (HTTP/gRPC inbound)
#   9090 – admin HTTP  (/health, /ready, /status, /metrics)
#   9091 – admin gRPC
EXPOSE 8443 9090 9091

USER mxruntime

# Liveness probe – hits /health which requires no auth
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD curl -sf http://localhost:9090/health || exit 1

ENTRYPOINT ["/usr/local/bin/mxruntime"]
CMD ["--config", "/etc/mx/config.toml", "--serve-admin"]
