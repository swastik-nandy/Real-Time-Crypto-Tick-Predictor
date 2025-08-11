# ================== Stage 1: Build Rust binaries ==================
FROM rust:1.88-slim AS builder
WORKDIR /app

# Install build deps for Rust (TLS support)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential pkg-config libssl-dev ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Copy project files into builder
COPY . .

# Build only the two required binaries
RUN cargo build --release --locked --bin trigger --bin websocket


# ================== Stage 2: Final runtime ==================
FROM python:3.11-slim
WORKDIR /app

# Install runtime deps (TLS + Git for push.py)
RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates libssl3 tzdata \
 && rm -rf /var/lib/apt/lists/*

# Copy compiled Rust binaries from builder
COPY --from=builder /app/target/release/trigger .
COPY --from=builder /app/target/release/websocket .

# Install Python deps first for caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app (includes scripts/push.py and src/)
COPY . .

# Default envs
ENV PYTHONUNBUFFERED=1 LOG_LEVEL=DEBUG

# Start trigger and websocket together
CMD ./trigger & ./websocket
