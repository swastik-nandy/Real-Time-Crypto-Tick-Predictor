# ------------------ Stage 1: Build Rust binaries -----------------------
FROM rust:1.88.0-slim AS builder

# Set working directory inside container
WORKDIR /app

# Copy everything from data/ into container
COPY . .

# Install required tools for compiling Redis/Postgres dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Build Rust binaries (trigger.rs and websocket.rs should be in src/bin/)
RUN cargo build --release


# ------------------ Stage 2: Runtime Image -----------------------------
FROM python:3.11-slim

# Set working directory for runtime container
WORKDIR /app

# Copy built binaries from builder
COPY --from=builder /app/target/release/trigger ./trigger
COPY --from=builder /app/target/release/websocket ./websocket

# Copy scripts, .env, and push.py-related files
COPY . .

# Install runtime tools (for GitHub push)
RUN apt-get update && apt-get install -y \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies (used in push.py)
RUN pip install --no-cache-dir -r requirements.txt

# Environment setup
ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=DEBUG

# ------------------ CMD: Start trigger and websocket concurrently ------
CMD ["sh", "-c", "\
    echo '🟢 Container started at: $(date)'; \
    echo '📂 Working dir: $(pwd)'; \
    echo '🐍 Python:'; python3 --version; \
    echo '🦀 Rust binaries in:'; ls -lh trigger websocket; \
    echo '📦 Python packages:'; pip list; \
    echo '🔍 Checking Redis connection...'; \
    python3 -c 'import os, redis; print(redis.Redis.from_url(os.environ.get(\"REDIS_URL\")).ping())'; \
    echo '🔍 Checking Postgres connection...'; \
    python3 -c 'import asyncpg, asyncio, os; asyncio.run(asyncpg.connect(os.environ[\"DATABASE_URL\"]))'; \
    echo '🚀 Launching trigger and websocket...'; \
    ./trigger & \
    ./websocket"]
