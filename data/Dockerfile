# ------------------ Stage 1: Build Rust binaries -----------------------
FROM rust:1.88.0-slim AS builder

WORKDIR /app
COPY . .

RUN apt-get update && apt-get install -y \
    build-essential \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN cargo build --release


# ------------------ Stage 2: Runtime Image -----------------------------
FROM python:3.11-slim

WORKDIR /app

COPY --from=builder /app/target/release/trigger ./trigger
COPY --from=builder /app/target/release/websocket ./websocket
COPY . .

RUN apt-get update && apt-get install -y \
    git \
    ca-certificates \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONUNBUFFERED=1
ENV LOG_LEVEL=DEBUG

# ------------------ CMD: Start trigger and websocket concurrently ------
CMD ["sh", "-c", "\
    echo '🟢 Container started at: $(date)'; \
    echo '📂 Working dir: $(pwd)'; \
    echo '🐍 Python:'; python3 --version; \
    echo '🦀 Rust binaries in:'; ls -lh trigger websocket; \
    echo '📦 Python packages:'; pip list; \
    echo '=== ENV CHECK ==='; \
    env | grep -E 'DATABASE_URL|REDIS_URL' || true; \
    python3 - <<'PY'\n\
import os, re, sys, asyncio, asyncpg, redis\n\
# Mask passwords when printing\n\
def mask(url):\n\
    return re.sub(r':[^@]+@', ':***@', url)\n\
\n\
# --- Check DATABASE_URL ---\n\
pg_url = os.getenv('DATABASE_URL')\n\
if not pg_url:\n\
    print('❌ DATABASE_URL is missing or empty!', file=sys.stderr)\n\
    sys.exit(1)\n\
print('[env] DATABASE_URL =', mask(pg_url))\n\
pg_url = pg_url.replace('postgresql+asyncpg://', 'postgresql://')\n\
\n\
# --- Check REDIS_URL ---\n\
rd_url = os.getenv('REDIS_URL')\n\
if not rd_url:\n\
    print('❌ REDIS_URL is missing or empty!', file=sys.stderr)\n\
    sys.exit(1)\n\
print('[env] REDIS_URL =', mask(rd_url))\n\
\n\
# --- Redis connection check ---\n\
try:\n\
    r = redis.Redis.from_url(rd_url)\n\
    print('[health] Redis ping ->', r.ping())\n\
except Exception as e:\n\
    print('❌ Redis connection failed:', e, file=sys.stderr)\n\
    sys.exit(1)\n\
\n\
# --- Postgres connection check ---\n\
async def check_pg():\n\
    try:\n\
        conn = await asyncpg.connect(pg_url)\n\
        await conn.close()\n\
        print('[health] Postgres OK')\n\
    except Exception as e:\n\
        print('❌ Postgres connection failed:', e, file=sys.stderr)\n\
        sys.exit(1)\n\
\n\
asyncio.run(check_pg())\n\
PY\n\
    echo '🚀 Launching trigger and websocket...'; \
    ./trigger & \
    ./websocket"]
