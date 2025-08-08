import asyncio
import websockets
import json
import logging
from datetime import datetime, timezone
from typing import Set, Dict
import os
import sys
from pathlib import Path
from redis.asyncio import Redis
from dotenv import load_dotenv

# --- Logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("websocket-streamer")

# --- ENV Setup ---
sys.path.append(str(Path(__file__).resolve().parent))
if not os.environ.get("ENV"):
    load_dotenv(Path(__file__).resolve().parents[1] / ".env")

FINNHUB_API_KEY = os.environ["FINNHUB_API_KEY"]
REDIS_URL = os.environ["REDIS_URL"]
FINNHUB_WS_URL = f"wss://ws.finnhub.io?token={FINNHUB_API_KEY}"

SYMBOLS_KEY = "stock:symbols"
PRICE_PREFIX = "stock:price:"
TRADE_PREFIX = "stock:trade:"
OHLCV_PREFIX = "stock:ohlcv:"

# --- In-memory OHLCV Buffers ---
ohlcv_buffer: Dict[str, list] = {}

# --- Redis Utils ---
async def get_symbols(redis: Redis) -> Set[str]:
    try:
        symbols = await redis.smembers(SYMBOLS_KEY)
        return {s.decode() if isinstance(s, bytes) else s for s in symbols}
    except Exception as e:
        logger.error(f"[Redis] Failed to fetch symbols: {e}")
        return set()

async def flush_trade(redis: Redis, trade: dict):
    symbol = trade.get("s")
    price = trade.get("p")
    timestamp = trade.get("t")
    volume = trade.get("v", 0)
    now = datetime.now(timezone.utc).isoformat()

    if not symbol or price is None:
        return

    trade_info = {
        "price": price,
        "timestamp": timestamp,
        "volume": volume,
        "updated_at": now
    }

    try:
        pipe = redis.pipeline()
        pipe.set(f"{PRICE_PREFIX}{symbol}", price)
        pipe.hset(f"{TRADE_PREFIX}{symbol}", mapping=trade_info)
        await pipe.execute()
    except Exception as e:
        logger.error(f"[Redis] Pipeline failed for trade: {e}")

    # Update OHLCV buffer
    if symbol not in ohlcv_buffer:
        ohlcv_buffer[symbol] = []
    ohlcv_buffer[symbol].append({"price": price, "volume": volume})

async def flush_ohlcv(redis: Redis):
    now = datetime.now(timezone.utc).isoformat()
    for symbol, trades in ohlcv_buffer.items():
        if not trades:
            continue
        prices = [t["price"] for t in trades]
        volumes = [t["volume"] for t in trades]

        open_ = prices[0]
        high = max(prices)
        low = min(prices)
        close = prices[-1]
        volume = sum(volumes)

        ohlcv = {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "updated_at": now
        }

        try:
            await redis.hset(f"{OHLCV_PREFIX}{symbol}", mapping=ohlcv)
        except Exception as e:
            logger.error(f"[Redis] Failed to flush OHLCV for {symbol}: {e}")

    ohlcv_buffer.clear()

# --- Streamer ---
async def stream_trades():
    redis = Redis.from_url(REDIS_URL, decode_responses=True)
    logger.info("[Redis] Connected ✅")

    reconnect_delay = 3
    max_delay = 60

    async def periodic_ohlcv_flush():
        while True:
            await asyncio.sleep(10)
            await flush_ohlcv(redis)

    asyncio.create_task(periodic_ohlcv_flush())

    while True:
        try:
            logger.info("[WS] Connecting to Finnhub...")
            async with websockets.connect(FINNHUB_WS_URL, ping_interval=20, ping_timeout=10) as ws:
                logger.info("[WS] Connected ✅")
                reconnect_delay = 3

                symbols = await get_symbols(redis)
                if not symbols:
                    logger.warning("[WS] No symbols to subscribe — sleeping 30s")
                    await asyncio.sleep(30)
                    continue

                for sym in symbols:
                    await ws.send(json.dumps({"type": "subscribe", "symbol": sym}))

                while True:
                    msg = await asyncio.wait_for(ws.recv(), timeout=30)
                    data = json.loads(msg)
                    if data.get("type") == "trade":
                        for trade in data.get("data", []):
                            await flush_trade(redis, trade)

        except asyncio.TimeoutError:
            logger.warning("[WS] Timeout — retrying")
        except websockets.ConnectionClosed:
            logger.warning("[WS] Connection closed — reconnecting")
        except Exception as e:
            logger.error(f"[WS] Error: {e}")

        logger.info(f"[WS] Reconnecting in {reconnect_delay}s...")
        await asyncio.sleep(reconnect_delay)
        reconnect_delay = min(reconnect_delay * 2, max_delay)

# --- Boot ---
if __name__ == "__main__":
    try:
        logger.info("🚀 Starting real-time streamer with OHLCV updates every 10s")
        asyncio.run(stream_trades())
    except Exception as e:
        logger.exception(f"[FATAL] Crashed: {e}")
