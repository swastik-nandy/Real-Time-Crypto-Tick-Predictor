import asyncio
import os
import asyncpg
from redis.asyncio import Redis
from datetime import datetime, time, date
from pathlib import Path
from dotenv import load_dotenv

from SyncRedis import initialize_redis_symbols, sync_symbols_to_redis
from cleaner import run_cleanup
from fetcher import run_fetcher
import sys

#--------------------ENV--------------------

if not os.environ.get("ENV"):
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

REDIS_URL = os.environ.get("REDIS_URL")
DATABASE_URL = os.environ.get("DATABASE_URL")
DSN = DATABASE_URL.replace("postgresql+asyncpg://", "postgresql://")

#--------------------POSTGRES LISTENER--------------------

async def on_stock_change(conn, pid, channel, payload):
    print("\n✅ Detected change in 'stocks' table. Syncing Redis...")
    await sync_symbols_to_redis()

async def listen_to_stock_changes():
    try:
        conn = await asyncpg.connect(DSN)
        await conn.add_listener("stock_changed", on_stock_change)
        print("✅ Listening to 'stock_changed' notifications from Postgres")
        while True:
            await asyncio.sleep(3600)
    except Exception as e:
        print(f"❌ LISTEN failed: {e}")

#--------------------FLAGS--------------------

fetcher_running = False
cleanup_stock_done = False
cleanup_predicted_done = False
last_backup_day = None  # Track date of last successful backup

#--------------------HELPERS--------------------

async def table_not_empty(conn, table: str) -> bool:
    try:
        result = await conn.fetchval(f"SELECT EXISTS (SELECT 1 FROM {table} LIMIT 1);")
        return result
    except Exception as e:
        print(f"❌ Failed to check {table}: {e}")
        return False

#--------------------TIME TRIGGERS--------------------

async def time_based_trigger():
    global fetcher_running, cleanup_stock_done, cleanup_predicted_done, last_backup_day

    pg_pool = await asyncpg.create_pool(DSN)

    while True:
        now = datetime.utcnow()
        current_time = now.time()
        today = now.date()

#--------------------BACKUP--------------------

        if time(5, 0) <= current_time < time(5, 2):
            if last_backup_day != today:
                print("✅ Running backup.py for daily export...")
                try:
                    process = await asyncio.create_subprocess_exec(
                        sys.executable, "backup.py",
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    stdout, stderr = await process.communicate()

                    if process.returncode == 0:
                        print("✅ Backup completed successfully")
                        print(stdout.decode().strip())
                        last_backup_day = today
                    else:
                        print("❌ Backup failed")
                        print(stderr.decode().strip())
                except Exception as e:
                    print(f"❌ Exception while running backup.py: {e}")

#--------------------CLEANUP stock_price_history (00:05 UTC)--------------------

        if time(5, 2) < current_time < time(5, 5) and not cleanup_stock_done:
            async with pg_pool.acquire() as conn:
                not_empty = await table_not_empty(conn, "stock_price_history")
                if not_empty:
                    print("✅ Running cleanup for stock_price_history")
                    await run_cleanup()
                else:
                    print("✅ stock_price_history already empty — skipping")
            cleanup_stock_done = True

        if current_time >= time(5, 5):
            cleanup_stock_done = False  # Reset flag

#--------------------CLEANUP predicted_prices --------------------------------------------------

        if current_time >= time(23, 0) and not cleanup_predicted_done:
            async with pg_pool.acquire() as conn:
                not_empty = await table_not_empty(conn, "predicted_prices")
                if not_empty:
                    print("✅ Running cleanup for predicted_prices")
                    await run_cleanup()
                else:
                    print("✅ predicted_prices already empty — skipping")
            cleanup_predicted_done = True

        if current_time < time(23, 0):
            cleanup_predicted_done = False  # Reset flag

#--------------------FETCHER TRIGGER-------------------------------------------------------

        if (current_time <= time(5, 0) or current_time >= time(5, 5)) and not fetcher_running:
            print(f"✅ Starting fetcher at {current_time}")
            asyncio.create_task(run_fetcher())
            fetcher_running = True

        if time(5, 0) <= current_time <= time(5,5):

            fetcher_running = False  # Reset flag

        await asyncio.sleep(30)

#--------------------MAIN ENTRY--------------------

async def main():
    print("✅ Trigger Controller Started")
    redis = Redis.from_url(REDIS_URL, decode_responses=True)

    await redis.ping()
    print("✅ Redis connected")

    await initialize_redis_symbols()

    await asyncio.gather(
        listen_to_stock_changes(),
        time_based_trigger()
    )

if __name__ == "__main__":
    asyncio.run(main())
