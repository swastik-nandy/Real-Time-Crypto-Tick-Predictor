import asyncio
import asyncpg
import pandas as pd
from pathlib import Path
import os
import subprocess
from datetime import datetime
import shutil
import traceback

# -------------------- CONFIG ---------------------

raw_url = os.environ.get("DATABASE_URL")
if raw_url and raw_url.startswith("postgresql+asyncpg://"):
    DATABASE_URL = raw_url.replace("postgresql+asyncpg://", "postgresql://")
else:
    DATABASE_URL = raw_url

GIT_REPO_DIR = Path(__file__).resolve().parents[1]
CSV_PATH = GIT_REPO_DIR / "stock_price_history.csv"
BRANCH_NAME = "backups"
COMMIT_TIME = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "").strip()
REPO = os.environ.get("GITHUB_REPO", "").strip()  # e.g., "swastik-nandy/Real-Time-Stock-Analytics"

# -------------------- EXPORT FUNCTION --------------------

async def export_stock_price_history():
    print("📥 Starting export from PostgreSQL...")
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        rows = await conn.fetch("SELECT * FROM stock_price_history")
        await conn.close()

        if not rows:
            print("❌ No data found in stock_price_history.")
            return False

        df = pd.DataFrame([dict(row) for row in rows])
        df.to_csv(CSV_PATH, index=False)
        print(f"✅ Exported {len(df)} rows to {CSV_PATH}")
        return True

    except Exception as e:
        print(f"❌ Export failed: {e}")
        traceback.print_exc()
        return False

# -------------------- GIT COMMIT & PUSH --------------------

def commit_and_push():
    try:
        print(f"📂 Changing working directory to: {GIT_REPO_DIR}")
        os.chdir(GIT_REPO_DIR)

        # Wipe old .git if broken
        git_dir = GIT_REPO_DIR / ".git"
        if git_dir.exists():
            print("🧹 Removing existing .git directory...")
            shutil.rmtree(git_dir, ignore_errors=True)

        print("🌀 Initializing fresh Git repository...")
        subprocess.run(["git", "init"], check=True)

        print("👤 Setting Git config...")
        subprocess.run(["git", "config", "user.email", "actions@github.com"], check=True)
        subprocess.run(["git", "config", "user.name", "github-actions"], check=True)

        print(f"🔗 Adding Git remote: {REPO}")
        subprocess.run(["git", "remote", "add", "origin",
                        f"https://x-access-token:{GITHUB_TOKEN}@github.com/{REPO}.git"], check=True)

        print("📡 Fetching remote origin (non-fatal)...")
        subprocess.run(["git", "fetch", "origin"], check=False)

        print(f"🌿 Checking out branch: {BRANCH_NAME}")
        subprocess.run(["git", "checkout", "-B", BRANCH_NAME], check=True)

        if not CSV_PATH.exists():
            raise FileNotFoundError(f"CSV file not found at expected location: {CSV_PATH}")

        print(f"➕ Staging file: {CSV_PATH.name}")
        subprocess.run(["git", "add", str(CSV_PATH)], check=True)

        print(f"📝 Committing backup with message: '📊 Daily backup: {COMMIT_TIME}'")
        subprocess.run(["git", "commit", "-m", f"📊 Daily backup: {COMMIT_TIME}"], check=False)

        push_url = f"https://x-access-token:{GITHUB_TOKEN}@github.com/{REPO}.git"
        print(f"🚀 Pushing to GitHub repo: {REPO}, branch: {BRANCH_NAME}")
        subprocess.run(["git", "push", "--force", push_url, f"HEAD:{BRANCH_NAME}"], check=True)

        print("✅ Backup pushed to GitHub successfully.")

    except subprocess.CalledProcessError as e:
        print(f"❌ Git command failed: {e}")
        traceback.print_exc()
    except Exception as e:
        print(f"❌ Unexpected error during Git operations: {e}")
        traceback.print_exc()

# -------------------- MAIN --------------------

if __name__ == "__main__":
    try:
        print("📦 Starting backup.py script...")
        print(f"🕒 Timestamp: {COMMIT_TIME}")
        print(f"📊 Target GitHub repo: {REPO}")
        print(f"📍 CSV output path: {CSV_PATH}")
        print(f"🔌 DB connection: {DATABASE_URL}")

        success = asyncio.run(export_stock_price_history())

        if success:
            commit_and_push()
        else:
            print("❌ Backup process aborted — export failed or returned no data.")

    except Exception as e:
        print("❌ Backup script crashed due to unhandled exception:")
        traceback.print_exc()
