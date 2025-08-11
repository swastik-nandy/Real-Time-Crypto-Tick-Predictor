# **TRIGGERSTOCK**

A fully asynchronous, memory-efficient stock analytics platform for **real-time price ingestion, prediction, and analytics**.

---

## 🚀 Overview

Built using:

- 🗪 **Tokio(Rust) + Redis + PostgreSQL**
- 🛁 **Finnhub WebSocket** streaming
- ⚙️ **Micro-batched Redis pipeline**
- 🧠 **Daily model retraining pipeline**
- 🐳 **Dockerized microservices**
- ☁️ Deployable on Fly.io, Northflank, Render, or any cloud VM

---

## 🧠 What It Does

| Component                   | Description                                                                   |
| --------------------------- | ----------------------------------------------------------------------------- |
| ✅ `websocket.py`            | Connects to Finnhub WebSocket and pushes live prices into Redis (1ms latency) |
| ✅ `fetcher.py`              | Every 10s, reads Redis and writes to Postgres `stock_price_history`           |
| ✅ `trigger.py`              | Starts the fetcher only between 13:00 UTC and 21:00 UTC                       |
| ✅ `cleaner.py`              | VACUUM FULL + TRUNCATE daily to keep Postgres lean                            |
| 🔄 `model_trainer.py` (WIP) | Retrains XGBoost model daily on new data                                      |
| 🧪 `FastAPI backend` (WIP)  | Provides API for dashboard, alerts, and predictions                           |

---

## 🏗 Architecture

```
         +------------+                 +-------------------+
         |  Finnhub   |  <--WebSocket-- |   websocket.py     |
         +------------+                 +---------+---------+
                                                   |
                                                   ↓
                                             +-----+-----+
                                             |   Redis    |
                                             +-----+-----+
                                                   |
                                         +---------+----------+
                                         |     fetcher.py      |
                                         |  (every 10 seconds) |
                                         +---------+----------+
                                                   ↓
                                          +--------+--------+
                                          |    PostgreSQL    |
                                          | stock_price_history |
                                          +------------------+
```

---

## ⚙️ Tech Stack

- 🐍 Python 3.11 (async-first)
- 🔸 FastAPI (for APIs and triggers)
- 📆 Redis (live price cache)
- 📂 PostgreSQL (price history, model features)
- 📉 XGBoost (ML model)
- 🐳 Docker (per-service container builds)
- 🧪 GitHub Actions (daily retrain, cleanup)
- ☁️ Deploys easily on Fly.io, Northflank, Render

---

## 💻 Local Setup

### 1️⃣ Clone the Repo

```bash
git clone https://github.com/your-username/real-time-stock-analytics.git
cd real-time-stock-analytics/Backend
```

---

### 2️⃣ Project Structure

```
Real-Time-Stock-Analytics/
├── Backend/
│   ├── services/
│   │   ├── websocket.py
│   │   ├── fetcher.py
│   │   ├── trigger.py
│   │   ├── cleaner.py
│   ├── app/
│   │   ├── core/config.py
│   │   └── db/, models/, ...
│   ├── requirements.txt
│   └── .env
├── images/
│   ├── Dockerfile.websocket
│   ├── Dockerfile.fetcher
│   └── ...
├── README.md
```

---

### 3️⃣ Environment Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

### 4️⃣ Environment Variables (Dummy)

```env
FINNHUB_API_KEY=your_finnhub_key
REDIS_URL=redis://localhost:6379
DATABASE_URL=postgresql+asyncpg://user:pass@localhost:5432/dbname
ENV=local
```

---

## 📈 Model Training (Coming Soon)

- Uses **XGBoost** + **sliding window**
- Daily retraining on past 7–10 days
- Predicts short-term price trend
- Metrics: **MSE**, **directional accuracy**

---

## 🤝 Contributing

PRs are welcome!\
If you're interested in **real-time systems**, **stock modeling**, or **ML infra**, open an issue or contribute directly.

---

## 📜 License

MIT © 2025 Swastik Nandy

