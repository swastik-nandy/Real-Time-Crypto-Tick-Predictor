use std::{
    collections::HashMap,
    env,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use tokio::time::{sleep, timeout};
use tokio_postgres::{Client as PgClient, NoTls, types::ToSql};

use postgres_native_tls::MakeTlsConnector;
use native_tls::TlsConnector;

const FETCH_INTERVAL: Duration = Duration::from_secs(10);
const REDIS_TIMEOUT: Duration = Duration::from_secs(3);
const POSTGRES_TIMEOUT: Duration = Duration::from_secs(5);

/// Auto-handle Postgres TLS for remote, NoTLS for local
async fn connect_pg(pg_url: &str) -> PgClient {
    let is_local = pg_url.contains("localhost") || pg_url.contains("127.0.0.1");

    if is_local {
        println!("🌐 Connecting to Postgres without TLS (local)...");
        let (client, connection) = tokio_postgres::connect(pg_url, NoTls)
            .await
            .expect("❌ Local Postgres connection failed");
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("❌ Postgres connection error: {}", e);
            }
        });
        return client;
    }

    println!("🔐 Connecting to Postgres with TLS...");
    let tls_connector = TlsConnector::new().expect("❌ Failed to create TLS connector");
    let tls = MakeTlsConnector::new(tls_connector);

    match tokio_postgres::connect(pg_url, tls).await {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("❌ Postgres connection error: {}", e);
                }
            });
            println!("✅ Connected to Postgres (TLS)");
            client
        }
        Err(e) => {
            eprintln!("⚠️ TLS connection failed: {e}");
            println!("🔓 Falling back to NoTLS...");
            let (client, connection) = tokio_postgres::connect(pg_url, NoTls)
                .await
                .expect("❌ NoTLS Postgres connection failed");
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("❌ Postgres connection error: {}", e);
                }
            });
            client
        }
    }
}

/// Auto-handle Redis TLS for remote, NoTLS for local
async fn connect_redis(redis_url: &str) -> redis::aio::MultiplexedConnection {
    let is_local = redis_url.contains("localhost") || redis_url.contains("127.0.0.1");

    if is_local || redis_url.starts_with("redis://") {
        println!("🌐 Connecting to Redis without TLS...");
        let client = redis::Client::open(redis_url).expect("❌ Invalid Redis URL");
        return client
            .get_multiplexed_async_connection()
            .await
            .expect("❌ Redis NoTLS connection failed");
    }

    println!("🔐 Connecting to Redis with TLS...");
    let client = redis::Client::open(redis_url).expect("❌ Invalid Redis URL");

    match client.get_multiplexed_async_connection().await {
        Ok(conn) => {
            println!("✅ Connected to Redis (TLS verified)");
            conn
        }
        Err(e) => {
            eprintln!("⚠️ TLS connection failed: {e}");
            println!("🔓 Retrying Redis connection without TLS...");
            let url_no_tls = redis_url.replacen("rediss://", "redis://", 1);
            let client = redis::Client::open(url_no_tls).expect("❌ Invalid Redis URL");
            client
                .get_multiplexed_async_connection()
                .await
                .expect("❌ Redis NoTLS connection failed")
        }
    }
}

pub async fn run(flag: Arc<AtomicBool>) {
    println!("🚀 Fetcher started");
    dotenv::dotenv().ok();

    let redis_url = env::var("REDIS_URL").expect("❌ REDIS_URL not set");
    let pg_url = env::var("DATABASE_URL").expect("❌ DATABASE_URL not set");

    // Connect to Redis & Postgres with auto TLS/NoTLS logic
    let mut redis = connect_redis(&redis_url).await;
    let pg = connect_pg(&pg_url).await;

    // Preload symbol -> id map from DB
    println!("📥 Loading stock symbol map from DB...");
    let rows = pg
        .query("SELECT id, symbol FROM stocks", &[])
        .await
        .expect("❌ Failed to load stock map");
    println!("✅ Loaded {} stock symbols from DB", rows.len());

    let id_map: HashMap<String, i32> =
        rows.into_iter().map(|r| (r.get::<_, String>(1), r.get::<_, i32>(0))).collect();

    const SYMBOLS_KEY: &str = "stock:symbols";
    const OHLCV_PREFIX: &str = "stock:ohlcv:";

    while flag.load(Ordering::Relaxed) {
        // 1) Get symbols from Redis
        let symbols: Vec<String> = match timeout(REDIS_TIMEOUT, redis.smembers::<_, Vec<String>>(SYMBOLS_KEY)).await {
            Ok(Ok(v)) => {
                if v.is_empty() {
                    println!("⚠️ No symbols found in Redis — skipping insert this cycle.");
                }
                v
            }
            Ok(Err(e)) => {
                eprintln!("❌ Redis smembers error: {e}");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
            Err(_) => {
                eprintln!("⏱️ Redis smembers timed out");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        // 2) Fetch OHLCV for all symbols
        if symbols.is_empty() {
            sleep(FETCH_INTERVAL).await;
            continue;
        }

        let mut pipe = redis::pipe();
        for s in &symbols {
            pipe.hgetall(format!("{OHLCV_PREFIX}{s}"));
        }
        let rows: Vec<HashMap<String, String>> =
            match timeout(REDIS_TIMEOUT, pipe.query_async(&mut redis)).await {
                Ok(Ok(v)) => v,
                Ok(Err(e)) => {
                    eprintln!("❌ Redis pipeline error: {e}");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
                Err(_) => {
                    eprintln!("⏱️ Redis pipeline timed out");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

        // 3) Build insert query
        let mut values: Vec<Box<dyn ToSql + Sync>> = Vec::new();
        let mut placeholders = Vec::new();
        let mut i = 1;
        let mut skipped_empty = 0;
        let mut skipped_missing_id = 0;
        let mut skipped_incomplete = 0;

        for (sym, map) in symbols.iter().zip(rows) {
            if map.is_empty() {
                skipped_empty += 1;
                continue;
            }

            let num = |k: &str| map.get(k).and_then(|s| s.parse::<f64>().ok());
            let (o, h, l, c, v) = (num("open"), num("high"), num("low"), num("close"), num("volume"));
            let ts = map
                .get("updated_at")
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.naive_utc());

            let (o, h, l, c, v, ts) = match (o, h, l, c, v, ts) {
                (Some(o), Some(h), Some(l), Some(c), Some(v), Some(ts)) => (o, h, l, c, v, ts),
                _ => {
                    skipped_incomplete += 1;
                    continue;
                }
            };

            let stock_id = match id_map.get(sym) {
                Some(&id) => id,
                None => {
                    skipped_missing_id += 1;
                    continue;
                }
            };

            placeholders.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                i, i + 1, i + 2, i + 3, i + 4, i + 5, i + 6, i + 7
            ));
            i += 8;

            values.push(Box::new(stock_id));
            values.push(Box::new(sym.clone()));
            values.push(Box::new(o));
            values.push(Box::new(h));
            values.push(Box::new(l));
            values.push(Box::new(c));
            values.push(Box::new(v));
            values.push(Box::new(ts));
        }

        if skipped_empty > 0 {
            println!("⚠️ Skipped {} symbols with empty OHLCV", skipped_empty);
        }
        if skipped_incomplete > 0 {
            println!("⚠️ Skipped {} symbols with incomplete OHLCV", skipped_incomplete);
        }
        if skipped_missing_id > 0 {
            println!("⚠️ Skipped {} symbols not found in DB", skipped_missing_id);
        }

        // 4) Insert into DB
        if placeholders.is_empty() {
            println!("ℹ️ No valid rows to insert this cycle.");
        } else {
            let sql = format!(
                "INSERT INTO stock_price_history \
                 (stock_id, symbol, open, high, low, close, volume, trade_time_stamp) \
                 VALUES {}",
                placeholders.join(", ")
            );

            let params: Vec<&(dyn ToSql + Sync)> =
                values.iter().map(|v| v.as_ref() as &(dyn ToSql + Sync)).collect();

            match timeout(POSTGRES_TIMEOUT, pg.execute(&sql, &params)).await {
                Ok(Ok(n)) => println!("✅ Inserted {} rows at {}", n, Utc::now().format("%H:%M:%S")),
                Ok(Err(e)) => eprintln!("❌ Postgres insert error: {e}"),
                Err(_) => eprintln!("⏱️ Postgres insert timed out"),
            }
        }

        sleep(FETCH_INTERVAL).await;
    }

    println!("🧹 Fetcher stopped");
}
