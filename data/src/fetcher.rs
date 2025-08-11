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
use tokio_postgres::NoTls;

use postgres_native_tls::MakeTlsConnector;
use native_tls::TlsConnector;
use tokio_postgres::Client as PgClient;
use tokio_postgres::types::ToSql;

const FETCH_INTERVAL: Duration = Duration::from_secs(10);
const REDIS_TIMEOUT: Duration = Duration::from_secs(3);
const POSTGRES_TIMEOUT: Duration = Duration::from_secs(5);

/// Auto-handle Postgres TLS (Render) or NoTLS (local)
async fn connect_pg(pg_url: &str) -> PgClient {
    let tls_connector = TlsConnector::new().expect("Failed to create TLS connector");
    let tls = MakeTlsConnector::new(tls_connector);

    match tokio_postgres::connect(pg_url, tls).await {
        Ok((client, connection)) => {
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("❌ Postgres connection error: {}", e);
                }
            });
            println!("🔐 Connected to Postgres with TLS");
            client
        }
        Err(e) => {
            eprintln!("⚠️ TLS connection failed: {e}");
            println!("🔓 Falling back to NoTLS...");
            let (client, connection) = tokio_postgres::connect(pg_url, NoTls)
                .await
                .expect("NoTLS Postgres connection failed");
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    eprintln!("❌ Postgres connection error: {}", e);
                }
            });
            client
        }
    }
}

pub async fn run(flag: Arc<AtomicBool>) {
    println!("🚀 Fetcher started");
    dotenv::dotenv().ok();

    let redis_url = env::var("REDIS_URL").expect("REDIS_URL not set");
    let pg_url = env::var("DATABASE_URL").expect("DATABASE_URL not set");

    // ---------------- Redis ----------------
    let redis_client = redis::Client::open(redis_url).expect("Invalid Redis URL");
    let mut redis = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("Redis connection failed");

    // ---------------- Postgres (auto TLS) ---------------------
    let pg = connect_pg(&pg_url).await;

    // Preload symbol -> id
    let rows = pg
        .query("SELECT id, symbol FROM stocks", &[])
        .await
        .expect("load stock map");
    let id_map: HashMap<String, i32> =
        rows.into_iter().map(|r| (r.get::<_, String>(1), r.get::<_, i32>(0))).collect();

    const SYMBOLS_KEY: &str = "stock:symbols";
    const OHLCV_PREFIX: &str = "stock:ohlcv:";

    while flag.load(Ordering::Relaxed) {
        // 1) symbols
        let symbols: Vec<String> = match timeout(REDIS_TIMEOUT, redis.smembers(SYMBOLS_KEY)).await {
            Ok(Ok(v)) => v,
            _ => {
                eprintln!("⚠️ Redis smembers failed/timed out");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        // 2) pipeline hgetall
        let mut pipe = redis::pipe();
        for s in &symbols {
            pipe.hgetall(format!("{OHLCV_PREFIX}{s}"));
        }
        let rows: Vec<HashMap<String, String>> =
            match timeout(REDIS_TIMEOUT, pipe.query_async(&mut redis)).await {
                Ok(Ok(v)) => v,
                _ => {
                    eprintln!("⚠️ Redis pipeline failed/timed out");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

        // 3) build insert
        let mut values: Vec<Box<dyn ToSql + Sync>> = Vec::new(); // No Send bound
        let mut placeholders = Vec::new();
        let mut i = 1;

        for (sym, map) in symbols.iter().zip(rows) {
            if map.is_empty() {
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
                _ => continue,
            };

            let stock_id = match id_map.get(sym) {
                Some(&id) => id,
                None => continue,
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

        // 4) insert
        if !placeholders.is_empty() {
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
