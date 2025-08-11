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

// ------------------------------------- Postgres -----------------------------------------
use tokio_postgres::{config::SslMode, types::ToSql, Client as PgClient, Config};

// ------------- TLS for Postgres via native-tls -------------------
use postgres_native_tls::MakeTlsConnector;
use native_tls::TlsConnector;

const FETCH_INTERVAL: Duration = Duration::from_secs(10);
const REDIS_TIMEOUT: Duration = Duration::from_secs(3);
const POSTGRES_TIMEOUT: Duration = Duration::from_secs(5);

fn build_pg_tls() -> MakeTlsConnector {
    let connector = TlsConnector::builder()
        .build()
        .expect("Failed to create native-tls connector");
    MakeTlsConnector::new(connector)
}

fn pg_config_tls(url: &str) -> Config {
    use std::str::FromStr;
    let mut cfg = Config::from_str(url).expect("Invalid DATABASE_URL");
    cfg.ssl_mode(SslMode::Require);
    cfg
}

async fn connect_pg(cfg: &Config, tls: MakeTlsConnector) -> PgClient {
    for attempt in 1..=5 {
        match cfg.connect(tls.clone()).await {
            Ok((client, conn)) => {
                tokio::spawn(async move {
                    if let Err(e) = conn.await {
                        eprintln!("❌ Postgres connection error: {e}");
                    }
                });
                return client;
            }
            Err(e) => {
                eprintln!("⚠️ Postgres connect failed (attempt {attempt}): {e}");
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
    panic!("❌ Could not connect to Postgres after 5 attempts");
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

    // ---------------- Postgres (TLS via native-tls) ---------------------
    let pg_tls = build_pg_tls();
    let pg_cfg = pg_config_tls(&pg_url);
    let pg = connect_pg(&pg_cfg, pg_tls).await;

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
        let mut values: Vec<Box<dyn ToSql + Send + Sync>> = Vec::new();
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

            // FIX: Match the trait bounds so Send + Sync is preserved
            let params: Vec<&(dyn ToSql + Send + Sync)> = values.iter().map(|v| v.as_ref()).collect();

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
