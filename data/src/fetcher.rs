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
use futures::stream::StreamExt;
use redis::{aio::ConnectionLike, AsyncCommands, Client as RedisClient, RedisResult};
use tokio::time::{sleep, timeout};

// Postgres TLS
use tokio_postgres::{Client as PgClient, Config, NoTls, types::ToSql};
use tokio_postgres::config::SslMode;
use postgres_rustls::MakeRustlsConnect;
use rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;

const FETCH_INTERVAL: Duration = Duration::from_secs(10);
const REDIS_TIMEOUT: Duration = Duration::from_secs(3);
const POSTGRES_TIMEOUT: Duration = Duration::from_secs(5);

/// Build a rustls TLS connector for Postgres
fn build_pg_tls() -> MakeRustlsConnect {
    let mut root_store = RootCertStore::empty();
    for cert in load_native_certs().expect("Could not load platform certs") {
        root_store.add(cert).expect("Invalid cert");
    }
    let tls_config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    MakeRustlsConnect::new(Arc::new(tls_config))
}

/// Parse DATABASE_URL into Config + force TLS
fn pg_config_tls(url: &str) -> Config {
    use std::str::FromStr;
    let mut cfg = Config::from_str(url).expect("Invalid DATABASE_URL");
    cfg.ssl_mode(SslMode::Require);
    cfg
}

/// Connect to Postgres with retries
async fn connect_pg(cfg: &Config, tls: MakeRustlsConnect) -> PgClient {
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

pub async fn run(fetcher_running: Arc<AtomicBool>) {
    println!("🚀 Fetcher started");

    dotenv::dotenv().ok();
    let redis_url = env::var("REDIS_URL").expect("REDIS_URL not set");
    let pg_url = env::var("DATABASE_URL").expect("DATABASE_URL not set");

    // Connect Redis (auto TLS with rediss://)
    let redis_client = RedisClient::open(redis_url).expect("Invalid Redis URL");
    let mut redis_conn = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("Redis connection failed");

    // Connect Postgres TLS
    let pg_tls = build_pg_tls();
    let pg_cfg = pg_config_tls(&pg_url);
    let pg_client = connect_pg(&pg_cfg, pg_tls).await;

    // Load stock ID map
    let rows = pg_client
        .query("SELECT id, symbol FROM stocks", &[])
        .await
        .expect("Failed to load stock ID map");

    let stock_id_map: HashMap<String, i32> = rows
        .into_iter()
        .map(|row| (row.get::<_, String>(1), row.get::<_, i32>(0)))
        .collect();

    const SYMBOLS_KEY: &str = "stock:symbols";
    const OHLCV_PREFIX: &str = "stock:ohlcv:";

    while fetcher_running.load(Ordering::Relaxed) {
        let symbols: Vec<String> = match timeout(REDIS_TIMEOUT, redis_conn.smembers(SYMBOLS_KEY)).await {
            Ok(Ok(s)) => s,
            _ => {
                eprintln!("⚠️ Redis symbols fetch failed/timed out");
                sleep(Duration::from_secs(1)).await;
                continue;
            }
        };

        let mut pipe = redis::pipe();
        for sym in &symbols {
            pipe.hgetall(format!("{OHLCV_PREFIX}{sym}"));
        }

        let results: Vec<HashMap<String, String>> =
            match timeout(REDIS_TIMEOUT, pipe.query_async(&mut redis_conn)).await {
                Ok(Ok(r)) => r,
                _ => {
                    eprintln!("⚠️ Redis OHLCV fetch failed/timed out");
                    sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

        let mut values: Vec<Box<dyn ToSql + Send + Sync>> = Vec::new();
        let mut placeholders = Vec::new();
        let mut idx = 1;

        for (symbol, map) in symbols.iter().zip(results) {
            if map.is_empty() { continue; }

            let parse_f = |k: &str| map.get(k).and_then(|s| s.parse::<f64>().ok());
            let (o, h, l, c, v) = (
                parse_f("open"),
                parse_f("high"),
                parse_f("low"),
                parse_f("close"),
                parse_f("volume"),
            );

            let updated_at = map.get("updated_at")
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.naive_utc());

            if let (Some(o), Some(h), Some(l), Some(c), Some(v), Some(ts)) = (o, h, l, c, v, updated_at) {
                if let Some(stock_id) = stock_id_map.get(symbol) {
                    placeholders.push(format!(
                        "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                        idx, idx + 1, idx + 2, idx + 3, idx + 4, idx + 5, idx + 6, idx + 7
                    ));
                    idx += 8;

                    values.push(Box::new(*stock_id));
                    values.push(Box::new(symbol.clone()));
                    values.push(Box::new(o));
                    values.push(Box::new(h));
                    values.push(Box::new(l));
                    values.push(Box::new(c));
                    values.push(Box::new(v));
                    values.push(Box::new(ts));
                }
            }
        }

        if !placeholders.is_empty() {
            let query = format!(
                "INSERT INTO stock_price_history \
                 (stock_id, symbol, open, high, low, close, volume, trade_time_stamp) \
                 VALUES {}",
                placeholders.join(", ")
            );
            let params: Vec<&(dyn ToSql + Sync)> = values.iter().map(|v| v.as_ref()).collect();

            match timeout(POSTGRES_TIMEOUT, pg_client.execute(&query, &params)).await {
                Ok(Ok(count)) => println!("✅ Inserted {} rows at {}", count, Utc::now()),
                _ => eprintln!("⚠️ Postgres insert failed/timed out"),
            }
        }

        sleep(FETCH_INTERVAL).await;
    }

    println!("🛑 Fetcher stopped");
}
