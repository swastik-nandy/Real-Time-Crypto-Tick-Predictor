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
use tokio::time::{sleep, timeout, Instant};

// Postgres
use tokio_postgres::{Client, Config};
use tokio_postgres::config::SslMode;
use tokio_postgres::tls::MakeTlsConnect; // ✅ correct public import
use tokio_postgres::types::ToSql;

// TLS for Postgres via rustls
use postgres_rustls::MakeRustlsConnect;
use rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;

const FETCH_INTERVAL_SECS: u64 = 10;
const REDIS_OP_TIMEOUT: Duration = Duration::from_secs(3);
const POSTGRES_OP_TIMEOUT: Duration = Duration::from_secs(5);
const SLEEP_TICK: Duration = Duration::from_millis(100);

// --- TLS + connect helpers ----------------------------------------------------

fn build_tls() -> MakeRustlsConnect {
    let mut root_store = RootCertStore::empty();
    for cert in load_native_certs().expect("Could not load platform certs") {
        root_store.add(cert).expect("adding platform cert failed");
    }
    let tls_config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    MakeRustlsConnect::new(Arc::new(tls_config))
}

fn pg_config_force_tls(url: &str) -> Config {
    use std::str::FromStr;
    let mut cfg = Config::from_str(url).expect("Invalid DATABASE_URL");
    cfg.ssl_mode(SslMode::Require);
    cfg
}

async fn connect_pg_cfg<T>(cfg: &Config, tls: T) -> Client
where
    T: MakeTlsConnect<tokio_postgres::Socket> + Send + Sync + 'static,
{
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        match cfg.connect(tls.clone()).await {
            Ok((client, connection)) => {
                tokio::spawn(async move {
                    if let Err(e) = connection.await {
                        eprintln!("❌ PostgreSQL connection error: {e}");
                    }
                });
                return client;
            }
            Err(e) => {
                eprintln!("⚠️ Postgres connect failed (attempt {attempt}): {e}");
                if attempt >= 5 {
                    panic!("Postgres connection failed after {attempt} attempts: {e}");
                }
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

// --- entrypoint ---------------------------------------------------------------

pub async fn run(fetcher_running: Arc<AtomicBool>) {
    println!("🚀 Fetcher started");
    dotenv::dotenv().ok();

    // Env
    let redis_url = env::var("REDIS_URL").expect("REDIS_URL not set");
    let pg_url = env::var("DATABASE_URL").expect("Postgres URL not set");

    // Redis connection
    let redis_client = redis::Client::open(redis_url).expect("Invalid Redis URL");
    let mut redis = redis_client
        .get_multiplexed_async_connection()
        .await
        .expect("Redis connection failed");

    // Postgres connection (TLS)
    let tls = build_tls();
    let cfg = pg_config_force_tls(&pg_url);
    let pg_client = connect_pg_cfg(&cfg, tls).await;

    // Load stock_id map
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

    loop {
        if !fetcher_running.load(Ordering::Relaxed) {
            println!("🛑 Stop signal → exiting fetcher");
            break;
        }

        let loop_start = Instant::now();

        // 1) Get symbols
        let symbols: Vec<String> = match timeout(REDIS_OP_TIMEOUT, redis.smembers(SYMBOLS_KEY)).await {
            Ok(Ok(s)) => s,
            Ok(Err(err)) => {
                eprintln!("❌ Redis smembers: {err}");
                coop_sleep(&fetcher_running, Duration::from_secs(1)).await;
                continue;
            }
            Err(_) => {
                eprintln!("⏱️ Redis smembers timed out");
                coop_sleep(&fetcher_running, Duration::from_secs(1)).await;
                continue;
            }
        };

        if !fetcher_running.load(Ordering::Relaxed) { break; }

        // 2) HGETALL pipeline
        let mut pipe = redis::pipe();
        for sym in &symbols {
            pipe.hgetall(format!("{OHLCV_PREFIX}{sym}"));
        }
        let results: Vec<HashMap<String, String>> =
            match timeout(REDIS_OP_TIMEOUT, pipe.query_async(&mut redis)).await {
                Ok(Ok(r)) => r,
                Ok(Err(err)) => {
                    eprintln!("❌ Redis pipeline: {err}");
                    coop_sleep(&fetcher_running, Duration::from_secs(1)).await;
                    continue;
                }
                Err(_) => {
                    eprintln!("⏱️ Redis pipeline timed out");
                    coop_sleep(&fetcher_running, Duration::from_secs(1)).await;
                    continue;
                }
            };

        if !fetcher_running.load(Ordering::Relaxed) { break; }

        // 3) Build batch
        let mut values: Vec<Box<dyn ToSql + Send + Sync>> = Vec::new();
        let mut placeholders = Vec::new();
        let mut idx = 1;

        for (symbol, map) in symbols.iter().zip(results) {
            if map.is_empty() { continue; }

            let parse_f = |k: &str| map.get(k).and_then(|s| s.parse::<f64>().ok());
            let (open, high, low, close, volume) = (
                parse_f("open"),
                parse_f("high"),
                parse_f("low"),
                parse_f("close"),
                parse_f("volume"),
            );

            let updated_at = map.get("updated_at")
                .map(|s| s.trim_matches('"'))
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.naive_utc());

            let (open, high, low, close, volume, updated_at) = match (open, high, low, close, volume, updated_at) {
                (Some(o), Some(h), Some(l), Some(c), Some(v), Some(t)) => (o, h, l, c, v, t),
                _ => continue,
            };

            let stock_id = match stock_id_map.get(symbol) {
                Some(id) => *id,
                None => continue,
            };

            placeholders.push(format!(
                "(${}, ${}, ${}, ${}, ${}, ${}, ${}, ${})",
                idx, idx + 1, idx + 2, idx + 3, idx + 4, idx + 5, idx + 6, idx + 7
            ));
            idx += 8;

            values.push(Box::new(stock_id));
            values.push(Box::new(symbol.clone()));
            values.push(Box::new(open));
            values.push(Box::new(high));
            values.push(Box::new(low));
            values.push(Box::new(close));
            values.push(Box::new(volume));
            values.push(Box::new(updated_at));
        }

        if !fetcher_running.load(Ordering::Relaxed) { break; }

        // 4) Insert batch
        if !placeholders.is_empty() {
            let query = format!(
                "INSERT INTO stock_price_history \
                 (stock_id, symbol, open, high, low, close, volume, trade_time_stamp) \
                 VALUES {}",
                placeholders.join(", ")
            );

            let params: Vec<&(dyn ToSql + Sync)> =
                values.iter().map(|v| v.as_ref() as &(dyn ToSql + Sync)).collect();

            match timeout(POSTGRES_OP_TIMEOUT, pg_client.execute(&query, &params)).await {
                Ok(Ok(count)) => println!("✅ Inserted {} rows at {}", count, Utc::now().format("%H:%M:%S")),
                Ok(Err(err))  => eprintln!("❌ Postgres insert: {err}"),
                Err(_)        => eprintln!("⏱️ Postgres insert timed out"),
            }
        }

        // 5) Cooperative sleep
        let elapsed = loop_start.elapsed();
        if elapsed < Duration::from_secs(FETCH_INTERVAL_SECS) {
            coop_sleep(&fetcher_running, Duration::from_secs(FETCH_INTERVAL_SECS) - elapsed).await;
        } else {
            println!("⚠️ Fetch took {:.2?}, skipping sleep", elapsed);
            coop_sleep(&fetcher_running, Duration::from_millis(50)).await;
        }
    }

    println!("🧹 Fetcher exited cleanly");
}

async fn coop_sleep(flag: &Arc<AtomicBool>, total: Duration) {
    let start = Instant::now();
    while start.elapsed() < total {
        if !flag.load(Ordering::Relaxed) { break; }
        let remaining = total.saturating_sub(start.elapsed());
        let nap = if remaining > SLEEP_TICK { SLEEP_TICK } else { remaining };
        if nap.is_zero() { break; }
        sleep(nap).await;
    }
}
