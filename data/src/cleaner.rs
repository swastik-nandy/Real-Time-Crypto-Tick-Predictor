use std::{env, sync::{Arc, atomic::{AtomicBool, Ordering}}, time::Duration};

use tokio::time::sleep;
use tokio_postgres::{config::SslMode, Client as PgClient, Config};

// ----------------------------- TLS for Postgres via rustls ---------------------------------------

use postgres_rustls::MakeTlsConnector;
use rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;
use tokio_rustls::TlsConnector;

fn build_pg_tls() -> MakeTlsConnector {
    let mut roots = RootCertStore::empty();
    for cert in load_native_certs().expect("load platform certs") {
        roots.add(cert).expect("invalid platform cert");
    }
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = TlsConnector::from(Arc::new(cfg));
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

pub async fn run() {
    println!("🧼 Cleaner starting…");
    dotenv::dotenv().ok();

    let pg_url = env::var("DATABASE_URL").expect("DATABASE_URL not set");
    let tls = build_pg_tls();
    let cfg = pg_config_tls(&pg_url);
    let pg = connect_pg(&cfg, tls).await;

    // ---------------------------------Do maintenance -------------------------


    match pg.execute("TRUNCATE TABLE stock_price_history", &[]).await {
        Ok(_) => println!("✅ TRUNCATE succeeded"),
        Err(e) => eprintln!("❌ TRUNCATE failed: {e}"),
    }

    match pg.execute("VACUUM stock_price_history", &[]).await {
        Ok(_) => println!("✅ VACUUM succeeded"),
        Err(e) => eprintln!("❌ VACUUM failed: {e}"),
    }

    println!("✨ Cleaner finished");
}
