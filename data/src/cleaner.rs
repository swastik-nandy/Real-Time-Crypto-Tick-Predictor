use std::{env, sync::Arc, time::Duration};
use tokio::time::sleep;

use tokio_postgres::{Client, Config};
use tokio_postgres::config::SslMode;
use tokio_postgres::types::ToSql;

// TLS for Postgres via rustls (0.21) + postgres_rustls (0.1.x)
use postgres_rustls::MakeTlsConnect;
use rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;

// --- TLS + connect helpers ----------------------------------------------------

fn build_tls() -> MakeTlsConnect {
    // Build TLS config with native roots
    let mut root_store = RootCertStore::empty();
    for cert in load_native_certs().expect("Could not load platform certificates") {
        // rustls 0.21: add(&Certificate) takes a reference
        root_store.add(&cert).expect("adding platform cert failed");
    }
    let tls_config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    MakeTlsConnect::new(Arc::new(tls_config))
}

fn pg_config_force_tls(url: &str) -> Config {
    use std::str::FromStr;
    let mut cfg = Config::from_str(url).expect("Invalid DATABASE_URL");
    cfg.ssl_mode(SslMode::Require);
    cfg
}

async fn connect_pg_cfg(cfg: &Config, tls: &MakeTlsConnect) -> Client {
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

pub async fn run() {
    println!("🧼 Cleaner starting…");
    dotenv::dotenv().ok();

    let pg_url = env::var("DATABASE_URL").expect("Postgres URL not set");

    let tls = build_tls();
    let cfg = pg_config_force_tls(&pg_url);
    let client = connect_pg_cfg(&cfg, &tls).await;

    // TRUNCATE
    match client
        .execute("TRUNCATE TABLE stock_price_history", &[] as &[&(dyn ToSql + Sync)])
        .await
    {
        Ok(_) => println!("✅ TRUNCATE succeeded"),
        Err(e) => eprintln!("❌ TRUNCATE failed: {e}"),
    }

    // VACUUM
    match client
        .execute("VACUUM stock_price_history", &[] as &[&(dyn ToSql + Sync)])
        .await
    {
        Ok(_) => println!("✅ VACUUM succeeded"),
        Err(e) => eprintln!("❌ VACUUM failed: {e}"),
    }

    println!("✨ Cleaner finished");
}
