use std::env;

use tokio_postgres::types::ToSql;

// TLS for Postgres via Rustls
use postgres_rustls::MakeRustlsConnect;
use rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;

pub async fn run() {
    println!("🧼 Cleaner starting…");
    dotenv::dotenv().ok();

    let pg_url = env::var("DATABASE_URL").expect("Postgres URL not set");

    // --- Build a Rustls TLS connector using system root certs ---
    let mut root_store = RootCertStore::empty();
    for cert in load_native_certs().expect("Could not load platform certificates") {
        root_store.add(cert).unwrap();
    }
    let tls_config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let tls = MakeRustlsConnect::new(tls_config);

    // --- Connect with TLS ---
    let (client, connection) = tokio_postgres::connect(&pg_url, tls)
        .await
        .expect("Failed to connect to Postgres");

    // Spawn the connection handler (non-blocking)
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("❌ Postgres connection error: {e}");
        }
    });

    // --- TRUNCATE ---
    match client.execute("TRUNCATE TABLE stock_price_history", &[] as &[&(dyn ToSql + Sync)]).await {
        Ok(_) => println!("✅ TRUNCATE succeeded"),
        Err(e) => eprintln!("❌ TRUNCATE failed: {e}"),
    }

    // --- VACUUM ---
    match client.execute("VACUUM stock_price_history", &[] as &[&(dyn ToSql + Sync)]).await {
        Ok(_) => println!("✅ VACUUM succeeded"),
        Err(e) => eprintln!("❌ VACUUM failed: {e}"),
    }

    println!("✨ Cleaner finished");
}
