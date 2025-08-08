use std::env;
use tokio_postgres::types::ToSql;

// TLS for Postgres via rustls
use postgres_rustls::MakeTlsConnect;
use rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;

pub async fn run() {
    println!("🧼 Cleaner starting…");
    dotenv::dotenv().ok();

    let pg_url = env::var("DATABASE_URL").expect("Postgres URL not set");

    // Build TLS config with native roots
    let mut root_store = RootCertStore::empty();
    for cert in load_native_certs().expect("Could not load platform certificates") {
        root_store.add(&cert).unwrap();
    }
    let tls_config = ClientConfig::builder()
        .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let tls = MakeTlsConnect::new(tls_config);

    let (client, connection) = tokio_postgres::connect(&pg_url, tls)
        .await
        .expect("Failed to connect to Postgres");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("❌ Postgres connection error: {e}");
        }
    });

    match client.execute("TRUNCATE TABLE stock_price_history", &[] as &[&(dyn ToSql + Sync)]).await {
        Ok(_) => println!("✅ TRUNCATE succeeded"),
        Err(e) => eprintln!("❌ TRUNCATE failed: {e}"),
    }

    match client.execute("VACUUM stock_price_history", &[] as &[&(dyn ToSql + Sync)]).await {
        Ok(_) => println!("✅ VACUUM succeeded"),
        Err(e) => eprintln!("❌ VACUUM failed: {e}"),
    }

    println!("✨ Cleaner finished");
}
