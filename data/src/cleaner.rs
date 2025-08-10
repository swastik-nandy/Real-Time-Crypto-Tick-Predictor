use std::{env, sync::Arc};
use tokio_postgres::types::ToSql;

// postgres + rustls
use postgres_rustls::{MakeTlsConnector, TlsConnector, set_postgresql_alpn};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};
use rustls_native_certs::load_native_certs;

fn with_ssl_require(url: &str) -> String {
    if url.contains("sslmode=") { url.to_string() }
    else if url.contains('?') { format!("{url}&sslmode=require") }
    else { format!("{url}?sslmode=require") }
}

pub async fn run() {
    println!("🧼 Cleaner starting…");
    dotenv::dotenv().ok();

    let mut pg_url = env::var("DATABASE_URL").expect("Postgres URL not set");
    pg_url = with_ssl_require(&pg_url);

    // Build rustls config with native roots
    let mut roots = RootCertStore::empty();
    for cert in load_native_certs().expect("load platform certs") {
        // rustls >=0.23 uses owned DER; add() takes by value
        roots.add(cert).expect("add platform cert");
    }

    let mut cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    // Harmless for postgres negotiation; required if you switch to direct TLS in the future
    set_postgresql_alpn(&mut cfg);

    // Wrap into a tokio-postgres compatible connector
    let tls = MakeTlsConnector::new(TlsConnector::from(Arc::new(cfg)));

    let (client, connection) = tokio_postgres::connect(&pg_url, tls)
        .await
        .expect("Failed to connect to Postgres over TLS");

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
