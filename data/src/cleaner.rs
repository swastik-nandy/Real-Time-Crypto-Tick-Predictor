use std::env;
use tokio_postgres::NoTls;

pub async fn run() {
    dotenv::dotenv().ok();

    let pg_url = env::var("DATABASE_URL").expect("Postgres URL not set");

    let (client, connection) = tokio_postgres::connect(&pg_url, NoTls)
        .await
        .expect("Failed to connect to Postgres");

    // Spawn the connection handler (non-blocking)
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("❌ Postgres connection error: {}", e);
        }
    });

    println!("🧼 Cleaner started");

    // --- TRUNCATE ---
    match client.execute("TRUNCATE TABLE stock_price_history", &[]).await {
        Ok(_) => println!("✅ TRUNCATE succeeded"),
        Err(e) => eprintln!("❌ TRUNCATE failed: {e}"),
    }

    // --- VACUUM ---
    match client.execute("VACUUM stock_price_history", &[]).await {
        Ok(_) => println!("✅ VACUUM succeeded"),
        Err(e) => eprintln!("❌ VACUUM failed: {e}"),
    }

    println!("✨ Cleaner finished");
}
