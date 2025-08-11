use std::{collections::HashMap, env, time::Duration};

use chrono::{Utc, TimeZone};
use dotenv::dotenv;
use futures::{stream::StreamExt, SinkExt};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use tokio::{task::LocalSet, time::sleep};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};

const SYMBOLS_KEY: &str = "stock:symbols";
const PRICE_PREFIX: &str = "stock:price:";
const TRADE_PREFIX: &str = "stock:trade:";
const OHLCV_PREFIX: &str = "stock:ohlcv:";

#[derive(Debug, Deserialize)]
struct WebSocketMessage {
    r#type: String,
    data: Option<Vec<TradeData>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
struct TradeData {
    s: String,     // symbol
    p: f64,        // price
    v: Option<f64>,// volume
    t: i64,        // trade time in ms since epoch
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();
    let local = LocalSet::new();

    local
        .run_until(async {
            if let Err(e) = run().await {
                eprintln!("❌ Application error: {}", e);
            }
        })
        .await;

    Ok(())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let api_key = env::var("FINNHUB_API_KEY")?;
    let redis_url = env::var("REDIS_URL")?;

    // --- Auto-handle TLS for Redis ---
    let redis_client = redis::Client::open(redis_url.clone())?;
    if redis_url.starts_with("rediss://") {
        println!("🔐 Connecting to Redis with TLS...");
    } else {
        println!("🌐 Connecting to Redis without TLS...");
    }

    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    println!("✅ Connected to Redis");

    // WebSocket URL
    let ws_url = url::Url::parse(&format!("wss://ws.finnhub.io?token={}", api_key))?;

    // OHLCV in-memory state: symbol -> (open, high, low, close, volume)
    let mut ohlcv_map: HashMap<String, (f64, f64, f64, f64, f64)> = HashMap::new();

    let mut reconnect_delay = Duration::from_secs(3);

    loop {
        println!("🌐 Attempting connection to Finnhub WebSocket...");

        match connect_async(ws_url.clone()).await {
            Ok((mut ws_stream, _)) => {
                println!("✅ WebSocket connected successfully.");
                reconnect_delay = Duration::from_secs(3);
                let mut last_symbols = Vec::new();

                loop {
                    // Refresh subscriptions
                    match redis.smembers::<_, Vec<String>>(SYMBOLS_KEY).await {
                        Ok(current_symbols) => {
                            if current_symbols != last_symbols {
                                last_symbols = current_symbols.clone();

                                if current_symbols.is_empty() {
                                    println!("⚠️ No stock symbols in '{}'", SYMBOLS_KEY);
                                    continue;
                                }

                                println!(
                                    "🔄 Updating subscriptions for {} symbols...",
                                    current_symbols.len()
                                );
                                for sym in &current_symbols {
                                    let msg =
                                        format!(r#"{{"type":"subscribe","symbol":"{}"}}"#, sym);
                                    if let Err(e) = ws_stream.send(Message::Text(msg.into())).await {
                                        eprintln!("❌ Failed to subscribe {}: {}", sym, e);
                                    }
                                    sleep(Duration::from_millis(50)).await;
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("❌ Redis symbol fetch error: {}", e);
                            sleep(Duration::from_secs(5)).await;
                            continue;
                        }
                    }

                    // Process incoming WebSocket messages
                    while let Some(msg) = ws_stream.next().await {
                        match msg {
                            Ok(Message::Text(text)) => {
                                if let Ok(parsed) =
                                    serde_json::from_str::<WebSocketMessage>(&text)
                                {
                                    if parsed.r#type == "trade" {
                                        if let Some(trades) = parsed.data {
                                            let mut redis_conn = match redis_client
                                                .get_multiplexed_async_connection()
                                                .await
                                            {
                                                Ok(conn) => conn,
                                                Err(e) => {
                                                    eprintln!(
                                                        "❌ Redis reconnect error: {}",
                                                        e
                                                    );
                                                    continue;
                                                }
                                            };

                                            for trade in trades {
                                                let symbol = trade.s.clone();
                                                let price = trade.p;
                                                let volume = trade.v.unwrap_or(0.0);

                                                // Convert Finnhub's trade.t (ms since epoch) to RFC3339
                                                let trade_time = Utc.timestamp_millis_opt(trade.t)
                                                    .single()
                                                    .expect("Invalid trade timestamp");
                                                let trade_time_str = trade_time.to_rfc3339();

                                                // 1) Price + trade info
                                                let _ = redis_conn
                                                    .set::<_, _, ()>(
                                                        format!("{}{}", PRICE_PREFIX, symbol),
                                                        price,
                                                    )
                                                    .await;
                                                let _ = redis_conn
                                                    .hset_multiple::<_, _, _, ()>(
                                                        format!("{}{}", TRADE_PREFIX, symbol),
                                                        &[
                                                            ("price".to_string(),
                                                                price.to_string()),
                                                            ("timestamp".to_string(),
                                                                trade.t.to_string()),
                                                            ("volume".to_string(),
                                                                volume.to_string()),
                                                            ("updated_at".to_string(),
                                                                trade_time_str.clone()),
                                                        ],
                                                    )
                                                    .await;

                                                // 2) Update OHLCV state
                                                let entry = ohlcv_map
                                                    .entry(symbol.clone())
                                                    .or_insert((
                                                        price, // open
                                                        price, // high
                                                        price, // low
                                                        price, // close
                                                        0.0,   // volume
                                                    ));
                                                entry.1 = entry.1.max(price); // high
                                                entry.2 = entry.2.min(price); // low
                                                entry.3 = price; // close
                                                entry.4 += volume; // volume

                                                // 3) Immediate OHLCV flush to Redis
                                                let fields = [
                                                    ("open".to_string(), entry.0.to_string()),
                                                    ("high".to_string(), entry.1.to_string()),
                                                    ("low".to_string(), entry.2.to_string()),
                                                    ("close".to_string(), entry.3.to_string()),
                                                    ("volume".to_string(), entry.4.to_string()),
                                                    ("updated_at".to_string(),
                                                        trade_time_str.clone()),
                                                ];
                                                let _ = redis_conn
                                                    .hset_multiple::<_, _, _, ()>(
                                                        format!("{}{}", OHLCV_PREFIX, symbol),
                                                        &fields,
                                                    )
                                                    .await;
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(_) => {}
                            Err(e) => {
                                eprintln!("❌ WebSocket stream error: {}", e);
                                break;
                            }
                        }
                    }

                    println!("🔁 WebSocket disconnected. Retrying...");
                    break;
                }
            }
            Err(e) => {
                eprintln!("❌ Connection error: {}", e);
            }
        }

        println!("⏳ Waiting {}s before retry...", reconnect_delay.as_secs());
        sleep(reconnect_delay).await;
        reconnect_delay = (reconnect_delay * 2).min(Duration::from_secs(60));
    }
}
