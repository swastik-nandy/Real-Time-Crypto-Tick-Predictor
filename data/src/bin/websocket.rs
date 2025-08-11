use std::{collections::HashMap, env, time::Duration};

use chrono::Utc;
use dotenv::dotenv;
use futures::{stream::StreamExt, SinkExt};
use redis::{aio::MultiplexedConnection, AsyncCommands};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    sync::mpsc,
    task::LocalSet,
    time::{sleep, timeout, Instant},
};
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
    s: String,
    p: f64,
    v: Option<f64>,
    t: i64,
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
    let redis_client = if redis_url.starts_with("rediss://") {
        println!("🔐 Connecting to Redis with TLS...");
        redis::Client::open(redis_url)?
    } else {
        println!("🌐 Connecting to Redis without TLS...");
        redis::Client::open(redis_url)?
    };

    let mut redis = redis_client.get_multiplexed_async_connection().await?;
    println!("✅ Connected to Redis");

    // WebSocket URL
    let ws_url = url::Url::parse(&format!("wss://ws.finnhub.io?token={}", api_key))?;

    let (tx, mut rx) = mpsc::channel::<TradeData>(100_000);

    // Spawn OHLCV flush in local task
    tokio::task::spawn_local(async move {
        periodic_ohlcv_flush(redis_client.clone(), &mut rx).await;
    });

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

                                println!("🔄 Updating subscriptions...");
                                for sym in &current_symbols {
                                    let msg = json!({ "type": "subscribe", "symbol": sym }).to_string();
                                    if let Err(e) = ws_stream.send(Message::Text(msg.into())).await {
                                        eprintln!("❌ Failed to subscribe {}: {}", sym, e);
                                    } else {
                                        println!("📡 Subscribed to {}", sym);
                                    }
                                    sleep(Duration::from_millis(100)).await;
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
                                if let Ok(parsed) = serde_json::from_str::<WebSocketMessage>(&text) {
                                    if parsed.r#type == "trade" {
                                        if let Some(trades) = parsed.data {
                                            for trade in trades {
                                                if tx.send(trade).await.is_err() {
                                                    eprintln!("⚠️ Dropped trade due to full channel buffer");
                                                }
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

async fn periodic_ohlcv_flush(
    client: redis::Client,
    rx: &mut mpsc::Receiver<TradeData>,
) {
    let mut ohlcv_buffer: HashMap<String, Vec<TradeData>> = HashMap::new();

    loop {
        let mut redis: MultiplexedConnection = match client.get_multiplexed_async_connection().await
        {
            Ok(conn) => conn,
            Err(e) => {
                eprintln!("❌ Redis reconnect error: {}", e);
                sleep(Duration::from_secs(3)).await;
                continue;
            }
        };

        let start = Instant::now();
        let mut trades_received = 0;

        while start.elapsed() < Duration::from_secs(10) {
            match timeout(Duration::from_millis(200), rx.recv()).await {
                Ok(Some(trade)) => {
                    trades_received += 1;
                    let symbol = &trade.s;
                    let now = Utc::now().to_rfc3339();
                    let volume = trade.v.unwrap_or(0.0);

                    let trade_info = json!({
                        "price": trade.p,
                        "timestamp": trade.t,
                        "volume": volume,
                        "updated_at": now
                    });

                    let trade_fields: Vec<(String, String)> = trade_info
                        .as_object()
                        .unwrap()
                        .iter()
                        .map(|(k, v)| (k.clone(), v.to_string()))
                        .collect();

                    let _ = redis
                        .set::<_, _, ()>(format!("{}{}", PRICE_PREFIX, symbol), trade.p)
                        .await;
                    let _ = redis
                        .hset_multiple::<_, _, _, ()>(
                            format!("{}{}", TRADE_PREFIX, symbol),
                            &trade_fields,
                        )
                        .await;

                    ohlcv_buffer.entry(symbol.clone()).or_default().push(trade);
                }
                Ok(None) => break,
                Err(_) => continue,
            }
        }

        let now = Utc::now().to_rfc3339();

        let flushed_symbols = if trades_received == 0 {
            println!("⏳ No trades received → no OHLCV to flush");
            0
        } else {
            println!("🔄 Flushing OHLCV for {} symbols", ohlcv_buffer.len());
            let mut success_count = 0;

            for (symbol, trades) in ohlcv_buffer.drain() {
                if trades.is_empty() {
                    continue;
                }

                let prices: Vec<f64> = trades.iter().map(|t| t.p).collect();
                let volumes: Vec<f64> = trades.iter().map(|t| t.v.unwrap_or(0.0)).collect();

                let ohlcv = json!({
                    "open": prices.first().unwrap(),
                    "high": prices.iter().cloned().fold(f64::MIN, f64::max),
                    "low": prices.iter().cloned().fold(f64::MAX, f64::min),
                    "close": prices.last().unwrap(),
                    "volume": volumes.iter().sum::<f64>(),
                    "updated_at": now
                });

                let redis_fields: Vec<(String, String)> = ohlcv
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| (k.clone(), v.to_string()))
                    .collect();

                match redis
                    .hset_multiple::<_, _, _, ()>(
                        format!("{}{}", OHLCV_PREFIX, symbol),
                        &redis_fields,
                    )
                    .await
                {
                    Ok(_) => success_count += 1,
                    Err(e) => eprintln!("❌ OHLCV flush failed for {}: {}", symbol, e),
                }
            }

            success_count
        };

        println!("✅ Successfully updated {} symbols to Redis", flushed_symbols);

        let elapsed = start.elapsed();
        if elapsed < Duration::from_secs(10) {
            sleep(Duration::from_secs(10) - elapsed).await;
        }
    }
}
