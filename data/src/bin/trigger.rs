use chrono::{NaiveDate, NaiveTime, Timelike, Utc};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::{
    task::{JoinHandle, spawn_local},
    time::{sleep, timeout, Duration, Instant},
};
use data_collection::{cleaner, fetcher};

//------------------------------------CONFIG & CONSTRAINTS--------------------------------------------------------

const MAINT_START: NaiveTime = NaiveTime::from_hms_opt(5, 0, 0).unwrap(); // 05:00–05:05 UTC
const MAINT_END: NaiveTime = NaiveTime::from_hms_opt(5, 5, 0).unwrap();
const CLEAN_TIME: NaiveTime = NaiveTime::from_hms_opt(5, 3, 0).unwrap(); // 05:02 UTC

const LOOP_TICK: Duration = Duration::from_secs(2);
const FETCHER_JOIN_TIMEOUT: Duration = Duration::from_secs(10);
const RESTART_DEBOUNCE: Duration = Duration::from_secs(3);

// -----------------------------------FETCHER PROCESS STRUCTURE------------------------------------------------------------------------------

struct FetcherProc {
    flag: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    last_start: Option<Instant>,
}

impl FetcherProc {
    fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            handle: None,
            last_start: None,
        }
    }

    fn is_running(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
            && self.handle.as_ref().map(|h| !h.is_finished()).unwrap_or(false)
    }

    async fn start(&mut self) {
        if self.is_running() {
            return;
        }
        if let Some(t) = self.last_start {
            if t.elapsed() < RESTART_DEBOUNCE {
                return;
            }
        }
        self.flag.store(true, Ordering::Relaxed);
        let flag = self.flag.clone();
        self.handle = Some(spawn_local(async move {
            let _ = fetcher::run(flag).await;
        }));
        self.last_start = Some(Instant::now());
        println!("✅ fetcher started");
    }

    async fn stop(&mut self) {
        self.flag.store(false, Ordering::Relaxed);

        if let Some(handle) = self.handle.take() {
            println!("🛑 stopping fetcher…");
            match timeout(FETCHER_JOIN_TIMEOUT, handle).await {
                Ok(join_res) => {
                    if let Err(e) = join_res {
                        eprintln!("⚠️ fetcher task panicked: {e}");
                    } else {
                        println!("🧹 fetcher stopped cleanly");
                    }
                }
                Err(_) => {
                    eprintln!(
                        "⏳ fetcher didn’t stop in {:?}; force-abort",
                        FETCHER_JOIN_TIMEOUT
                    );
                }
            }
        }
    }
}

//-----------------------------------MAIN LOOP------------------------------------------------------------------

// Must be current_thread for spawn_local to work
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut fetcher = FetcherProc::new();
    let mut last_cleaned: Option<NaiveDate> = None;
    let mut last_pushed: Option<NaiveDate> = None;

    loop {
        let tick_start = Instant::now();
        let now = Utc::now();
        let today = now.date_naive();
        let t = now.time();

        let in_window = t >= MAINT_START && t < MAINT_END;

        //--------------------------------------GITHUB PUSH-------------------------------------------------
        if in_window && t < CLEAN_TIME && last_pushed != Some(today) {
            println!(
                "📤 launching GitHub pusher at {}",
                now.format("%Y-%m-%d %H:%M:%S UTC")
            );

            let push_status = tokio::process::Command::new("python3")
                .arg("scripts/push.py") // adjust path if needed
                .output()
                .await;

            match push_status {
                Ok(o) if o.status.success() => println!("✅ GitHub push completed"),
                Ok(o) => eprintln!(
                    "❌ GitHub push failed:\n{}",
                    String::from_utf8_lossy(&o.stderr)
                ),
                Err(e) => eprintln!("🚨 Failed to launch push.py: {e}"),
            }

            last_pushed = Some(today);
        }

        // --------------------------------------CLEANER----------------------------------------
        if in_window && t >= CLEAN_TIME && last_cleaned != Some(today) {
            println!("🧼 cleaner starting at {}", now.format("%Y-%m-%d %H:%M:%S UTC"));
            cleaner::run().await;
            last_cleaned = Some(today);
            println!("✅ cleaner completed via trigger.rs");
        }

        //--------------------------------FETCHER LIFECYCLE MANAGEMENT-----------------------------------------------
        if in_window {
            if fetcher.is_running() {
                fetcher.stop().await;
            }
        } else {
            if !fetcher.is_running() {
                fetcher.start().await;
            }
        }

        // --------------------------------DRIFT-CORRECTED SLEEP-----------------------------------------------------------
        let elapsed = tick_start.elapsed();
        sleep(LOOP_TICK.saturating_sub(elapsed)).await;
    }
}
