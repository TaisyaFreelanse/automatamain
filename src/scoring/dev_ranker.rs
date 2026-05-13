//! Dynamic dev ranking based on the bot's *own* closed trades.
//!
//! Replaces "developer is in the historical creator stats table → forever
//! good". A dev that we ourselves have traded badly into recently gets
//! demoted; a dev whose tokens we made money on gets promoted.
//!
//! Persisted as JSON so restarts don't reset the ranking.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use solana_address::Address;
use tokio::sync::{mpsc, oneshot};

use crate::scoring::config::PersistenceConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum DevCategory {
    APlus,
    A,
    Neutral,
    Bad,
    /// Dev hasn't been seen in TTL window — we treat them as if we have no
    /// information, neither bonus nor penalty.
    Stale,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct DevRecord {
    pub score: f64,
    pub total_tokens: u64,
    pub strong_successes: u64,
    pub successes: u64,
    pub neutrals: u64,
    pub rugs: u64,
    pub last_seen_unix: u64,
}

#[derive(Clone, Copy, Debug)]
pub enum TokenOutcome {
    StrongSuccess, // pnl_pct >= 50
    Success,       // pnl_pct >= 30
    Neutral,
    Rug, // pnl_pct <= -16
}

impl TokenOutcome {
    pub fn classify(pnl_pct: f64) -> Self {
        if pnl_pct >= 50.0 {
            Self::StrongSuccess
        } else if pnl_pct >= 30.0 {
            Self::Success
        } else if pnl_pct <= -16.0 {
            Self::Rug
        } else {
            Self::Neutral
        }
    }
}

// --- Actor ------------------------------------------------------------------

#[derive(Debug)]
enum Msg {
    UpdateOutcome {
        dev: Address,
        outcome: TokenOutcome,
    },
    GetCategory {
        dev: Address,
        respond_to: oneshot::Sender<(DevCategory, DevRecord)>,
    },
    SnapshotCounts {
        respond_to: oneshot::Sender<DevRankerSnapshot>,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct DevRankerSnapshot {
    pub total_devs: usize,
    pub a_plus: usize,
    pub a: usize,
    pub neutral: usize,
    pub bad: usize,
    pub stale: usize,
}

#[derive(Clone)]
pub struct DevRankerHandle {
    tx: mpsc::Sender<Msg>,
}

impl DevRankerHandle {
    pub async fn note_outcome(&self, dev: Address, outcome: TokenOutcome) {
        let _ = self.tx.send(Msg::UpdateOutcome { dev, outcome }).await;
    }

    pub async fn category(&self, dev: Address) -> (DevCategory, DevRecord) {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(Msg::GetCategory {
                dev,
                respond_to: tx,
            })
            .await;
        rx.await
            .unwrap_or((DevCategory::Neutral, DevRecord::default()))
    }

    pub async fn snapshot(&self) -> DevRankerSnapshot {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Msg::SnapshotCounts { respond_to: tx }).await;
        rx.await.unwrap_or(DevRankerSnapshot {
            total_devs: 0,
            a_plus: 0,
            a: 0,
            neutral: 0,
            bad: 0,
            stale: 0,
        })
    }
}

pub fn spawn(config: PersistenceConfig) -> DevRankerHandle {
    let (tx, mut rx) = mpsc::channel::<Msg>(2048);
    let handle = DevRankerHandle { tx };
    let path = PathBuf::from(&config.dev_ranker_path);
    let flush_secs = config.flush_every_secs.max(1);
    let ttl = config.entity_ttl_secs;

    tokio::spawn(async move {
        let mut state: HashMap<String, DevRecord> = load_state(&path).await;
        let mut dirty = false;
        let mut flush_ticker = tokio::time::interval(std::time::Duration::from_secs(flush_secs));
        flush_ticker.tick().await; // skip first immediate

        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    match msg {
                        Msg::UpdateOutcome { dev, outcome } => {
                            let key = dev.to_string();
                            let now = unix_now();
                            let entry = state.entry(key).or_default();
                            entry.total_tokens += 1;
                            match outcome {
                                TokenOutcome::StrongSuccess => {
                                    entry.score += 2.0;
                                    entry.strong_successes += 1;
                                }
                                TokenOutcome::Success => {
                                    entry.score += 1.0;
                                    entry.successes += 1;
                                }
                                TokenOutcome::Neutral => {
                                    entry.neutrals += 1;
                                }
                                TokenOutcome::Rug => {
                                    entry.score -= 2.0;
                                    entry.rugs += 1;
                                }
                            }
                            entry.last_seen_unix = now;
                            dirty = true;
                        }
                        Msg::GetCategory { dev, respond_to } => {
                            let key = dev.to_string();
                            let now = unix_now();
                            let result = match state.get(&key).cloned() {
                                Some(rec) => (categorize(&rec, now, ttl), rec),
                                None => (DevCategory::Neutral, DevRecord::default()),
                            };
                            let _ = respond_to.send(result);
                        }
                        Msg::SnapshotCounts { respond_to } => {
                            let now = unix_now();
                            let mut snap = DevRankerSnapshot {
                                total_devs: state.len(),
                                a_plus: 0,
                                a: 0,
                                neutral: 0,
                                bad: 0,
                                stale: 0,
                            };
                            for rec in state.values() {
                                match categorize(rec, now, ttl) {
                                    DevCategory::APlus => snap.a_plus += 1,
                                    DevCategory::A => snap.a += 1,
                                    DevCategory::Neutral => snap.neutral += 1,
                                    DevCategory::Bad => snap.bad += 1,
                                    DevCategory::Stale => snap.stale += 1,
                                }
                            }
                            let _ = respond_to.send(snap);
                        }
                    }
                }
                _ = flush_ticker.tick() => {
                    if dirty {
                        if let Err(e) = save_state(&path, &state).await {
                            eprintln!("[dev_ranker] flush failed: {e}");
                        } else {
                            dirty = false;
                        }
                    }
                }
                else => break,
            }
        }
    });

    handle
}

fn categorize(rec: &DevRecord, now: u64, ttl_secs: u64) -> DevCategory {
    if ttl_secs > 0 && now.saturating_sub(rec.last_seen_unix) > ttl_secs {
        return DevCategory::Stale;
    }
    if rec.score >= 3.0 {
        DevCategory::APlus
    } else if rec.score >= 1.0 {
        DevCategory::A
    } else if rec.score <= -2.0 {
        DevCategory::Bad
    } else {
        DevCategory::Neutral
    }
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn load_state(path: &PathBuf) -> HashMap<String, DevRecord> {
    match tokio::fs::read(path).await {
        Ok(bytes) => match serde_json::from_slice::<HashMap<String, DevRecord>>(&bytes) {
            Ok(map) => map,
            Err(e) => {
                eprintln!("[dev_ranker] state file corrupted, starting fresh: {e}");
                HashMap::new()
            }
        },
        Err(_) => HashMap::new(),
    }
}

async fn save_state(
    path: &PathBuf,
    state: &HashMap<String, DevRecord>,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    let bytes = serde_json::to_vec(state).map_err(|e| std::io::Error::other(e.to_string()))?;
    let tmp = path.with_extension("json.tmp");
    tokio::fs::write(&tmp, bytes).await?;
    tokio::fs::rename(tmp, path).await
}

// Allow Arc convenience for use sites that prefer cloning the handle through Arc.
pub type SharedDevRanker = Arc<DevRankerHandle>;
