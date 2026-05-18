//! Smart-money tracking. Wallets that consistently appear among the early
//! buyers of *our* profitable trades get a positive score; the opposite for
//! wallets that show up in our losing trades. Persisted as JSON.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use solana_address::Address;
use tokio::sync::{mpsc, oneshot};

use crate::scoring::config::PersistenceConfig;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WalletRecord {
    pub trades: u64,
    pub wins: u64,
    pub losses: u64,
    /// Sum of pnl_pct across closed trades the wallet was an early buyer of.
    pub pnl_pct_sum: f64,
    pub last_seen_unix: u64,
}

impl WalletRecord {
    pub fn winrate(&self) -> f64 {
        if self.trades == 0 {
            0.0
        } else {
            self.wins as f64 / self.trades as f64
        }
    }
    pub fn avg_pnl_pct(&self) -> f64 {
        if self.trades == 0 {
            0.0
        } else {
            self.pnl_pct_sum / self.trades as f64
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct SmartMoneySnapshot {
    pub total_wallets: usize,
    pub smart_wallets: usize,
}

#[derive(Debug)]
enum Msg {
    UpdateOutcomes {
        wallets: Vec<Address>,
        pnl_pct: f64,
    },
    CountSmart {
        wallets: Vec<Address>,
        respond_to: oneshot::Sender<u32>,
    },
    /// Wallets from `wallets` that qualify as "smart" in the registry.
    FilterSmart {
        wallets: Vec<Address>,
        respond_to: oneshot::Sender<Vec<Address>>,
    },
    Snapshot {
        respond_to: oneshot::Sender<SmartMoneySnapshot>,
    },
}

#[derive(Clone)]
pub struct SmartMoneyHandle {
    tx: mpsc::Sender<Msg>,
}

impl SmartMoneyHandle {
    pub async fn note_trade_outcome(&self, wallets: Vec<Address>, pnl_pct: f64) {
        let _ = self.tx.send(Msg::UpdateOutcomes { wallets, pnl_pct }).await;
    }

    pub async fn count_smart(&self, wallets: Vec<Address>) -> u32 {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(Msg::CountSmart {
                wallets,
                respond_to: tx,
            })
            .await;
        rx.await.unwrap_or(0)
    }

    pub async fn filter_smart_wallets(&self, wallets: Vec<Address>) -> Vec<Address> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(Msg::FilterSmart {
                wallets,
                respond_to: tx,
            })
            .await;
        rx.await.unwrap_or_default()
    }

    pub async fn snapshot(&self) -> SmartMoneySnapshot {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(Msg::Snapshot { respond_to: tx }).await;
        rx.await.unwrap_or(SmartMoneySnapshot {
            total_wallets: 0,
            smart_wallets: 0,
        })
    }
}

pub fn spawn(config: PersistenceConfig) -> SmartMoneyHandle {
    let (tx, mut rx) = mpsc::channel::<Msg>(2048);
    let handle = SmartMoneyHandle { tx };
    let path = PathBuf::from(&config.smart_money_path);
    let flush_secs = config.flush_every_secs.max(1);
    let ttl = config.entity_ttl_secs;

    tokio::spawn(async move {
        let mut state: HashMap<String, WalletRecord> = load_state(&path).await;
        let mut dirty = false;
        let mut flush_ticker = tokio::time::interval(std::time::Duration::from_secs(flush_secs));
        flush_ticker.tick().await;

        loop {
            tokio::select! {
                Some(msg) = rx.recv() => {
                    match msg {
                        Msg::UpdateOutcomes { wallets, pnl_pct } => {
                            let now = unix_now();
                            for w in wallets {
                                let entry = state.entry(w.to_string()).or_default();
                                entry.trades += 1;
                                entry.pnl_pct_sum += pnl_pct;
                                if pnl_pct > 0.0 {
                                    entry.wins += 1;
                                } else {
                                    entry.losses += 1;
                                }
                                entry.last_seen_unix = now;
                            }
                            dirty = true;
                        }
                        Msg::CountSmart { wallets, respond_to } => {
                            let now = unix_now();
                            let mut count = 0_u32;
                            for w in wallets {
                                if let Some(rec) = state.get(&w.to_string())
                                    && is_smart(rec, now, ttl) {
                                    count += 1;
                                }
                            }
                            let _ = respond_to.send(count);
                        }
                        Msg::FilterSmart {
                            wallets,
                            respond_to,
                        } => {
                            let now = unix_now();
                            let mut out = Vec::new();
                            for w in wallets {
                                if let Some(rec) = state.get(&w.to_string())
                                    && is_smart(rec, now, ttl) {
                                    out.push(w);
                                }
                            }
                            let _ = respond_to.send(out);
                        }
                        Msg::Snapshot { respond_to } => {
                            let now = unix_now();
                            let total = state.len();
                            let smart = state.values().filter(|r| is_smart(r, now, ttl)).count();
                            let _ = respond_to.send(SmartMoneySnapshot {
                                total_wallets: total,
                                smart_wallets: smart,
                            });
                        }
                    }
                }
                _ = flush_ticker.tick() => {
                    if dirty {
                        if let Err(e) = save_state(&path, &state).await {
                            eprintln!("[smart_money] flush failed: {e}");
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

/// "Smart" = stat-positive AND has a non-trivial sample size AND not stale.
/// We deliberately bake the threshold in here (not in YAML) — these are
/// criteria for *this* heuristic, not knobs the operator should tune
/// independently from `count_smart`.
fn is_smart(rec: &WalletRecord, now: u64, ttl_secs: u64) -> bool {
    if ttl_secs > 0 && now.saturating_sub(rec.last_seen_unix) > ttl_secs {
        return false;
    }
    if rec.trades < 4 {
        return false;
    }
    rec.winrate() >= 0.6 && rec.avg_pnl_pct() >= 5.0
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

async fn load_state(path: &PathBuf) -> HashMap<String, WalletRecord> {
    match tokio::fs::read(path).await {
        Ok(bytes) => match serde_json::from_slice::<HashMap<String, WalletRecord>>(&bytes) {
            Ok(map) => map,
            Err(e) => {
                eprintln!("[smart_money] state file corrupted, starting fresh: {e}");
                HashMap::new()
            }
        },
        Err(_) => HashMap::new(),
    }
}

async fn save_state(
    path: &PathBuf,
    state: &HashMap<String, WalletRecord>,
) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        let _ = tokio::fs::create_dir_all(parent).await;
    }
    let bytes = serde_json::to_vec(state).map_err(|e| std::io::Error::other(e.to_string()))?;
    let tmp = path.with_extension("json.tmp");
    tokio::fs::write(&tmp, bytes).await?;
    tokio::fs::rename(tmp, path).await
}

pub type SharedSmartMoney = Arc<SmartMoneyHandle>;
