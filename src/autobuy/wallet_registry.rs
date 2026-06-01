//! Multi-wallet copy-trade: server-side keys from env, one broker per wallet.

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
};

use serde::{Deserialize, Serialize};
use solana_address::Address;
use solana_keypair::{Keypair, Signer};

use crate::{
    autobuy::{
        broker::Broker,
        broker_mock::MockBroker,
        execution::{ExecutionConfig, ExecutionMode},
        pump_brocker::SolanaBroker,
    },
    generalize::{general_commands::TradeAction, general_pool::Pool},
};

/// Per-wallet entry in `filter_config.yaml` (no secret values — env var names only).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalletEntryConfig {
    pub id: String,
    #[serde(default)]
    pub label: String,
    #[serde(default = "default_wallet_enabled")]
    pub enabled: bool,
    /// Env var holding base58 private key, e.g. `PRIVATE_KEY_WALLET_1`.
    pub private_key_env: String,
    /// Fixed buy size in SOL; `None` = use tier amount from the signal.
    #[serde(default)]
    pub size_sol: Option<f64>,
    /// Demo mode starting balance for this wallet (defaults to global `start_balance_sol`).
    #[serde(default)]
    pub demo_balance_sol: Option<f64>,
}

fn default_wallet_enabled() -> bool {
    true
}

impl WalletEntryConfig {
    pub fn effective_label(&self) -> String {
        if self.label.is_empty() {
            self.id.clone()
        } else {
            self.label.clone()
        }
    }
}

/// Runtime handle for one trading wallet.
pub struct WalletHandle {
    pub id: String,
    pub label: String,
    pub enabled: AtomicBool,
    pub size_sol: RwLock<Option<f64>>,
    pub private_key_env: String,
    pub pubkey: String,
    pub wallet_address: Address,
    pub broker: Arc<dyn Broker>,
    pub balance: AtomicU64,
}

impl WalletHandle {
    pub fn balance_sol(&self) -> f64 {
        f64::from_bits(self.balance.load(Ordering::Relaxed))
    }

    pub fn set_balance_sol(&self, bal: f64) {
        self.balance.store(bal.to_bits(), Ordering::Relaxed);
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    pub fn set_enabled(&self, on: bool) {
        self.enabled.store(on, Ordering::Relaxed);
    }

    pub fn size_sol(&self) -> Option<f64> {
        *self.size_sol.read().unwrap_or_else(|e| e.into_inner())
    }

    pub fn set_size_sol(&self, v: Option<f64>) {
        *self.size_sol.write().unwrap_or_else(|e| e.into_inner()) = v;
    }

    pub fn amount_for_signal(&self, signal_sol: f64) -> f64 {
        self.size_sol().unwrap_or(signal_sol)
    }

    pub fn wire_snapshot(&self) -> WalletWire {
        WalletWire {
            id: self.id.clone(),
            label: self.label.clone(),
            enabled: self.is_enabled(),
            pubkey: self.pubkey.clone(),
            balance_sol: self.balance_sol(),
            size_sol: self.size_sol(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WalletWire {
    pub id: String,
    pub label: String,
    pub enabled: bool,
    pub pubkey: String,
    pub balance_sol: f64,
    pub size_sol: Option<f64>,
}

pub struct WalletRegistry {
    ordered: Vec<Arc<WalletHandle>>,
    by_id: HashMap<String, Arc<WalletHandle>>,
    by_pubkey: HashMap<Address, Arc<WalletHandle>>,
    mode_label: &'static str,
}

impl WalletRegistry {
    pub fn mode_label(&self) -> &'static str {
        self.mode_label
    }

    pub fn all(&self) -> &[Arc<WalletHandle>] {
        &self.ordered
    }

    pub fn get(&self, id: &str) -> Option<Arc<WalletHandle>> {
        self.by_id.get(id).cloned()
    }

    pub fn enabled_wallets(&self) -> Vec<Arc<WalletHandle>> {
        self.ordered
            .iter()
            .filter(|w| w.is_enabled())
            .cloned()
            .collect()
    }

    pub fn total_balance_sol(&self) -> f64 {
        self.ordered.iter().map(|w| w.balance_sol()).sum()
    }

    pub fn primary_pubkey(&self) -> String {
        self.ordered
            .first()
            .map(|w| w.pubkey.clone())
            .unwrap_or_else(|| "no-wallet".to_string())
    }

    pub fn wire_snapshots(&self) -> Vec<WalletWire> {
        self.ordered.iter().map(|w| w.wire_snapshot()).collect()
    }

    pub fn on_trade(&self, trade: &TradeAction, pool: &dyn Pool) {
        let trader = trade.trader();
        if let Some(w) = self.by_pubkey.get(&trader) {
            w.broker.on_trade(trade, pool);
        }
    }

    pub async fn refresh_all_balances(&self, refresh_every_n_ticks: u64, tick: u64) {
        for w in &self.ordered {
            if tick.is_multiple_of(refresh_every_n_ticks)
                && let Err(e) = w.broker.refresh_onchain_balance().await
            {
                eprintln!("[BROKER] {} balance refresh failed: {e}", w.id);
            }
            if let Ok(bal) = w.broker.balance_sol().await {
                w.set_balance_sol(bal);
            }
        }
    }

    pub fn apply_config_patch(&self, entries: &[WalletEntryConfig]) {
        for e in entries {
            if let Some(w) = self.by_id.get(&e.id) {
                w.set_enabled(e.enabled);
                w.set_size_sol(e.size_sol);
            }
        }
    }
}

/// When yaml has no `wallets` section, use a single wallet from `PRIVATE_KEY`.
pub fn default_wallet_entries() -> Vec<WalletEntryConfig> {
    vec![WalletEntryConfig {
        id: "wallet_1".to_string(),
        label: "Main".to_string(),
        enabled: true,
        private_key_env: "PRIVATE_KEY".to_string(),
        size_sol: None,
        demo_balance_sol: None,
    }]
}

pub async fn build_wallet_registry(
    entries: &[WalletEntryConfig],
    execution: &ExecutionConfig,
    default_start_balance_sol: f64,
) -> Result<WalletRegistry, String> {
    let effective: Vec<WalletEntryConfig> = if entries.is_empty() {
        default_wallet_entries()
    } else {
        entries.to_vec()
    };

    let mut ordered = Vec::new();
    let mut by_id = HashMap::new();
    let mut by_pubkey = HashMap::new();
    let mode_label = match execution.mode {
        ExecutionMode::Demo => "demo",
        ExecutionMode::Live => "live",
    };

    for cfg in effective {
        if by_id.contains_key(&cfg.id) {
            return Err(format!("duplicate wallet id {}", cfg.id));
        }

        let (broker, pubkey_str, wallet_address, initial_bal) = match execution.mode {
            ExecutionMode::Demo => {
                let start = cfg.demo_balance_sol.unwrap_or(default_start_balance_sol);
                let broker: Arc<dyn Broker> = Arc::new(MockBroker::new(start));
                let kp = Keypair::new();
                let pubkey = kp.pubkey().to_string();
                let addr: Address = pubkey
                    .parse()
                    .map_err(|e| format!("demo pubkey parse: {e}"))?;
                (broker, pubkey, addr, start)
            }
            ExecutionMode::Live => {
                let private_key = match std::env::var(&cfg.private_key_env) {
                    Ok(k) => k,
                    Err(_) if !cfg.enabled => {
                        eprintln!(
                            "[EXEC] wallet={} skipped: disabled and env {} not set (add key + restart to enable)",
                            cfg.id, cfg.private_key_env
                        );
                        continue;
                    }
                    Err(_) => {
                        return Err(format!(
                            "env {} must be set for wallet {} (live mode, enabled)",
                            cfg.private_key_env, cfg.id
                        ));
                    }
                };
                let rpc_url = std::env::var("SOLANA_RPC_URL")
                    .or_else(|_| std::env::var("SOLANA_HTTP"))
                    .map_err(|_| {
                        "SOLANA_RPC_URL (or SOLANA_HTTP) env var must be set for live mode"
                            .to_string()
                    })?;
                let keypair = Arc::new(Keypair::from_base58_string(&private_key));
                let pubkey = keypair.pubkey().to_string();
                let wallet_address: Address = pubkey
                    .parse()
                    .map_err(|e| format!("Failed to parse wallet {} pubkey: {e}", cfg.id))?;
                let broker = SolanaBroker::new(
                    rpc_url,
                    wallet_address,
                    keypair,
                    execution.live.clone(),
                    default_start_balance_sol,
                )
                .await
                .map_err(|e| format!("SolanaBroker init for {} failed: {e}", cfg.id))?;
                let bal = broker
                    .balance_sol()
                    .await
                    .unwrap_or(default_start_balance_sol);
                (
                    Arc::new(broker) as Arc<dyn Broker>,
                    pubkey,
                    wallet_address,
                    bal,
                )
            }
        };

        println!(
            "[EXEC] wallet={} label={} mode={} pubkey={} enabled={} size_sol={:?}",
            cfg.id,
            cfg.effective_label(),
            mode_label,
            pubkey_str,
            cfg.enabled,
            cfg.size_sol,
        );

        let handle = Arc::new(WalletHandle {
            id: cfg.id.clone(),
            label: cfg.effective_label(),
            enabled: AtomicBool::new(cfg.enabled),
            size_sol: RwLock::new(cfg.size_sol),
            private_key_env: cfg.private_key_env.clone(),
            pubkey: pubkey_str,
            wallet_address,
            broker,
            balance: AtomicU64::new(initial_bal.to_bits()),
        });
        by_pubkey.insert(wallet_address, handle.clone());
        by_id.insert(cfg.id.clone(), handle.clone());
        ordered.push(handle);
    }

    Ok(WalletRegistry {
        ordered,
        by_id,
        by_pubkey,
        mode_label,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn amount_for_signal_uses_wallet_override() {
        let h = WalletHandle {
            id: "wallet_2".to_string(),
            label: "Copy".to_string(),
            enabled: AtomicBool::new(true),
            size_sol: RwLock::new(Some(0.05)),
            private_key_env: "PRIVATE_KEY_WALLET_2".to_string(),
            pubkey: "pub".to_string(),
            wallet_address: "11111111111111111111111111111111"
                .parse()
                .unwrap(),
            broker: Arc::new(MockBroker::new(1.0)),
            balance: AtomicU64::new(1.0f64.to_bits()),
        };
        assert!((h.amount_for_signal(0.4) - 0.05).abs() < f64::EPSILON);
        h.set_size_sol(None);
        assert!((h.amount_for_signal(0.4) - 0.4).abs() < f64::EPSILON);
    }

    #[test]
    fn default_wallet_entries_single_main() {
        let e = default_wallet_entries();
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].id, "wallet_1");
        assert_eq!(e[0].private_key_env, "PRIVATE_KEY");
    }
}
