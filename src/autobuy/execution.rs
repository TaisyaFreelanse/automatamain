//! Execution layer: choose between `MockBroker` (Demo) and `SolanaBroker`
//! (Live) using a single config block. The rest of the bot is broker-agnostic
//! and works against the `Broker` trait, so nothing else needs to change when
//! flipping the mode.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use solana_keypair::{Keypair, Signer};

use crate::autobuy::{
    broker::Broker, broker_mock::MockBroker, pump_brocker::SolanaBroker,
};

// --- Mode --------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum ExecutionMode {
    /// In-memory simulated trading (no RPC, no wallet).
    #[default]
    Demo,
    /// Real on-chain execution via Solana RPC + signed transactions.
    Live,
}

// --- Live-only knobs ---------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiveExecutionConfig {
    /// Slippage tolerance in basis points (100 = 1%). Used to derive
    /// `min_token_out` on buy and `min_sol_out` on sell from the pool's
    /// current price.
    #[serde(default = "default_slippage_bps")]
    pub slippage_bps: u32,

    /// Priority fee in micro-lamports per compute unit. ComputeBudget
    /// `SetComputeUnitPrice` instruction. 0 disables the priority fee.
    #[serde(default = "default_priority_fee")]
    pub priority_fee_micro_lamports: u64,

    /// Hard compute-unit ceiling for the tx (ComputeBudget
    /// `SetComputeUnitLimit`). 0 keeps the default cluster limit.
    #[serde(default = "default_cu_limit")]
    pub compute_unit_limit: u32,

    /// How many times to retry a transient `send_transaction` failure.
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// How often the background task re-reads the wallet balance from RPC.
    /// Cached value is what `Broker::balance_sol()` returns between refreshes.
    #[serde(default = "default_balance_refresh_secs")]
    pub balance_refresh_secs: u64,

    /// Skip preflight simulation on send (faster, but you get less info on
    /// bad txs). Recommended `false` while you're validating live.
    #[serde(default)]
    pub skip_preflight: bool,
}

impl Default for LiveExecutionConfig {
    fn default() -> Self {
        Self {
            slippage_bps: default_slippage_bps(),
            priority_fee_micro_lamports: default_priority_fee(),
            compute_unit_limit: default_cu_limit(),
            max_retries: default_max_retries(),
            balance_refresh_secs: default_balance_refresh_secs(),
            skip_preflight: false,
        }
    }
}

fn default_slippage_bps() -> u32 {
    500
}
fn default_priority_fee() -> u64 {
    200_000
}
fn default_cu_limit() -> u32 {
    200_000
}
fn default_max_retries() -> u32 {
    3
}
fn default_balance_refresh_secs() -> u64 {
    10
}

// --- Top-level ExecutionConfig ----------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct ExecutionConfig {
    #[serde(default)]
    pub mode: ExecutionMode,
    #[serde(default)]
    pub live: LiveExecutionConfig,
}

// --- Factory ----------------------------------------------------------------

/// Build the right `Broker` for the configured mode.
///
/// * `demo_start_balance_sol` — initial SOL balance for `MockBroker`
///   (matches the existing `start_balance_sol` yaml setting).
/// * In Live mode, expects `PRIVATE_KEY` and `SOLANA_HTTP` (overridable via
///   `SOLANA_RPC_URL`) env vars. Falls back to a clear error message if not
///   set so we never silently start a live bot without a wallet. If the RPC
///   returns only transient errors while fetching the initial balance (e.g.
///   HTTP 429), the broker starts with `demo_start_balance_sol` from yaml as a
///   placeholder until the background balance refresh succeeds.
pub async fn build_broker(
    cfg: &ExecutionConfig,
    demo_start_balance_sol: f64,
) -> Result<Arc<dyn Broker>, String> {
    match cfg.mode {
        ExecutionMode::Demo => {
            println!(
                "[EXEC] Mode=DEMO (MockBroker), start_balance={:.4} SOL",
                demo_start_balance_sol
            );
            Ok(Arc::new(MockBroker::new(demo_start_balance_sol)))
        }
        ExecutionMode::Live => {
            let private_key = std::env::var("PRIVATE_KEY")
                .map_err(|_| "PRIVATE_KEY env var must be set for live mode".to_string())?;
            let rpc_url = std::env::var("SOLANA_RPC_URL")
                .or_else(|_| std::env::var("SOLANA_HTTP"))
                .map_err(|_| {
                    "SOLANA_RPC_URL (or SOLANA_HTTP) env var must be set for live mode"
                        .to_string()
                })?;

            let keypair = Arc::new(Keypair::from_base58_string(&private_key));
            let pubkey = keypair.pubkey();
            let wallet_address: solana_address::Address = pubkey
                .to_string()
                .parse()
                .map_err(|e| format!("Failed to parse wallet pubkey: {e}"))?;

            println!(
                "[EXEC] Mode=LIVE (SolanaBroker), wallet={} slippage_bps={} \
                 priority_fee_uL={} cu_limit={} retries={} refresh={}s",
                wallet_address,
                cfg.live.slippage_bps,
                cfg.live.priority_fee_micro_lamports,
                cfg.live.compute_unit_limit,
                cfg.live.max_retries,
                cfg.live.balance_refresh_secs,
            );

            let broker = SolanaBroker::new(
                rpc_url,
                wallet_address,
                keypair,
                cfg.live.clone(),
                demo_start_balance_sol,
            )
            .await
            .map_err(|e| format!("SolanaBroker init failed: {e}"))?;
            Ok(Arc::new(broker))
        }
    }
}
