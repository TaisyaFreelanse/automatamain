use async_trait::async_trait;
use solana_address::Address;
use thiserror::Error;

use crate::generalize::{general_commands::TradeAction, general_pool::Pool};

// ── Receipts ──────────────────────────────────────────────────────────────────

pub struct BuyReceipt {
    /// SOL actually spent (may differ from requested due to slippage/fees).
    pub sol_spent: f64,
    /// Token units received.
    pub tokens_received: f64,
    /// On-chain transaction signature (None for demo / mock).
    pub signature: Option<String>,
    /// For pump bonding buys: implied mcap in SOL from on-chain virtual
    /// reserves **after** the confirmed fill. The manager should prefer this
    /// over `pool.market_cap()` from the WS cache, which can lag the fill or
    /// already reflect later trades — otherwise TP/% and dashboards misread
    /// the true entry vs fast 1s candles.
    pub entry_mcap_fill_sol: Option<f64>,
}

pub struct SellReceipt {
    /// Net SOL change to the wallet from confirmed on-chain tx meta
    /// (`post_balances - pre_balances` for the wallet account), in SOL.
    /// Includes fees, rent refunds from `CloseAccount`, etc.
    pub sol_received_actual: f64,
    /// Bonding-curve / Jupiter estimate (e.g. quote × slippage discount)
    /// before execution — for comparison with [`Self::sol_received_actual`].
    pub sol_received_estimated: f64,
    /// Primary signature for this logical sell (last leg if multi-tx drain).
    pub signature: Option<String>,
}

impl SellReceipt {
    /// Demo / mock broker: model and “on-chain” are the same number.
    #[must_use]
    pub fn mock(sol: f64, signature: Option<String>) -> Self {
        Self {
            sol_received_actual: sol,
            sol_received_estimated: sol,
            signature,
        }
    }
}

// ── Error ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Error)]
pub enum BrokerError {
    #[error("Insufficient balance: have {have:.4} SOL, need {need:.4} SOL")]
    InsufficientBalance { have: f64, need: f64 },
    #[error("No open position for mint {0}")]
    PositionNotFound(Address),
    #[error("Transaction failed: {0}")]
    TransactionFailed(String),
    /// Jupiter could not quote/swap (even after chunked attempts). Position must be
    /// closed manually; manager clears `is_closing` and surfaces a dashboard alert.
    #[error("Jupiter sell exhausted (manual exit required): {0}")]
    JupiterSellExhausted(String),
    /// Mint account never appeared on RPC (stale feed / dead mint / indexer lag).
    #[error("Mint not on-chain: {mint} ({detail})")]
    MintNotOnChain { mint: String, detail: String },
    #[error("Custom : {0}")]
    Custom(String),
}

impl BrokerError {
    /// Full exit failed on Jupiter routing; operator should sell the ATA manually.
    pub fn requires_manual_sell(&self) -> bool {
        matches!(self, BrokerError::JupiterSellExhausted(_))
    }

    /// Pre-buy wait exhausted or mint account missing — not a failed trade attempt.
    pub fn is_mint_not_on_chain(&self) -> bool {
        matches!(self, BrokerError::MintNotOnChain { .. })
    }

    /// Post-graduation BUY found no Jupiter route (excluded-Pump and allow-Pump
    /// both `NO_ROUTES_FOUND`, or the route hit bonding-curve 6005). Treated as a
    /// skip (`post_grad_no_route`), not a failed on-chain trade.
    pub fn is_post_grad_no_route(&self) -> bool {
        matches!(self, BrokerError::Custom(msg) if msg.contains("post_grad_no_route"))
    }
}

// ── Trait ─────────────────────────────────────────────────────────────────────

#[async_trait]
pub trait Broker: Send + Sync {
    /// Open a position: spend `amount_sol` SOL, receive tokens.
    async fn buy(
        &self,
        mint: Address,
        amount_sol: f64,
        pool: &dyn Pool,
    ) -> Result<BuyReceipt, BrokerError>;

    /// Close or reduce a position: sell `token_amount` tokens, receive SOL.
    ///
    /// If `close_account_after == true`, the broker treats this as a full
    /// exit: the on-chain SELL is sized to the *current* ATA balance (not
    /// `token_amount`, which may be stale after rounding) and a
    /// `CloseAccount` instruction is appended to the same transaction so the
    /// ATA's rent-exempt SOL (~0.00203928 SOL per token account) is refunded
    /// to the wallet atomically. Without this, every new position
    /// permanently locks rent and the wallet bleeds SOL even on a flat P&L.
    async fn sell(
        &self,
        mint: Address,
        token_amount: f64,
        pool: &dyn Pool,
        close_account_after: bool,
    ) -> Result<SellReceipt, BrokerError>;

    /// Current SOL balance (locally cached value — call
    /// [`refresh_onchain_balance`] to pull a fresh value from the chain).
    async fn balance_sol(&self) -> Result<f64, BrokerError>;

    /// Re-read the wallet balance from the RPC. No-op for in-memory brokers.
    /// Live brokers override this and use it for periodic reconciliation.
    async fn refresh_onchain_balance(&self) -> Result<(), BrokerError> {
        Ok(())
    }

    /// Called for every trade observed on the live feed. Live brokers use this
    /// to update tracked token holdings / cached SOL balance from the
    /// authoritative on-chain events. Demo/mock brokers ignore it.
    fn on_trade(&self, _trade: &TradeAction, _pool: &dyn Pool) {}

    /// Drop any broker-local tracking for `mint` (e.g. after the operator
    /// abandons a stuck UI position). Default: no-op.
    fn forget_position(&self, _mint: Address) {}

    /// Human-readable broker label for logs and the `/status` endpoint.
    fn mode_label(&self) -> &'static str {
        "unknown"
    }
}
