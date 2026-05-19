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
    /// SOL received from the sale.
    pub sol_received: f64,
    /// On-chain transaction signature (None for demo / mock).
    pub signature: Option<String>,
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
    #[error("Custom : {0}")]
    Custom(String),
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
