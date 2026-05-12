use async_trait::async_trait;
use solana_address::Address;
use thiserror::Error;

use crate::generalize::general_pool::Pool;

// ── Receipts ──────────────────────────────────────────────────────────────────

pub struct BuyReceipt {
    /// SOL actually spent (may differ from requested due to slippage/fees).
    pub sol_spent: f64,
    /// Token units received.
    pub tokens_received: f64,
}

pub struct SellReceipt {
    /// SOL received from the sale.
    pub sol_received: f64,
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
    async fn sell(
        &self,
        mint: Address,
        token_amount: f64,
        pool: &dyn Pool,
    ) -> Result<SellReceipt, BrokerError>;

    /// Current SOL balance.
    async fn balance_sol(&self) -> Result<f64, BrokerError>;
}
