use std::{
    collections::HashMap,
    sync::Mutex,
};

use async_trait::async_trait;
use solana_address::Address;

use crate::generalize::general_pool::Pool;

use super::broker::{Broker, BrokerError, BuyReceipt, SellReceipt};

// ── State per open position ───────────────────────────────────────────────────

struct MockPosition {
    tokens: f64,
    entry_mcap: f64,
}

// ── Mock broker ───────────────────────────────────────────────────────────────

pub struct MockBroker {
    balance: Mutex<f64>,
    positions: Mutex<HashMap<Address, MockPosition>>,
}

impl MockBroker {
    pub fn new(initial_balance_sol: f64) -> Self {
        Self {
            balance: Mutex::new(initial_balance_sol),
            positions: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Broker for MockBroker {
    async fn buy(
        &self,
        mint: Address,
        amount_sol: f64,
        pool: &dyn Pool,
    ) -> Result<BuyReceipt, BrokerError> {
        let mut bal = self.balance.lock().unwrap();
        if *bal < amount_sol {
            return Err(BrokerError::InsufficientBalance {
                have: *bal,
                need: amount_sol,
            });
        }
        *bal -= amount_sol;

        let entry_mcap = pool.market_cap().amount().to_float();
        // Token units = SOL spent (1:1 in mock — cancels in PnL formula)
        let tokens_received = amount_sol;

        self.positions.lock().unwrap().insert(
            mint,
            MockPosition { tokens: tokens_received, entry_mcap },
        );

        Ok(BuyReceipt { sol_spent: amount_sol, tokens_received, signature: None })
    }

    async fn sell(
        &self,
        mint: Address,
        token_amount: f64,
        pool: &dyn Pool,
    ) -> Result<SellReceipt, BrokerError> {
        let current_mcap = pool.market_cap().amount().to_float();

        let sol_received = {
            let mut positions = self.positions.lock().unwrap();
            let pos = positions
                .get_mut(&mint)
                .ok_or(BrokerError::PositionNotFound(mint))?;

            let price_ratio = if pos.entry_mcap > 0.0 {
                current_mcap / pos.entry_mcap
            } else {
                1.0
            };

            let sol = token_amount * price_ratio;

            pos.tokens -= token_amount;
            if pos.tokens <= 0.0 {
                positions.remove(&mint);
            }

            sol
        };

        *self.balance.lock().unwrap() += sol_received;

        Ok(SellReceipt { sol_received, signature: None })
    }

    async fn balance_sol(&self) -> Result<f64, BrokerError> {
        Ok(*self.balance.lock().unwrap())
    }

    fn mode_label(&self) -> &'static str {
        "demo"
    }
}
