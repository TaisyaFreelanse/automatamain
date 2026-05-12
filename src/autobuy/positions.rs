use crate::{generalize::general_pool::Pool, helper::Amount};
use solana_address::Address;
use std::collections::HashMap;

pub struct Position {
    pub pool: Box<dyn Pool>,
    pub initial_holdings: Amount,
    pub holdings: Amount,
    pub enter_mcap: Amount,
    pub highest_mcap: f64,
    /// Unified profit floor — exit if pnl() drops at or below this value.
    /// Negative = loss threshold (e.g. -25.0), positive = locked-in profit floor (e.g. 5.0).
    pub exit_profit_floor: f64,
    pub tp1_triggered: bool,
    pub tp2_triggered: bool,
    pub trailing_active: bool,
    /// Blocks duplicate full-close signals while the sell tx is in-flight.
    pub is_closing: bool,
    /// Blocks a second partial sell from being queued before the first one executes.
    pub pending_partial_sell: bool,
    pub entry_time: u64,
    /// Cumulative SOL returned across all partial and final sells.
    pub total_returned: f64,
}

impl Position {
    pub fn new(pool: Box<dyn Pool>, buy_amount: Amount, current_time: u64) -> Self {
        let enter_mcap = pool.market_cap().amount();
        Self {
            highest_mcap: enter_mcap.to_float(),
            enter_mcap,
            pool,
            initial_holdings: buy_amount,
            holdings: buy_amount,
            exit_profit_floor: -16.0, // overwritten by SmartBuyConfig on open
            tp1_triggered: false,
            tp2_triggered: false,
            trailing_active: false,
            is_closing: false,
            pending_partial_sell: false,
            entry_time: current_time,
            total_returned: 0.0,
        }
    }

    pub fn pnl(&self) -> f64 {
        let enter_mcap = self.enter_mcap.to_float();
        if enter_mcap == 0.0 {
            return 0.0;
        }
        (self.pool.market_cap().amount().to_float() / enter_mcap - 1.0) * 100.0
    }
}
