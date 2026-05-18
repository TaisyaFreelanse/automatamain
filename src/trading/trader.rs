use core::fmt;
use std::fmt::Display;

use crate::{helper::Amount, trading::offer::Offer};

#[derive(Debug, Clone)]
pub struct Trader {
    current_holdings: Amount,
    avg_cost_basis: u128,
    total_spent: Amount,
    realized_pnl: i64,
    /// Cumulative quote (lamports) received from sells — for early sell-pressure tape.
    sell_proceeds_raw: u64,
    sell_event_count: u32,

    trader_type: TraderType,
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum TraderType {
    Creator,
    Sniper,
    Regular,
}

impl Display for TraderType {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TraderType::Creator => "Creator",
            TraderType::Sniper => "Sniper",
            TraderType::Regular => "Regular",
        };
        write!(f, "{}", s)
    }
}

impl Trader {
    pub fn empty(trader_type: TraderType, decimals: u8, paired_decimals: u8) -> Self {
        Self {
            current_holdings: Amount::from_raw(0, decimals),
            avg_cost_basis: 0,
            total_spent: Amount::from_raw(0, paired_decimals),
            realized_pnl: 0,
            sell_proceeds_raw: 0,
            sell_event_count: 0,

            trader_type,
        }
    }

    pub fn apply(&mut self, offer: Offer) {
        let scale = 10u128.pow(self.current_holdings.decimals() as u32);
        match offer {
            Offer::Buy { buy_for, received } => {
                let new_holdings = self.current_holdings + received;
                if new_holdings.raw() == 0 {
                    return;
                }
                self.avg_cost_basis = (self.avg_cost_basis * self.current_holdings.raw() as u128
                    + buy_for as u128 * scale * scale)
                    / new_holdings.raw() as u128;
                self.current_holdings = new_holdings;
                self.total_spent =
                    self.total_spent + Amount::from_raw(buy_for, self.total_spent.decimals());
            }
            Offer::Sell {
                sell_amount,
                received,
            } => {
                let cost_of_sold =
                    (self.avg_cost_basis * sell_amount.raw() as u128 / scale / scale) as i64;
                self.realized_pnl += received as i64 - cost_of_sold;
                self.current_holdings = self.current_holdings - sell_amount;
                self.sell_proceeds_raw = self.sell_proceeds_raw.saturating_add(received);
                self.sell_event_count = self.sell_event_count.saturating_add(1);
            }
        }
    }

    pub fn pnl(&self) -> i64 {
        self.realized_pnl
    }

    pub fn pnl_percent(&self) -> f64 {
        if self.total_spent.raw() == 0 {
            return 0.0;
        }
        self.realized_pnl as f64 / self.total_spent.raw() as f64 * 100.0
    }

    pub fn holdings(&self) -> Amount {
        self.current_holdings
    }

    pub fn avg_cost_basis(&self) -> u128 {
        self.avg_cost_basis
    }

    pub fn total_spent(&self) -> Amount {
        self.total_spent
    }

    pub fn trader_type(&self) -> TraderType {
        self.trader_type
    }

    pub fn sell_proceeds_raw(&self) -> u64 {
        self.sell_proceeds_raw
    }

    pub fn sell_event_count(&self) -> u32 {
        self.sell_event_count
    }
}
