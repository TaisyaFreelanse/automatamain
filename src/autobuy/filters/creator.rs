use serde::{Deserialize, Serialize};

use crate::persistence::creators::CreatorStatistics;

fn default_spam_skip_coins() -> Option<u64> {
    Some(100)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatorStatisticsFilter {
    pub min_total_coins: Option<u64>,
    pub max_total_coins: Option<u64>,

    /// Early spam-dev cutoff: if a dev has launched more than this many coins,
    /// skip the token *before* running the expensive creator-stats aggregation
    /// (which would otherwise scan hundreds of thousands of trade rows). Such
    /// prolific devs are almost always spam/serial ruggers. Checked with a cheap
    /// capped `count` so cost is independent of how many coins the dev has.
    /// `None` disables the early gate. Defaults to 100.
    #[serde(default = "default_spam_skip_coins")]
    pub spam_skip_coins: Option<u64>,

    pub min_median_market_cap: Option<f64>,
    pub min_trader_pnl_average: Option<f64>,
    pub min_total_holders_average: Option<f64>,
    pub min_average_volume: Option<f64>,
    pub min_median_total_trades: Option<f64>,
    pub min_average_unique_buy_to_sell_ratio: Option<f64>,
    pub min_average_buy_trader_size: Option<f64>,
}

impl CreatorStatisticsFilter {
    pub fn filter(&self, s: &CreatorStatistics) -> bool {
        if let Some(min) = self.min_total_coins
            && s.total_coins < min {
                return false;
            }

        if let Some(max) = self.max_total_coins
            && s.total_coins > max {
                return false;
            }

        if let Some(min) = self.min_median_market_cap
            && s.median_market_cap.amount().to_float() < min {
                return false;
            }

        if let Some(min) = self.min_trader_pnl_average
            && s.trader_pnl_average < min {
                return false;
            }

        if let Some(min) = self.min_total_holders_average
            && (s.total_holders_average as f64) < min {
                return false;
            }

        if let Some(min) = self.min_average_volume
            && s.average_volume < min {
                return false;
            }

        if let Some(min) = self.min_median_total_trades
            && (s.median_total_trades as f64) < min {
                return false;
            }

        if let Some(min) = self.min_average_unique_buy_to_sell_ratio
            && s.average_unique_buy_to_sell_ratio < min {
                return false;
            }

        if let Some(min) = self.min_average_buy_trader_size
            && s.average_buy_trader_size.amount().to_float() < min {
                return false;
            }

        true
    }
}
