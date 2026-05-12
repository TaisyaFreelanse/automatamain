use serde::Serialize;
use solana_address::Address;

use crate::{generalize::general_commands::Currency, persistence::error::Error};

#[derive(Clone, Debug, Serialize)]
pub struct CreatorStatistics {
    pub median_market_cap: Currency,
    pub trader_pnl_average: f64,
    pub total_holders_average: u64,
    pub average_volume: f64,
    pub median_total_trades: u64,
    pub average_unique_buy_to_sell_ratio: f64,
    pub average_buy_trader_size: Currency,
    pub total_coins: u64,
}

#[async_trait::async_trait]
pub trait CreatorRepository {
    async fn get_creator_stats_in_sol(
        &self,
        dev_address: Address,
    ) -> Result<Option<CreatorStatistics>, Error>;
}
