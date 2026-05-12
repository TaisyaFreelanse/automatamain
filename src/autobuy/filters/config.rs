use serde::{Deserialize, Serialize};

use crate::autobuy::{filters::creator::CreatorStatisticsFilter, manager::SmartBuyConfig};

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub creator_config: CreatorStatisticsFilter,
    pub buy_config: SmartBuyConfig,
    pub ws_port: u16,
    pub http_port: u16,
    pub start_balance_sol: f64,
}
