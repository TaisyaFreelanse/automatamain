use serde::{Deserialize, Serialize};

use crate::{
    autobuy::{
        execution::ExecutionConfig, filters::creator::CreatorStatisticsFilter,
        manager::SmartBuyConfig,
    },
    scoring::config::{PersistenceConfig, ScoringConfig, StrategyConfig},
};

#[derive(Serialize, Deserialize)]
pub struct Config {
    pub creator_config: CreatorStatisticsFilter,
    pub buy_config: SmartBuyConfig,
    pub ws_port: u16,
    pub http_port: u16,
    pub start_balance_sol: f64,

    /// New: A+/A/SKIP score engine knobs. Existing yaml files without this
    /// section keep working (defaults are applied).
    #[serde(default)]
    pub scoring: ScoringConfig,

    /// New: daily caps, loss-streak pause, regime pause.
    #[serde(default)]
    pub strategy: StrategyConfig,

    /// New: where to persist dev_ranker / smart_money json.
    #[serde(default)]
    pub persistence: PersistenceConfig,

    /// New: demo (MockBroker) vs live (SolanaBroker) execution + live knobs.
    /// Default is demo, so existing yaml files keep running in simulation.
    #[serde(default)]
    pub execution: ExecutionConfig,
}
