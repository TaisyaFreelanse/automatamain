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

    /// Self-learning: trade/skip logging + conservative threshold tuning.
    #[serde(default)]
    pub learning: LearningConfig,
}

/// Knobs for the optional learning loop (see `crate::learning`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LearningConfig {
    /// Master switch: DB logging + background optimizer.
    #[serde(default = "learning_default_enabled")]
    pub enabled: bool,
    /// Run optimizer after this many **new** closed trades since the last run.
    #[serde(default = "learning_default_every")]
    pub analyze_every_trades: u64,
    /// Wake the loop at least this often (seconds); optimizer still respects `analyze_every_trades`.
    #[serde(default = "learning_default_interval")]
    pub analyze_interval_secs: u64,
    /// Minimum rows in `learning_trades` before heuristics may change the patch.
    #[serde(default = "learning_default_min_trades")]
    pub min_trades_for_update: i64,
}

impl Default for LearningConfig {
    fn default() -> Self {
        Self {
            enabled: learning_default_enabled(),
            analyze_every_trades: learning_default_every(),
            analyze_interval_secs: learning_default_interval(),
            min_trades_for_update: learning_default_min_trades(),
        }
    }
}

fn learning_default_enabled() -> bool {
    true
}
fn learning_default_every() -> u64 {
    100
}
fn learning_default_interval() -> u64 {
    3600
}
fn learning_default_min_trades() -> i64 {
    30
}
