use serde::{Deserialize, Serialize};

use crate::{
    autobuy::{
        execution::ExecutionConfig, filters::creator::CreatorStatisticsFilter,
        manager::SmartBuyConfig,
    },
    scoring::config::{PersistenceConfig, ScoringConfig, StrategyConfig},
};

/// In-memory mint cooldown after mcap/SOL divergence or false SL CRASH on WS glitch.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CurveQuarantineConfig {
    #[serde(default = "curve_quarantine_default_enabled")]
    pub enabled: bool,
    #[serde(default = "curve_quarantine_default_cooldown_secs")]
    pub cooldown_secs: u64,
    /// Quarantine when `filt_pnl - pnl_sol` >= this (e.g. mcap +23%, SOL -21%).
    #[serde(default = "curve_quarantine_default_min_divergence")]
    pub min_mcap_sol_divergence_pct: f64,
    /// Only when realized SOL PnL is at or below this.
    #[serde(default = "curve_quarantine_default_max_pnl_sol")]
    pub max_pnl_sol_pct: f64,
    /// SL CRASH with filt above this and raw at/below max_raw → quarantine.
    #[serde(default = "curve_quarantine_default_sl_crash_filt")]
    pub sl_crash_false_positive_min_filt_pnl: f64,
    #[serde(default = "curve_quarantine_default_sl_crash_raw")]
    pub sl_crash_false_positive_max_raw_pnl: f64,
}

impl Default for CurveQuarantineConfig {
    fn default() -> Self {
        Self {
            enabled: curve_quarantine_default_enabled(),
            cooldown_secs: curve_quarantine_default_cooldown_secs(),
            min_mcap_sol_divergence_pct: curve_quarantine_default_min_divergence(),
            max_pnl_sol_pct: curve_quarantine_default_max_pnl_sol(),
            sl_crash_false_positive_min_filt_pnl: curve_quarantine_default_sl_crash_filt(),
            sl_crash_false_positive_max_raw_pnl: curve_quarantine_default_sl_crash_raw(),
        }
    }
}

fn curve_quarantine_default_enabled() -> bool {
    true
}
fn curve_quarantine_default_cooldown_secs() -> u64 {
    24 * 3600
}
fn curve_quarantine_default_min_divergence() -> f64 {
    35.0
}
fn curve_quarantine_default_max_pnl_sol() -> f64 {
    -10.0
}
fn curve_quarantine_default_sl_crash_filt() -> f64 {
    10.0
}
fn curve_quarantine_default_sl_crash_raw() -> f64 {
    5.0
}

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

    /// Block devs after our bot cliff/rug exits (DB-backed cooldown).
    #[serde(default)]
    pub dev_blacklist: DevBlacklistConfig,

    /// Block re-buy on mints that showed bonding WS vs SOL PnL divergence.
    #[serde(default)]
    pub curve_quarantine: CurveQuarantineConfig,
}

/// Cooldown on dev wallet after bot SL CRASH / deep SL on our trades.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DevBlacklistConfig {
    #[serde(default = "dev_blacklist_default_enabled")]
    pub enabled: bool,
    /// How long new mints from this dev are skipped (`expires_at = closed_at + cooldown`).
    #[serde(default = "dev_blacklist_default_cooldown_secs")]
    pub cooldown_secs: i64,
    /// Plain `SL` (non-crash) blacklists when realized PnL % (SOL) is at or below this.
    #[serde(default = "dev_blacklist_default_min_pnl_pct")]
    pub min_pnl_pct_for_sl: f64,
    /// Plain `SL` also blacklists when `tick_drop=` in close reason is at or above this %.
    #[serde(default = "dev_blacklist_default_min_tick_drop")]
    pub min_tick_drop_pct: f64,
    /// Rug spike: `tick_drop=` at or above → permanent ban (not cooldown).
    #[serde(default = "dev_blacklist_default_permanent_min_tick_drop")]
    pub permanent_min_tick_drop_pct: f64,
    /// Rug spike: `filt_mcap` vs `raw_mcap` drop % in close reason → permanent ban.
    #[serde(default = "dev_blacklist_default_permanent_min_mcap_drop")]
    pub permanent_min_mcap_drop_pct: f64,
}

impl Default for DevBlacklistConfig {
    fn default() -> Self {
        Self {
            enabled: dev_blacklist_default_enabled(),
            cooldown_secs: dev_blacklist_default_cooldown_secs(),
            min_pnl_pct_for_sl: dev_blacklist_default_min_pnl_pct(),
            min_tick_drop_pct: dev_blacklist_default_min_tick_drop(),
            permanent_min_tick_drop_pct: dev_blacklist_default_permanent_min_tick_drop(),
            permanent_min_mcap_drop_pct: dev_blacklist_default_permanent_min_mcap_drop(),
        }
    }
}

fn dev_blacklist_default_enabled() -> bool {
    true
}
fn dev_blacklist_default_cooldown_secs() -> i64 {
    7 * 24 * 3600
}
fn dev_blacklist_default_min_pnl_pct() -> f64 {
    -30.0
}
fn dev_blacklist_default_min_tick_drop() -> f64 {
    40.0
}
fn dev_blacklist_default_permanent_min_tick_drop() -> f64 {
    55.0
}
fn dev_blacklist_default_permanent_min_mcap_drop() -> f64 {
    60.0
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
