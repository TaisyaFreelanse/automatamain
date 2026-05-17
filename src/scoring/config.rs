use serde::{Deserialize, Serialize};

/// Minimum tier that may proceed to `InitiateBuy` when `execution.mode` is
/// `live` and this gate is evaluated. Demo mode ignores this (same as legacy).
#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "PascalCase")]
pub enum MinBuyTier {
    #[default]
    A,
    APlus,
}

// --- ScoringConfig -----------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScoringConfig {
    /// How long to wait after a Create event before snapshotting features
    /// for the score engine. Lets the swarm accumulate early buyers and
    /// gives the bonding curve a price tick or two.
    #[serde(default = "default_window_ms")]
    pub scoring_window_ms: u64,

    #[serde(default = "default_a_plus")]
    pub a_plus_threshold: i32,

    #[serde(default = "default_a")]
    pub a_threshold: i32,

    /// When `true` and `execution.mode` is **live**, skip `InitiateBuy` unless
    /// the score breakdown includes `momentum_good` (mcap grew into the
    /// configured band during the scoring window).
    #[serde(default)]
    pub require_momentum_good: bool,

    /// When `execution.mode` is **live**, only this tier or higher may open a
    /// position. `A` = both A and A+ (after other gates); `APlus` = stricter,
    /// top-tier only. Pair with `require_momentum_good` so A entries still
    /// need confirmed mcap momentum.
    #[serde(default)]
    pub minimum_tier_for_buy: MinBuyTier,

    /// When `true`, use the pre–entry-filter-V2 score path: overheated-before-good
    /// momentum ordering, bundle penalty = `bundle_identical` else `bundle_similar`,
    /// smart-wallet buckets fixed at 3+/1+, and **YAML `thresholds` only** (learning
    /// merge is not applied to snapshot or score). Flip back to `false` for V2.
    #[serde(default)]
    pub legacy_scoring: bool,

    #[serde(default)]
    pub weights: ScoringWeights,

    #[serde(default)]
    pub thresholds: FeatureThresholds,

    #[serde(default)]
    pub size: TierSize,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            scoring_window_ms: default_window_ms(),
            a_plus_threshold: default_a_plus(),
            a_threshold: default_a(),
            require_momentum_good: false,
            minimum_tier_for_buy: MinBuyTier::A,
            legacy_scoring: false,
            weights: ScoringWeights::default(),
            thresholds: FeatureThresholds::default(),
            size: TierSize::default(),
        }
    }
}

fn default_window_ms() -> u64 {
    1500
}
fn default_a_plus() -> i32 {
    9
}
fn default_a() -> i32 {
    7
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScoringWeights {
    pub dev_history_strong: i32,
    pub dev_history_weak: i32,
    pub dev_ranker_a_plus: i32,
    pub dev_ranker_a: i32,
    pub dev_ranker_bad: i32,
    pub smart_wallets_3plus: i32,
    pub smart_wallets_1plus: i32,
    pub buyers_10plus: i32,
    pub buyers_6plus: i32,
    pub buyers_below_3: i32,
    pub buy_to_sell_ratio_high: i32,
    pub momentum_good: i32,
    pub momentum_overheated: i32,
    pub volume_ok: i32,
    pub bundle_similar: i32,
    pub bundle_identical: i32,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            dev_history_strong: 4,
            dev_history_weak: 2,
            dev_ranker_a_plus: 3,
            dev_ranker_a: 1,
            dev_ranker_bad: -3,
            smart_wallets_3plus: 3,
            smart_wallets_1plus: 1,
            buyers_10plus: 2,
            buyers_6plus: 1,
            buyers_below_3: -2,
            buy_to_sell_ratio_high: 1,
            momentum_good: 2,
            momentum_overheated: -3,
            volume_ok: 1,
            bundle_similar: -4,
            bundle_identical: -5,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FeatureThresholds {
    /// "Strong" dev history: at least this many past coins AND avg trader
    /// pnl above this percentage. Both conditions required.
    pub dev_strong_min_coins: u64,
    pub dev_strong_min_pnl_pct: f64,
    pub buyers_high: u64,
    pub buyers_mid: u64,
    pub buyers_low: u64,
    pub buy_to_sell_high: f64,
    /// Minimum % mcap growth (vs start of scoring window) for `momentum_good`.
    pub momentum_good_low_pct: f64,
    pub momentum_good_high_pct: f64,
    pub momentum_overheated_pct: f64,
    pub volume_ok_sol: f64,
    /// share of buys whose size is within ±tolerance of the median
    pub bundle_similar_ratio: f64,
    /// Share of buys with exactly equal raw size (logged / analytics; V2 score
    /// engine uses `similar_size_ratio` only for the bundle penalty).
    pub bundle_identical_ratio: f64,
    /// "similar" tolerance, in fraction of median (e.g. 0.05 = ±5%)
    pub bundle_similar_tolerance: f64,
    /// Minimum smart-wallet count for the `smart_wallets_3plus` score bucket.
    #[serde(default = "default_smart_wallet_3plus_min")]
    pub smart_wallet_3plus_min: u32,
    /// Minimum count for the `smart_wallets_1plus` bucket (must stay `< smart_wallet_3plus_min`).
    #[serde(default = "default_smart_wallet_1plus_min")]
    pub smart_wallet_1plus_min: u32,
}

impl Default for FeatureThresholds {
    fn default() -> Self {
        Self {
            dev_strong_min_coins: 5,
            dev_strong_min_pnl_pct: 20.0,
            buyers_high: 10,
            buyers_mid: 6,
            buyers_low: 3,
            buy_to_sell_high: 1.5,
            momentum_good_low_pct: 12.0,
            momentum_good_high_pct: 30.0,
            momentum_overheated_pct: 60.0,
            volume_ok_sol: 10.0,
            bundle_similar_ratio: 0.7,
            bundle_identical_ratio: 0.5,
            // Wider default so near-identical bundle sizes count toward `similar_size_ratio`.
            bundle_similar_tolerance: 0.10,
            smart_wallet_3plus_min: default_smart_wallet_3plus_min(),
            smart_wallet_1plus_min: default_smart_wallet_1plus_min(),
        }
    }
}

fn default_smart_wallet_3plus_min() -> u32 {
    3
}
fn default_smart_wallet_1plus_min() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TierSize {
    pub a_plus_sol: f64,
    pub a_sol: f64,
}

impl Default for TierSize {
    fn default() -> Self {
        Self {
            a_plus_sol: 0.4,
            a_sol: 0.3,
        }
    }
}

// --- StrategyConfig ----------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StrategyConfig {
    #[serde(default = "default_max_open")]
    pub max_open_positions: u32,
    #[serde(default = "default_max_trades_per_day")]
    pub max_trades_per_day: u32,
    /// Negative number, e.g. -2.0
    #[serde(default = "default_max_daily_loss")]
    pub max_daily_loss_sol: f64,
    /// Stop opening new positions after locking in this much profit
    #[serde(default = "default_daily_profit_lock")]
    pub daily_profit_lock_sol: f64,
    #[serde(default = "default_loss_streak")]
    pub loss_streak_limit: u32,
    #[serde(default = "default_loss_streak_pause")]
    pub loss_streak_pause_secs: u64,
    #[serde(default = "default_regime_pause")]
    pub market_regime_pause: bool,
    #[serde(default = "default_regime_window")]
    pub market_regime_window: u32,
    #[serde(default = "default_regime_loss_ratio")]
    pub market_regime_loss_ratio: f64,
    #[serde(default = "default_regime_pause_secs")]
    pub market_regime_pause_secs: u64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            max_open_positions: default_max_open(),
            max_trades_per_day: default_max_trades_per_day(),
            max_daily_loss_sol: default_max_daily_loss(),
            daily_profit_lock_sol: default_daily_profit_lock(),
            loss_streak_limit: default_loss_streak(),
            loss_streak_pause_secs: default_loss_streak_pause(),
            market_regime_pause: default_regime_pause(),
            market_regime_window: default_regime_window(),
            market_regime_loss_ratio: default_regime_loss_ratio(),
            market_regime_pause_secs: default_regime_pause_secs(),
        }
    }
}

fn default_max_open() -> u32 {
    3
}
fn default_max_trades_per_day() -> u32 {
    25
}
fn default_max_daily_loss() -> f64 {
    -2.0
}
fn default_daily_profit_lock() -> f64 {
    4.0
}
fn default_loss_streak() -> u32 {
    5
}
fn default_loss_streak_pause() -> u64 {
    1800
}
fn default_regime_pause() -> bool {
    true
}
fn default_regime_window() -> u32 {
    10
}
fn default_regime_loss_ratio() -> f64 {
    0.8
}
fn default_regime_pause_secs() -> u64 {
    1800
}

// --- PersistenceConfig -------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PersistenceConfig {
    #[serde(default = "default_dev_path")]
    pub dev_ranker_path: String,
    #[serde(default = "default_smart_path")]
    pub smart_money_path: String,
    /// JSON file merged into `scoring.thresholds` at runtime (learning engine).
    #[serde(default = "default_learning_overrides_path")]
    pub learning_overrides_path: String,
    #[serde(default = "default_flush")]
    pub flush_every_secs: u64,
    /// Drop wallets/devs whose last activity is older than this many seconds.
    /// 0 = never expire.
    #[serde(default = "default_ttl")]
    pub entity_ttl_secs: u64,
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            dev_ranker_path: default_dev_path(),
            smart_money_path: default_smart_path(),
            learning_overrides_path: default_learning_overrides_path(),
            flush_every_secs: default_flush(),
            entity_ttl_secs: default_ttl(),
        }
    }
}

fn default_learning_overrides_path() -> String {
    "./state/learning_overrides.json".into()
}

fn default_dev_path() -> String {
    "/home/automata/state/dev_ranker.json".into()
}
fn default_smart_path() -> String {
    "/home/automata/state/smart_money.json".into()
}
fn default_flush() -> u64 {
    30
}
fn default_ttl() -> u64 {
    48 * 3600
}
