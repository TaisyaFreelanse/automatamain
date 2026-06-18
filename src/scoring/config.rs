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

    /// Number of equal sub-samples inside `scoring_window_ms` for buyer-velocity
    /// and sell-pressure tape. `1` = legacy single wait (no mid-window slices).
    #[serde(default = "default_buyer_velocity_slices")]
    pub buyer_velocity_slices: usize,

    #[serde(default = "default_a_plus")]
    pub a_plus_threshold: i32,

    #[serde(default = "default_a")]
    pub a_threshold: i32,

    /// When `true` and `execution.mode` is **live**, skip `InitiateBuy` unless
    /// the score breakdown includes `momentum_good` (mcap grew into the
    /// configured band during the scoring window).
    #[serde(default)]
    pub require_momentum_good: bool,

    /// Smart-wallet count at/above which the `require_momentum_good` live gate is
    /// bypassed: strong smart money is itself a momentum signal, so such tokens
    /// should reach the continuation layer instead of being cut early for a
    /// missing `momentum_good` item. `0` disables the bypass (gate applies to
    /// all). Only relevant when `require_momentum_good` is `true`.
    #[serde(default = "default_momentum_good_smart_bypass")]
    pub momentum_good_smart_bypass: u32,

    /// Tier-A+ specific bypass for the `require_momentum_good` live gate. A top
    /// tier (A+) token with at least this many smart wallets is allowed past the
    /// momentum gate even without a `momentum_good` item, so strong A+ smart
    /// setups reach the continuation/parabolic layer instead of being cut early.
    /// This is stricter-scoped than `momentum_good_smart_bypass` (which applies
    /// to all tiers): it only loosens the gate for confirmed A+ smart entries.
    /// `0` disables. Only relevant when `require_momentum_good` is `true`.
    #[serde(default = "default_momentum_good_aplus_smart_bypass")]
    pub momentum_good_aplus_smart_bypass: u32,

    /// Tier-A bypass for `require_momentum_good`: strong tape (buyers, volume,
    /// absorb, buyer-velocity persistence) without requiring smart wallets.
    /// Weak A (score 6–7, thin tape) is intentionally excluded via `min_score`.
    #[serde(default)]
    pub momentum_good_strong_a: MomentumGoodStrongABypassConfig,

    /// When `execution.mode` is **live**, only this tier or higher may open a
    /// position. `A` = both A and A+ (after other gates); `APlus` = stricter,
    /// top-tier only. Pair with `require_momentum_good` so A entries still
    /// need confirmed mcap momentum.
    #[serde(default)]
    pub minimum_tier_for_buy: MinBuyTier,

    /// Score adjustment applied when the dev is a prolific serial launcher
    /// (> `creator_config.spam_skip_coins`). We no longer hard-skip such devs:
    /// we skip only the expensive creator-stats SQL and let the token compete
    /// on tape strength, but with this penalty so only an exceptional tape
    /// survives. Negative = penalty (recommended). `0` disables.
    #[serde(default = "default_spam_dev_penalty")]
    pub spam_dev_penalty: i32,

    /// When `true` (live), a spam-dev token may only open a position at **A+**
    /// tier — its tape must be exceptional, not just A. Pairs with
    /// `spam_dev_penalty` to keep rare strong runners from prolific devs while
    /// dropping the rest. Ignored in demo mode.
    #[serde(default = "default_spam_dev_require_a_plus")]
    pub spam_dev_require_a_plus: bool,

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

    /// Pre-buy anti-rug: low sell-side / fake pump gates and scoring tightenings.
    #[serde(default)]
    pub anti_rug: AntiRugConfig,

    /// Continuation validation layer: short post-score confirmation poll that
    /// aborts entries on broken continuation (fake / transient momentum).
    #[serde(default)]
    pub continuation: ContinuationConfig,

    /// Anti-parabolic entry gate: skip weak A-tier entries bought at the local
    /// peak with no smart money and no strong continuation (bought-the-top).
    #[serde(default)]
    pub anti_parabolic: AntiParabolicConfig,

    /// Live gate: block or defer weak tier-A (low score, no smart, neutral/weak dev).
    #[serde(default)]
    pub weak_a_gate: WeakATierGateConfig,

    /// Fresh-dev experimental lane (tier B): stats collection for devs with no
    /// creator history / launch_count=0. Does not relax continuation or anti-rug.
    #[serde(default)]
    pub tier_b: TierBGateConfig,

    /// Tier A / A+: bypass `require_momentum_good` when momentum is the only fail
    /// (`momentum_overheated` without `momentum_good`). Continuation / anti-rug unchanged.
    #[serde(default)]
    pub aa_momentum_override: AAMomentumOverrideConfig,
}

impl Default for ScoringConfig {
    fn default() -> Self {
        Self {
            scoring_window_ms: default_window_ms(),
            buyer_velocity_slices: default_buyer_velocity_slices(),
            a_plus_threshold: default_a_plus(),
            a_threshold: default_a(),
            require_momentum_good: true,
            momentum_good_smart_bypass: default_momentum_good_smart_bypass(),
            momentum_good_aplus_smart_bypass: default_momentum_good_aplus_smart_bypass(),
            momentum_good_strong_a: MomentumGoodStrongABypassConfig::default(),
            minimum_tier_for_buy: MinBuyTier::A,
            spam_dev_penalty: default_spam_dev_penalty(),
            spam_dev_require_a_plus: default_spam_dev_require_a_plus(),
            legacy_scoring: false,
            weights: ScoringWeights::default(),
            thresholds: FeatureThresholds::default(),
            size: TierSize::default(),
            anti_rug: AntiRugConfig::default(),
            continuation: ContinuationConfig::default(),
            anti_parabolic: AntiParabolicConfig::default(),
            weak_a_gate: WeakATierGateConfig::default(),
            tier_b: TierBGateConfig::default(),
            aa_momentum_override: AAMomentumOverrideConfig::default(),
        }
    }
}

/// Tier A / A+: let overheated momentum continue through the live pipeline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AAMomentumOverrideConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for AAMomentumOverrideConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

/// Tier B — Fresh Dev Setup (MVP stats lane).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TierBGateConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default = "default_tier_b_min_smart_wallets")]
    pub min_smart_wallets: u32,
    #[serde(default = "default_tier_b_min_buyers")]
    pub min_buyers: u64,
    #[serde(default = "default_tier_b_min_buy_volume_sol")]
    pub min_buy_volume_sol: f64,
    /// Minimum peak mcap % vs scoring-window start (`momentum_peak_pct`).
    #[serde(default = "default_tier_b_min_velocity_pct")]
    pub min_velocity_pct: f64,
    /// Deferred re-evaluation for fresh devs rejected only on immature early stats.
    #[serde(default)]
    pub fresh_watchlist: FreshWatchlistConfig,
    /// Strong fresh impulse bypass when B fails only on momentum (overheated band).
    #[serde(default)]
    pub hot_fresh_override: HotFreshOverrideConfig,
}

/// Poll loop for fresh devs that fail creator_config on early-stats only.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FreshWatchlistConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_fresh_watchlist_poll_ms")]
    pub poll_interval_ms: u64,
    #[serde(default = "default_fresh_watchlist_max_ms")]
    pub max_wait_ms: u64,
    #[serde(default = "default_fresh_watchlist_max_concurrent")]
    pub max_concurrent: usize,
}

impl Default for FreshWatchlistConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            poll_interval_ms: default_fresh_watchlist_poll_ms(),
            max_wait_ms: default_fresh_watchlist_max_ms(),
            max_concurrent: default_fresh_watchlist_max_concurrent(),
        }
    }
}

fn default_fresh_watchlist_poll_ms() -> u64 {
    5000
}
fn default_fresh_watchlist_max_ms() -> u64 {
    30000
}
fn default_fresh_watchlist_max_concurrent() -> usize {
    50
}

/// Tier B only: allow overheated fresh runners with exceptional tape into full pipeline.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HotFreshOverrideConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_hot_fresh_min_buyers")]
    pub min_buyers: u64,
    #[serde(default = "default_hot_fresh_min_buy_volume_sol")]
    pub min_buy_volume_sol: f64,
    #[serde(default = "default_hot_fresh_min_velocity_pct")]
    pub min_velocity_pct: f64,
}

impl Default for HotFreshOverrideConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_buyers: default_hot_fresh_min_buyers(),
            min_buy_volume_sol: default_hot_fresh_min_buy_volume_sol(),
            min_velocity_pct: default_hot_fresh_min_velocity_pct(),
        }
    }
}

fn default_hot_fresh_min_buyers() -> u64 {
    25
}
fn default_hot_fresh_min_buy_volume_sol() -> f64 {
    25.0
}
fn default_hot_fresh_min_velocity_pct() -> f64 {
    100.0
}

impl Default for TierBGateConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            min_smart_wallets: default_tier_b_min_smart_wallets(),
            min_buyers: default_tier_b_min_buyers(),
            min_buy_volume_sol: default_tier_b_min_buy_volume_sol(),
            min_velocity_pct: default_tier_b_min_velocity_pct(),
            fresh_watchlist: FreshWatchlistConfig::default(),
            hot_fresh_override: HotFreshOverrideConfig::default(),
        }
    }
}

fn default_tier_b_min_smart_wallets() -> u32 {
    0
}
fn default_tier_b_min_buyers() -> u64 {
    10
}
fn default_tier_b_min_buy_volume_sol() -> f64 {
    10.0
}
fn default_tier_b_min_velocity_pct() -> f64 {
    5.0
}

/// Blocks or forces continuation second-look on fragile tier-A entries (score≤7,
/// no smart money, neutral/weak dev, low volume or dump slices in the scoring tape).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WeakATierGateConfig {
    #[serde(default = "default_weak_a_gate_enabled")]
    pub enabled: bool,
    #[serde(default = "default_weak_a_max_score")]
    pub max_score: i32,
    #[serde(default)]
    pub max_smart_wallets: u32,
    /// Hard-skip weak A when `buy_volume_sol` in the scoring window is below this.
    #[serde(default = "default_weak_a_min_buy_volume_sol")]
    pub min_buy_volume_sol: f64,
    /// Hard-skip weak A when `repeat_dump_slices` is at or above this (≥1 catches CB7C5-style tape).
    #[serde(default = "default_weak_a_block_dump_slices_ge")]
    pub block_repeat_dump_slices_ge: u32,
    #[serde(default = "default_true")]
    pub block_dev_neutral: bool,
    #[serde(default = "default_true")]
    pub block_dev_history_weak: bool,
    /// Weak A that reaches continuation may defer on any first-fail reason up to this score.
    #[serde(default = "default_weak_a_cont_second_look_max_score")]
    pub continuation_second_look_max_score: i32,
    /// Hard skip: tier-A + dev_history_weak + no smart + fake b2s + no sell flow.
    #[serde(default)]
    pub synthetic_pump: WeakASyntheticPumpConfig,
}

/// Blocks weak-A synthetic pumps: high b2s with no real sell volume and weak dev history.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WeakASyntheticPumpConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Skip when `buy_to_sell_ratio` is strictly greater than this.
    #[serde(default = "default_synthetic_pump_b2s_threshold")]
    pub buy_to_sell_ratio_gt: f64,
    /// Skip when `sell_volume_window_sol` is strictly less than this.
    #[serde(default = "default_synthetic_pump_min_sell_vol")]
    pub sell_volume_window_sol_lt: f64,
}

impl Default for WeakASyntheticPumpConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            buy_to_sell_ratio_gt: default_synthetic_pump_b2s_threshold(),
            sell_volume_window_sol_lt: default_synthetic_pump_min_sell_vol(),
        }
    }
}

impl Default for WeakATierGateConfig {
    fn default() -> Self {
        Self {
            enabled: default_weak_a_gate_enabled(),
            max_score: default_weak_a_max_score(),
            max_smart_wallets: 0,
            min_buy_volume_sol: default_weak_a_min_buy_volume_sol(),
            block_repeat_dump_slices_ge: default_weak_a_block_dump_slices_ge(),
            block_dev_neutral: default_true(),
            block_dev_history_weak: default_true(),
            continuation_second_look_max_score: default_weak_a_cont_second_look_max_score(),
            synthetic_pump: WeakASyntheticPumpConfig::default(),
        }
    }
}

fn default_weak_a_gate_enabled() -> bool {
    true
}
fn default_weak_a_max_score() -> i32 {
    7
}
fn default_weak_a_min_buy_volume_sol() -> f64 {
    8.0
}
fn default_weak_a_block_dump_slices_ge() -> u32 {
    1
}
fn default_weak_a_cont_second_look_max_score() -> i32 {
    7
}
fn default_synthetic_pump_b2s_threshold() -> f64 {
    10.0
}
fn default_synthetic_pump_min_sell_vol() -> f64 {
    2.0
}

/// Strong smart-money count that bypasses the `require_momentum_good` live gate.
fn default_momentum_good_smart_bypass() -> u32 {
    2
}

/// Default A+ smart bypass: a top-tier (A+) token with >=1 smart wallet skips
/// the `require_momentum_good` gate (see `momentum_good_aplus_smart_bypass`).
fn default_momentum_good_aplus_smart_bypass() -> u32 {
    1
}

/// Live gate: tier **A** with score >= `min_score` and strong early tape may skip
/// `require_momentum_good` without smart wallets (see `momentum_good_strong_a`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MomentumGoodStrongABypassConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    /// Minimum total score (8 or 9 recommended). Score 6–7 weak A stays blocked.
    #[serde(default = "default_strong_a_min_score")]
    pub min_score: i32,
    /// Minimum regular+sniper buyers in the scoring window.
    #[serde(default = "default_strong_a_min_buyers")]
    pub min_buyers: u64,
    /// Minimum buy volume (SOL) in the scoring window.
    #[serde(default = "default_strong_a_min_buy_volume_sol")]
    pub min_buy_volume_sol: f64,
    /// Minimum buy-to-sell ratio (continuation / demand proxy).
    #[serde(default = "default_strong_a_min_buy_to_sell_ratio")]
    pub min_buy_to_sell_ratio: f64,
    /// Require `absorb_strong` score item or `absorb_quality_score` at/above this.
    #[serde(default = "default_true")]
    pub require_absorb_strong: bool,
    #[serde(default = "default_strong_a_min_absorb_quality")]
    pub min_absorb_quality: f64,
    /// Require `buyer_velocity_persistent` item or persistence at/above this.
    #[serde(default = "default_true")]
    pub require_buyer_velocity_persistent: bool,
    #[serde(default = "default_strong_a_min_buyer_velocity_persistence")]
    pub min_buyer_velocity_persistence: f64,
}

impl Default for MomentumGoodStrongABypassConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            min_score: default_strong_a_min_score(),
            min_buyers: default_strong_a_min_buyers(),
            min_buy_volume_sol: default_strong_a_min_buy_volume_sol(),
            min_buy_to_sell_ratio: default_strong_a_min_buy_to_sell_ratio(),
            require_absorb_strong: true,
            min_absorb_quality: default_strong_a_min_absorb_quality(),
            require_buyer_velocity_persistent: true,
            min_buyer_velocity_persistence: default_strong_a_min_buyer_velocity_persistence(),
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_strong_a_min_score() -> i32 {
    8
}

fn default_strong_a_min_buyers() -> u64 {
    8
}

fn default_strong_a_min_buy_volume_sol() -> f64 {
    8.0
}

fn default_strong_a_min_buy_to_sell_ratio() -> f64 {
    1.7
}

fn default_strong_a_min_absorb_quality() -> f64 {
    0.58
}

fn default_strong_a_min_buyer_velocity_persistence() -> f64 {
    0.62
}

/// Default scoring penalty for prolific spam devs (see `spam_dev_penalty`).
fn default_spam_dev_penalty() -> i32 {
    -3
}

/// Default: spam-dev tokens require A+ tier to buy (see `spam_dev_require_a_plus`).
fn default_spam_dev_require_a_plus() -> bool {
    true
}

/// For strong setups (A+ or score ≥ `min_score`), defer an immediate skip on
/// `cont_b2s_worsening` / `cont_sell_absorption`, wait, then re-poll for recovery.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContinuationSecondLookConfig {
    #[serde(default = "default_cont_second_look_enabled")]
    pub enabled: bool,
    /// Idle wait after the first confirm fails (ms).
    #[serde(default = "default_cont_second_look_wait_ms")]
    pub wait_ms: u64,
    /// Second confirm poll duration after `wait_ms` (ms).
    #[serde(default = "default_cont_second_look_recheck_window_ms")]
    pub recheck_window_ms: u64,
    #[serde(default = "default_cont_second_look_recheck_slices")]
    pub recheck_slices: usize,
    /// Strong A-tier entries need at least this score; A+ always qualifies.
    #[serde(default = "default_cont_second_look_min_score")]
    pub min_score: i32,
}

impl Default for ContinuationSecondLookConfig {
    fn default() -> Self {
        Self {
            enabled: default_cont_second_look_enabled(),
            wait_ms: default_cont_second_look_wait_ms(),
            recheck_window_ms: default_cont_second_look_recheck_window_ms(),
            recheck_slices: default_cont_second_look_recheck_slices(),
            min_score: default_cont_second_look_min_score(),
        }
    }
}

fn default_cont_second_look_enabled() -> bool {
    true
}
fn default_cont_second_look_wait_ms() -> u64 {
    1250
}
fn default_cont_second_look_recheck_window_ms() -> u64 {
    1200
}
fn default_cont_second_look_recheck_slices() -> usize {
    3
}
fn default_cont_second_look_min_score() -> i32 {
    10
}

/// A+ with no smart money bought at a high local peak: require a deferred
/// continuation re-check before buy (cuts rug-top entries; smart-money A+ exempt).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContinuationAplusPeakGuardConfig {
    #[serde(default = "default_aplus_peak_guard_enabled")]
    pub enabled: bool,
    #[serde(default = "default_aplus_peak_near_peak_ratio")]
    pub near_peak_ratio: f64,
    #[serde(default = "default_aplus_peak_min_mcap_sol")]
    pub min_mcap_sol: f64,
    /// First confirm must show at least this many upticks to buy without deferral.
    #[serde(default = "default_aplus_peak_strong_upticks")]
    pub strong_upticks: u32,
    #[serde(default = "default_aplus_peak_strong_new_buyers")]
    pub strong_new_buyers: u64,
    /// After deferred recheck, reject buy if `recheck_mcap` is below this fraction
    /// of score-time peak/current (aligns with fill_mcap_abort_min_ratio).
    #[serde(default = "default_aplus_peak_recheck_min_vs_peak_ratio")]
    pub recheck_min_vs_peak_ratio: f64,
}

impl Default for ContinuationAplusPeakGuardConfig {
    fn default() -> Self {
        Self {
            enabled: default_aplus_peak_guard_enabled(),
            near_peak_ratio: default_aplus_peak_near_peak_ratio(),
            min_mcap_sol: default_aplus_peak_min_mcap_sol(),
            strong_upticks: default_aplus_peak_strong_upticks(),
            strong_new_buyers: default_aplus_peak_strong_new_buyers(),
            recheck_min_vs_peak_ratio: default_aplus_peak_recheck_min_vs_peak_ratio(),
        }
    }
}

fn default_aplus_peak_guard_enabled() -> bool {
    true
}
fn default_aplus_peak_near_peak_ratio() -> f64 {
    0.97
}
fn default_aplus_peak_min_mcap_sol() -> f64 {
    90.0
}
fn default_aplus_peak_strong_upticks() -> u32 {
    2
}
fn default_aplus_peak_strong_new_buyers() -> u64 {
    2
}
fn default_aplus_peak_recheck_min_vs_peak_ratio() -> f64 {
    0.78
}

/// Continuation Validation Layer (doc 2.1 / 2.2 / 2.3). After a token passes
/// scoring + the existing gates, observe the tape for one short confirmation
/// window and abort the buy if continuation is breaking down: no price upticks,
/// worsening buy/sell ratio, sell absorption, or no new unique buyers (fake
/// momentum). Ships dark (`enabled = false`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContinuationConfig {
    #[serde(default = "default_continuation_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub second_look: ContinuationSecondLookConfig,
    #[serde(default)]
    pub aplus_peak_guard: ContinuationAplusPeakGuardConfig,
    /// Confirmation poll duration after the score passes (ms).
    #[serde(default = "default_continuation_confirm_window_ms")]
    pub confirm_window_ms: u64,
    /// Number of equal sub-samples inside `confirm_window_ms`.
    #[serde(default = "default_continuation_confirm_slices")]
    pub confirm_slices: usize,
    /// Require mcap to rise across at least this many of the confirm slices (doc 2.1).
    #[serde(default = "default_continuation_min_upticks")]
    pub min_upticks: u32,
    /// Require at least this many new unique buyers during the window (doc 2.3).
    #[serde(default = "default_continuation_min_new_buyers")]
    pub min_new_unique_buyers: u64,
    /// Abort if confirm-window b2s falls below this fraction of its scoring value (doc 2.1).
    #[serde(default = "default_continuation_max_b2s_drop_ratio")]
    pub max_b2s_drop_ratio: f64,
    /// Abort if confirm-window sell volume / buy volume exceeds this (sell absorption).
    #[serde(default = "default_continuation_max_sell_absorption_ratio")]
    pub max_sell_absorption_ratio: f64,
    /// Minimum sustained buys/sec during window. `0` disables this check (doc 2.2).
    #[serde(default = "default_continuation_min_buys_per_sec")]
    pub min_buys_per_sec: f64,
}

impl Default for ContinuationConfig {
    fn default() -> Self {
        Self {
            enabled: default_continuation_enabled(),
            second_look: ContinuationSecondLookConfig::default(),
            aplus_peak_guard: ContinuationAplusPeakGuardConfig::default(),
            confirm_window_ms: default_continuation_confirm_window_ms(),
            confirm_slices: default_continuation_confirm_slices(),
            min_upticks: default_continuation_min_upticks(),
            min_new_unique_buyers: default_continuation_min_new_buyers(),
            max_b2s_drop_ratio: default_continuation_max_b2s_drop_ratio(),
            max_sell_absorption_ratio: default_continuation_max_sell_absorption_ratio(),
            min_buys_per_sec: default_continuation_min_buys_per_sec(),
        }
    }
}

fn default_continuation_enabled() -> bool {
    false
}
fn default_continuation_confirm_window_ms() -> u64 {
    1500
}
fn default_continuation_confirm_slices() -> usize {
    2
}
fn default_continuation_min_upticks() -> u32 {
    1
}
fn default_continuation_min_new_buyers() -> u64 {
    1
}
fn default_continuation_max_b2s_drop_ratio() -> f64 {
    0.6
}
fn default_continuation_max_sell_absorption_ratio() -> f64 {
    1.5
}
fn default_continuation_min_buys_per_sec() -> f64 {
    0.0
}

/// Anti-parabolic entry gate. Targets the "bought the local top" pattern: a
/// token scored only weak A-tier, no smart money, entered while `current_mcap`
/// is already at/near `peak_mcap` (parabolic exhaustion). Such entries are only
/// allowed if the continuation poll shows *strong* fresh demand (upticks + new
/// buyers). Strong A+ / smart-money / runner setups are never affected. Ships
/// dark (`enabled = false`).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AntiParabolicConfig {
    #[serde(default = "default_anti_parabolic_enabled")]
    pub enabled: bool,
    /// `current_mcap >= peak_mcap * near_peak_ratio` counts as "at the peak".
    #[serde(default = "default_anti_parabolic_near_peak_ratio")]
    pub near_peak_ratio: f64,
    /// Score at/below this is "weak" (eligible for the gate). Above => allow.
    #[serde(default = "default_anti_parabolic_weak_score_max")]
    pub weak_score_max: i32,
    /// Confirmation poll window (ms) used when continuation is disabled.
    #[serde(default = "default_anti_parabolic_confirm_window_ms")]
    pub confirm_window_ms: u64,
    /// Confirmation sub-samples used when continuation is disabled.
    #[serde(default = "default_anti_parabolic_confirm_slices")]
    pub confirm_slices: usize,
    /// Upticks in the confirm window required to override the gate (strong demand).
    #[serde(default = "default_anti_parabolic_strong_upticks")]
    pub strong_upticks: u32,
    /// New unique buyers in the confirm window required to override the gate.
    #[serde(default = "default_anti_parabolic_strong_new_buyers")]
    pub strong_new_buyers: u64,
}

impl Default for AntiParabolicConfig {
    fn default() -> Self {
        Self {
            enabled: default_anti_parabolic_enabled(),
            near_peak_ratio: default_anti_parabolic_near_peak_ratio(),
            weak_score_max: default_anti_parabolic_weak_score_max(),
            confirm_window_ms: default_anti_parabolic_confirm_window_ms(),
            confirm_slices: default_anti_parabolic_confirm_slices(),
            strong_upticks: default_anti_parabolic_strong_upticks(),
            strong_new_buyers: default_anti_parabolic_strong_new_buyers(),
        }
    }
}

fn default_anti_parabolic_enabled() -> bool {
    false
}
fn default_anti_parabolic_near_peak_ratio() -> f64 {
    0.97
}
fn default_anti_parabolic_weak_score_max() -> i32 {
    9
}
fn default_anti_parabolic_confirm_window_ms() -> u64 {
    1500
}
fn default_anti_parabolic_confirm_slices() -> usize {
    2
}
fn default_anti_parabolic_strong_upticks() -> u32 {
    2
}
fn default_anti_parabolic_strong_new_buyers() -> u64 {
    3
}

/// Anti-rug entry filters (low fee flow / one-sided pump detection).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AntiRugConfig {
    /// Scoring tightenings (absorb/bv/b2s/tier cap). Independent of pre-buy gate.
    #[serde(default = "default_anti_rug_enabled")]
    pub enabled: bool,
    /// Hard pre-buy SKIP gate (`entry_skip_reason`). Temporary A/B: set false to allow buys.
    #[serde(default = "default_anti_rug_entry_gate_enabled")]
    pub entry_gate_enabled: bool,
    /// Skip buy when `sell_volume_window_sol` is below this (and buy volume is meaningful).
    #[serde(default = "default_min_sell_volume_window_sol")]
    pub min_sell_volume_window_sol: f64,
    /// `sell_volume / buy_volume` must be at least this (fee-flow proxy).
    #[serde(default = "default_min_fee_flow_ratio")]
    pub min_fee_flow_ratio: f64,
    /// Minimum normalized sell pressure score at entry.
    #[serde(default = "default_min_sell_pressure_score")]
    pub min_sell_pressure_score: f64,
    /// Gates apply only when `buy_volume_sol` is at least this.
    #[serde(default = "default_min_buy_volume_for_gates_sol")]
    pub min_buy_volume_for_gates_sol: f64,
    /// `absorb_strong` requires at least this much sell volume in the window.
    #[serde(default = "default_absorb_strong_min_sell_vol_sol")]
    pub absorb_strong_min_sell_vol_sol: f64,
    /// No `buy_to_sell_ratio_high` points when ratio >= this and sell vol is tiny.
    #[serde(default = "default_buy_to_sell_max_without_min_sell_vol")]
    pub buy_to_sell_max_without_min_sell_vol: f64,
    #[serde(default = "default_buy_to_sell_min_sell_vol_sol")]
    pub buy_to_sell_min_sell_vol_sol: f64,
    /// `buyer_velocity_persistent` needs at least this many velocity slices.
    #[serde(default = "default_buyer_velocity_min_slices")]
    pub buyer_velocity_min_slices: usize,
    /// A+ capped to A when peak mcap below this unless buy volume is high enough.
    #[serde(default = "default_low_mcap_peak_sol")]
    pub low_mcap_peak_sol: f64,
    #[serde(default = "default_low_mcap_min_buy_volume_sol")]
    pub low_mcap_min_buy_volume_sol: f64,
}

impl Default for AntiRugConfig {
    fn default() -> Self {
        Self {
            enabled: default_anti_rug_enabled(),
            entry_gate_enabled: default_anti_rug_entry_gate_enabled(),
            min_sell_volume_window_sol: default_min_sell_volume_window_sol(),
            min_fee_flow_ratio: default_min_fee_flow_ratio(),
            min_sell_pressure_score: default_min_sell_pressure_score(),
            min_buy_volume_for_gates_sol: default_min_buy_volume_for_gates_sol(),
            absorb_strong_min_sell_vol_sol: default_absorb_strong_min_sell_vol_sol(),
            buy_to_sell_max_without_min_sell_vol: default_buy_to_sell_max_without_min_sell_vol(),
            buy_to_sell_min_sell_vol_sol: default_buy_to_sell_min_sell_vol_sol(),
            buyer_velocity_min_slices: default_buyer_velocity_min_slices(),
            low_mcap_peak_sol: default_low_mcap_peak_sol(),
            low_mcap_min_buy_volume_sol: default_low_mcap_min_buy_volume_sol(),
        }
    }
}

fn default_anti_rug_enabled() -> bool {
    true
}
fn default_anti_rug_entry_gate_enabled() -> bool {
    true
}
fn default_min_sell_volume_window_sol() -> f64 {
    2.0
}
fn default_min_fee_flow_ratio() -> f64 {
    0.08
}
fn default_min_sell_pressure_score() -> f64 {
    0.05
}
fn default_min_buy_volume_for_gates_sol() -> f64 {
    8.0
}
fn default_absorb_strong_min_sell_vol_sol() -> f64 {
    1.5
}
fn default_buy_to_sell_max_without_min_sell_vol() -> f64 {
    12.0
}
fn default_buy_to_sell_min_sell_vol_sol() -> f64 {
    3.0
}
fn default_buyer_velocity_min_slices() -> usize {
    2
}
fn default_low_mcap_peak_sol() -> f64 {
    90.0
}
fn default_low_mcap_min_buy_volume_sol() -> f64 {
    25.0
}

fn default_window_ms() -> u64 {
    1500
}
fn default_buyer_velocity_slices() -> usize {
    3
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

    #[serde(default = "default_weight_buyer_velocity_persistent")]
    pub buyer_velocity_persistent: i32,
    #[serde(default = "default_weight_buyer_velocity_fading")]
    pub buyer_velocity_fading: i32,
    #[serde(default = "default_weight_sell_pressure_high")]
    pub sell_pressure_high: i32,
    #[serde(default = "default_weight_absorb_strong")]
    pub absorb_strong: i32,
    #[serde(default = "default_weight_smart_early_exit")]
    pub smart_early_exit_dump: i32,
    #[serde(default = "default_weight_repeat_dump")]
    pub repeat_dump_penalty: i32,
}

impl Default for ScoringWeights {
    fn default() -> Self {
        Self {
            dev_history_strong: 2,
            dev_history_weak: 2,
            dev_ranker_a_plus: 3,
            dev_ranker_a: 1,
            dev_ranker_bad: -3,
            smart_wallets_3plus: 4,
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
            buyer_velocity_persistent: default_weight_buyer_velocity_persistent(),
            buyer_velocity_fading: default_weight_buyer_velocity_fading(),
            sell_pressure_high: default_weight_sell_pressure_high(),
            absorb_strong: default_weight_absorb_strong(),
            smart_early_exit_dump: default_weight_smart_early_exit(),
            repeat_dump_penalty: default_weight_repeat_dump(),
        }
    }
}

fn default_weight_buyer_velocity_persistent() -> i32 {
    1
}
fn default_weight_buyer_velocity_fading() -> i32 {
    -2
}
fn default_weight_sell_pressure_high() -> i32 {
    -2
}
fn default_weight_absorb_strong() -> i32 {
    2
}
fn default_weight_smart_early_exit() -> i32 {
    -2
}
fn default_weight_repeat_dump() -> i32 {
    -1
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
            buyers_low: 4,
            buy_to_sell_high: 1.7,
            momentum_good_low_pct: 12.0,
            momentum_good_high_pct: 30.0,
            momentum_overheated_pct: 55.0,
            volume_ok_sol: 12.0,
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
    4
}
fn default_smart_wallet_1plus_min() -> u32 {
    1
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TierSize {
    pub a_plus_sol: f64,
    pub a_sol: f64,
    #[serde(default = "default_b_sol")]
    pub b_sol: f64,
}

fn default_b_sol() -> f64 {
    0.2
}

impl Default for TierSize {
    fn default() -> Self {
        Self {
            a_plus_sol: 0.4,
            a_sol: 0.3,
            b_sol: default_b_sol(),
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
