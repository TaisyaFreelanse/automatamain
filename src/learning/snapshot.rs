use serde::{Deserialize, Serialize};

use crate::scoring::{
    features::TokenFeatures,
    fresh_b::FreshBSubtype,
    score_engine::ScoreBreakdown,
};

/// Serializable snapshot at scoring time (attached to a buy / stored on Position).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LearningTradeSnapshot {
    pub mint: String,
    pub dev: String,
    pub entry_mcap_sol: f64,
    pub smart_wallet_count: u32,
    /// Bonding-curve mcap % change during the scoring window (same as score engine momentum).
    pub velocity_pct: f64,
    pub bundle_similar: f64,
    pub bundle_identical: f64,
    pub buyer_count: u64,
    pub buy_to_sell_ratio: f64,
    pub buy_volume_sol: f64,
    pub score_total: i32,
    pub tier: String,

    #[serde(default)]
    pub buyer_velocity_persistence: f64,
    #[serde(default)]
    pub buyer_velocity_new_per_slice: Vec<u64>,
    #[serde(default)]
    pub sell_pressure_score: f64,
    #[serde(default)]
    pub absorb_quality_score: f64,
    #[serde(default)]
    pub sell_volume_window_sol: f64,
    #[serde(default)]
    pub sell_events_window: u64,
    #[serde(default)]
    pub repeat_dump_slices: u32,
    #[serde(default)]
    pub smart_wallet_early_exits: u32,
    /// Score breakdown included `momentum_good` at entry (tier-A TIME KILL defer).
    #[serde(default)]
    pub has_momentum_good: bool,
    /// Tier B fresh subtype: `B_TRUE_FRESH` or `B_UNKNOWN` (only for tier B fresh lane).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fresh_b_subtype: Option<String>,
    /// Fresh Watchlist path marker: `added` | `passed` (only when applicable).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fresh_watchlist: Option<String>,
    /// Tier B hot-fresh override applied (`momentum_overheated_only`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fresh_b_hot_override: Option<bool>,
    #[serde(default)]
    pub momentum_overheated: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hot_override_reason: Option<String>,
    /// Tier A momentum-overheated override applied at live gate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub a_momentum_override: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub a_plus_momentum_override: Option<bool>,
    /// Mcap at first observation in scoring window (discovery).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_seen_mcap_sol: Option<f64>,
    /// Ms from pipeline start (mint create) to buy decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_to_buy_ms: Option<u64>,
}

impl LearningTradeSnapshot {
    pub fn from_scoring(f: &TokenFeatures, breakdown: &ScoreBreakdown) -> Self {
        let peak = f.peak_mcap_sol.max(f.current_mcap_sol);
        let velocity_pct = if f.initial_mcap_sol > 0.0 {
            (peak / f.initial_mcap_sol - 1.0) * 100.0
        } else {
            0.0
        };
        Self {
            mint: f.mint.to_string(),
            dev: f.dev.to_string(),
            entry_mcap_sol: f.current_mcap_sol,
            smart_wallet_count: f.smart_wallet_count,
            velocity_pct,
            bundle_similar: f.bundle.similar_size_ratio,
            bundle_identical: f.bundle.identical_size_ratio,
            buyer_count: f.buyer_count(),
            buy_to_sell_ratio: f.buy_to_sell_ratio,
            buy_volume_sol: f.buy_volume_sol,
            score_total: breakdown.total,
            tier: format!("{:?}", breakdown.tier),
            buyer_velocity_persistence: f.buyer_velocity_persistence,
            buyer_velocity_new_per_slice: f.buyer_velocity_new_per_slice.clone(),
            sell_pressure_score: f.sell_pressure_score,
            absorb_quality_score: f.absorb_quality_score,
            sell_volume_window_sol: f.sell_volume_window_sol,
            sell_events_window: f.sell_events_window,
            repeat_dump_slices: f.repeat_dump_slices,
            smart_wallet_early_exits: f.smart_wallet_early_exits,
            has_momentum_good: breakdown
                .items
                .iter()
                .any(|(name, _)| *name == "momentum_good"),
            fresh_b_subtype: None,
            fresh_watchlist: None,
            fresh_b_hot_override: None,
            momentum_overheated: breakdown
                .items
                .iter()
                .any(|(name, _)| *name == "momentum_overheated"),
            hot_override_reason: None,
            a_momentum_override: None,
            a_plus_momentum_override: None,
            first_seen_mcap_sol: None,
            discovery_to_buy_ms: None,
        }
    }

    pub fn apply_aa_momentum_override_fields(&mut self, breakdown: &ScoreBreakdown) {
        if breakdown.a_momentum_override {
            self.a_momentum_override = Some(true);
            self.hot_override_reason = Some("momentum_overheated_only".to_string());
        }
        if breakdown.a_plus_momentum_override {
            self.a_plus_momentum_override = Some(true);
            self.hot_override_reason = Some("momentum_overheated_only".to_string());
        }
    }

    pub fn apply_hot_override_fields(&mut self, hot_override: bool) {
        if hot_override {
            self.fresh_b_hot_override = Some(true);
            self.hot_override_reason = Some("momentum_overheated_only".to_string());
        }
    }

    pub fn from_scoring_fresh_b(
        f: &TokenFeatures,
        breakdown: &ScoreBreakdown,
        subtype: Option<FreshBSubtype>,
        fresh_watchlist: Option<&str>,
    ) -> Self {
        let mut snap = Self::from_scoring(f, breakdown);
        snap.fresh_b_subtype = subtype.map(|s| s.as_str().to_string());
        snap.fresh_watchlist = fresh_watchlist.map(str::to_string);
        snap.apply_hot_override_fields(breakdown.fresh_b_hot_override);
        snap.apply_aa_momentum_override_fields(breakdown);
        snap
    }
}
