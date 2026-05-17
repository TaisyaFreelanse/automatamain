use serde::{Deserialize, Serialize};

use crate::scoring::{features::TokenFeatures, score_engine::ScoreBreakdown};

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
}

impl LearningTradeSnapshot {
    pub fn from_scoring(f: &TokenFeatures, breakdown: &ScoreBreakdown) -> Self {
        let velocity_pct = if f.initial_mcap_sol > 0.0 {
            (f.current_mcap_sol / f.initial_mcap_sol - 1.0) * 100.0
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
        }
    }
}
