//! Combine the feature vector into a single score and an A+/A/SKIP tier.

use serde::Serialize;

use crate::scoring::config::{FeatureThresholds, ScoringConfig};
use crate::scoring::dev_ranker::DevCategory;
use crate::scoring::features::TokenFeatures;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Tier {
    APlus,
    A,
    Skip,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScoreBreakdown {
    pub total: i32,
    pub items: Vec<(&'static str, i32)>,
    pub tier: Tier,
    pub recommended_size_sol: f64,
}

pub struct ScoreEngine<'a> {
    cfg: &'a ScoringConfig,
}

impl<'a> ScoreEngine<'a> {
    pub fn new(cfg: &'a ScoringConfig) -> Self {
        Self { cfg }
    }

    pub fn score(&self, f: &TokenFeatures, thresholds: &FeatureThresholds) -> ScoreBreakdown {
        let w = &self.cfg.weights;
        let t = thresholds;
        let mut items: Vec<(&'static str, i32)> = Vec::with_capacity(12);
        let mut total = 0_i32;

        let add = |name: &'static str, points: i32, total: &mut i32, items: &mut Vec<_>| {
            if points != 0 {
                *total += points;
                items.push((name, points));
            }
        };

        // --- Dev history (static historical table) -------------------------
        if f.dev_has_history {
            let strong = f.dev_total_coins >= t.dev_strong_min_coins
                && f.dev_pnl_avg >= t.dev_strong_min_pnl_pct;
            if strong {
                add(
                    "dev_history_strong",
                    w.dev_history_strong,
                    &mut total,
                    &mut items,
                );
            } else {
                add(
                    "dev_history_weak",
                    w.dev_history_weak,
                    &mut total,
                    &mut items,
                );
            }
        }

        // --- Dev ranking (our own past trades) -----------------------------
        match f.dev_category {
            DevCategory::APlus => add(
                "dev_ranker_a_plus",
                w.dev_ranker_a_plus,
                &mut total,
                &mut items,
            ),
            DevCategory::A => add("dev_ranker_a", w.dev_ranker_a, &mut total, &mut items),
            DevCategory::Bad => add(
                "dev_ranker_bad",
                w.dev_ranker_bad,
                &mut total,
                &mut items,
            ),
            DevCategory::Neutral | DevCategory::Stale => {}
        }

        // --- Smart wallets -------------------------------------------------
        if f.smart_wallet_count >= t.smart_wallet_3plus_min {
            add(
                "smart_wallets_3plus",
                w.smart_wallets_3plus,
                &mut total,
                &mut items,
            );
        } else if f.smart_wallet_count >= t.smart_wallet_1plus_min {
            add(
                "smart_wallets_1plus",
                w.smart_wallets_1plus,
                &mut total,
                &mut items,
            );
        }

        // --- Early buyer count --------------------------------------------
        let buyers = f.buyer_count();
        if buyers >= t.buyers_high {
            add("buyers_10plus", w.buyers_10plus, &mut total, &mut items);
        } else if buyers >= t.buyers_mid {
            add("buyers_6plus", w.buyers_6plus, &mut total, &mut items);
        } else if buyers < t.buyers_low {
            add("buyers_below_3", w.buyers_below_3, &mut total, &mut items);
        }

        // --- Buy/sell pressure --------------------------------------------
        if f.buy_to_sell_ratio >= t.buy_to_sell_high {
            add(
                "buy_to_sell_ratio_high",
                w.buy_to_sell_ratio_high,
                &mut total,
                &mut items,
            );
        }

        // --- Momentum (mcap delta during scoring window) -------------------
        let momentum_pct = if f.initial_mcap_sol > 0.0 {
            (f.current_mcap_sol / f.initial_mcap_sol - 1.0) * 100.0
        } else {
            0.0
        };
        // V2: reward only launches inside [low, high]; penalize anything above
        // the good window, and still allow a separate "blow-off" floor via
        // `momentum_overheated_pct` when it sits above `good_high` (legacy YAML).
        let in_good_band =
            (t.momentum_good_low_pct..=t.momentum_good_high_pct).contains(&momentum_pct);
        if in_good_band {
            add("momentum_good", w.momentum_good, &mut total, &mut items);
        } else if momentum_pct > t.momentum_good_high_pct
            || (t.momentum_overheated_pct > t.momentum_good_high_pct
                && momentum_pct >= t.momentum_overheated_pct)
        {
            add(
                "momentum_overheated",
                w.momentum_overheated,
                &mut total,
                &mut items,
            );
        }

        // --- Volume --------------------------------------------------------
        if f.buy_volume_sol >= t.volume_ok_sol {
            add("volume_ok", w.volume_ok, &mut total, &mut items);
        }

        // --- Anti-bundle (V2) ----------------------------------------------
        // Similar-size clustering (median band) catches coordinated bundles
        // that no longer use byte-identical amounts. Penalty: `bundle_similar`.
        if f.bundle.similar_size_ratio >= t.bundle_similar_ratio {
            add("bundle_similar", w.bundle_similar, &mut total, &mut items);
        }

        let tier = if total >= self.cfg.a_plus_threshold {
            Tier::APlus
        } else if total >= self.cfg.a_threshold {
            Tier::A
        } else {
            Tier::Skip
        };

        let recommended_size_sol = match tier {
            Tier::APlus => self.cfg.size.a_plus_sol,
            Tier::A => self.cfg.size.a_sol,
            Tier::Skip => 0.0,
        };

        ScoreBreakdown {
            total,
            items,
            tier,
            recommended_size_sol,
        }
    }
}
