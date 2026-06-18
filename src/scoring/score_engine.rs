//! Combine the feature vector into a single score and an A+/A/SKIP tier.

use serde::Serialize;

use crate::scoring::anti_rug::{cap_tier_for_low_mcap, rewards_buy_to_sell_ratio};
use crate::scoring::config::{FeatureThresholds, ScoringConfig, ScoringWeights};
use crate::scoring::dev_ranker::DevCategory;
use crate::scoring::features::{
    aa_momentum_override_for_tier, momentum_peak_pct, tier_b_dev_eligible, tier_b_entry_ok,
    fresh_b_hot_override_ok, TokenFeatures,
};

/// Momentum for scoring: peak mcap in the window vs first sample (not end-only).
fn scoring_momentum_pct(f: &TokenFeatures) -> f64 {
    let peak = f.peak_mcap_sol.max(f.current_mcap_sol);
    momentum_peak_pct(f.initial_mcap_sol, peak)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Tier {
    APlus,
    A,
    B,
    Skip,
}

#[derive(Clone, Debug, Serialize)]
pub struct ScoreBreakdown {
    pub total: i32,
    pub items: Vec<(&'static str, i32)>,
    pub tier: Tier,
    pub recommended_size_sol: f64,
    /// Tier B assigned via `hot_fresh_override` (momentum-only fail + strong tape).
    #[serde(default)]
    pub fresh_b_hot_override: bool,
    /// Tier A: `require_momentum_good` bypass (`momentum_overheated` only).
    #[serde(default)]
    pub a_momentum_override: bool,
    /// Tier A+: same bypass for A+ lane.
    #[serde(default)]
    pub a_plus_momentum_override: bool,
}

pub struct ScoreEngine<'a> {
    cfg: &'a ScoringConfig,
}

impl<'a> ScoreEngine<'a> {
    pub fn new(cfg: &'a ScoringConfig) -> Self {
        Self { cfg }
    }

    pub fn score(&self, f: &TokenFeatures, thresholds: &FeatureThresholds) -> ScoreBreakdown {
        if self.cfg.legacy_scoring {
            self.score_legacy(f)
        } else {
            self.score_v2(f, thresholds)
        }
    }

    /// Pre–entry-filter-V2: YAML `thresholds` only, classic momentum + bundle rules.
    fn score_legacy(&self, f: &TokenFeatures) -> ScoreBreakdown {
        let w = &self.cfg.weights;
        let t = &self.cfg.thresholds;
        let mut items: Vec<(&'static str, i32)> = Vec::with_capacity(12);
        let mut total = 0_i32;

        let add = |name: &'static str, points: i32, total: &mut i32, items: &mut Vec<_>| {
            if points != 0 {
                *total += points;
                items.push((name, points));
            }
        };

        // Prolific serial launcher: heavy creator-stats was skipped, so this
        // token must earn its tier on tape alone — apply the spam penalty.
        if f.is_spam_dev {
            add(
                "spam_dev_penalty",
                self.cfg.spam_dev_penalty,
                &mut total,
                &mut items,
            );
        }

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
            DevCategory::Neutral | DevCategory::Stale | DevCategory::Fresh => {}
        }

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

        let buyers = f.buyer_count();
        if buyers >= t.buyers_high {
            add("buyers_10plus", w.buyers_10plus, &mut total, &mut items);
        } else if buyers >= t.buyers_mid {
            add("buyers_6plus", w.buyers_6plus, &mut total, &mut items);
        } else if buyers < t.buyers_low {
            add("buyers_below_3", w.buyers_below_3, &mut total, &mut items);
        }

        if rewards_buy_to_sell_ratio(f, &self.cfg.anti_rug, t.buy_to_sell_high) {
            add(
                "buy_to_sell_ratio_high",
                w.buy_to_sell_ratio_high,
                &mut total,
                &mut items,
            );
        }

        let momentum_pct = scoring_momentum_pct(f);
        if momentum_pct >= t.momentum_overheated_pct {
            add(
                "momentum_overheated",
                w.momentum_overheated,
                &mut total,
                &mut items,
            );
        } else if (t.momentum_good_low_pct..=t.momentum_good_high_pct).contains(&momentum_pct) {
            add("momentum_good", w.momentum_good, &mut total, &mut items);
        }

        if f.buy_volume_sol >= t.volume_ok_sol {
            add("volume_ok", w.volume_ok, &mut total, &mut items);
        }

        if f.bundle.identical_size_ratio >= t.bundle_identical_ratio {
            add(
                "bundle_identical",
                w.bundle_identical,
                &mut total,
                &mut items,
            );
        } else if f.bundle.similar_size_ratio >= t.bundle_similar_ratio {
            add("bundle_similar", w.bundle_similar, &mut total, &mut items);
        }

        self.apply_early_tape_scores(f, w, &mut total, &mut items);

        self.finish_breakdown(f, total, items)
    }

    /// Entry filter V2: merged thresholds, band-first momentum, similar-cluster bundle only.
    fn score_v2(&self, f: &TokenFeatures, thresholds: &FeatureThresholds) -> ScoreBreakdown {
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

        // Prolific serial launcher: heavy creator-stats was skipped, so this
        // token must earn its tier on tape alone — apply the spam penalty.
        if f.is_spam_dev {
            add(
                "spam_dev_penalty",
                self.cfg.spam_dev_penalty,
                &mut total,
                &mut items,
            );
        }

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
            DevCategory::Neutral | DevCategory::Stale | DevCategory::Fresh => {}
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
        if rewards_buy_to_sell_ratio(f, &self.cfg.anti_rug, t.buy_to_sell_high) {
            add(
                "buy_to_sell_ratio_high",
                w.buy_to_sell_ratio_high,
                &mut total,
                &mut items,
            );
        }

        // --- Momentum (peak mcap vs window start) --------------------------
        let momentum_pct = scoring_momentum_pct(f);
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

        self.apply_early_tape_scores(f, w, &mut total, &mut items);

        self.finish_breakdown(f, total, items)
    }

    fn finish_breakdown(
        &self,
        f: &TokenFeatures,
        total: i32,
        items: Vec<(&'static str, i32)>,
    ) -> ScoreBreakdown {
        let score_tier = cap_tier_for_low_mcap(
            f,
            &self.cfg.anti_rug,
            if total >= self.cfg.a_plus_threshold {
                Tier::APlus
            } else if total >= self.cfg.a_threshold {
                Tier::A
            } else {
                Tier::Skip
            },
        );

        // Fresh dev / zero-launch: never A or A+ — only tier B (if B gates pass) or Skip.
        let tier_b_ok = tier_b_entry_ok(&self.cfg.tier_b, f, &items);
        let hot_override = !tier_b_ok && fresh_b_hot_override_ok(&self.cfg.tier_b, f, &items);
        let tier = if tier_b_dev_eligible(f, &self.cfg.tier_b) {
            if tier_b_ok || hot_override {
                Tier::B
            } else {
                Tier::Skip
            }
        } else {
            score_tier
        };

        let recommended_size_sol = match tier {
            Tier::APlus => self.cfg.size.a_plus_sol,
            Tier::A => self.cfg.size.a_sol,
            Tier::B => self.cfg.size.b_sol,
            Tier::Skip => 0.0,
        };

        let a_momentum_override = tier == Tier::A
            && aa_momentum_override_for_tier(&self.cfg.aa_momentum_override, tier, &items);
        let a_plus_momentum_override = tier == Tier::APlus
            && aa_momentum_override_for_tier(&self.cfg.aa_momentum_override, tier, &items);

        ScoreBreakdown {
            total,
            items,
            tier,
            recommended_size_sol,
            fresh_b_hot_override: hot_override,
            a_momentum_override,
            a_plus_momentum_override,
        }
    }

    /// Buyer cadence + sell-tape signals (shared by legacy and V2 paths).
    fn apply_early_tape_scores(
        &self,
        f: &TokenFeatures,
        w: &ScoringWeights,
        total: &mut i32,
        items: &mut Vec<(&'static str, i32)>,
    ) {
        let ar = &self.cfg.anti_rug;
        let add = |name: &'static str, points: i32, total: &mut i32, items: &mut Vec<_>| {
            if points != 0 {
                *total += points;
                items.push((name, points));
            }
        };

        let min_slices = if ar.enabled {
            ar.buyer_velocity_min_slices.max(2)
        } else {
            1
        };
        if f.buyer_velocity_persistence >= 0.62
            && f.buyer_velocity_new_per_slice.len() >= min_slices
        {
            add(
                "buyer_velocity_persistent",
                w.buyer_velocity_persistent,
                total,
                items,
            );
        } else if f.buyer_velocity_persistence <= 0.28 && f.buyer_velocity_new_per_slice.len() >= 2
        {
            add(
                "buyer_velocity_fading",
                w.buyer_velocity_fading,
                total,
                items,
            );
        }

        if f.sell_pressure_score >= 0.58 {
            add("sell_pressure_high", w.sell_pressure_high, total, items);
        }

        let absorb_min_sell = if ar.enabled {
            ar.absorb_strong_min_sell_vol_sol
        } else {
            0.05
        };
        if f.absorb_quality_score >= 0.58 && f.sell_volume_window_sol >= absorb_min_sell {
            add("absorb_strong", w.absorb_strong, total, items);
        }

        if f.smart_wallet_early_exits >= 2 {
            add(
                "smart_early_exit_dump",
                w.smart_early_exit_dump,
                total,
                items,
            );
        } else if f.smart_wallet_early_exits == 1 {
            let pen = (w.smart_early_exit_dump / 2).max(-2);
            add("smart_early_exit_dump", pen, total, items);
        }

        if f.repeat_dump_slices >= 2 {
            add(
                "repeat_dump_cluster",
                w.repeat_dump_penalty.saturating_mul(3),
                total,
                items,
            );
        } else if f.repeat_dump_slices >= 1 {
            // Always penalize a dump slice in the scoring window (CB7C5 had dumps=1
            // with low sell_pressure and still reached tier A without this).
            add(
                "repeat_dump_cluster",
                w.repeat_dump_penalty.saturating_mul(2),
                total,
                items,
            );
        }
    }
}
