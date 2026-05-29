//! Pre-buy anti-rug gates and tier caps (low sell-side / fake pump detection).

use crate::scoring::config::AntiRugConfig;
use crate::scoring::features::TokenFeatures;
use crate::scoring::score_engine::Tier;

/// Live entry skip reason (logged to `learning_skipped.stage = anti_rug`).
pub fn entry_skip_reason(f: &TokenFeatures, cfg: &AntiRugConfig) -> Option<&'static str> {
    if !cfg.entry_gate_enabled || f.buy_volume_sol < cfg.min_buy_volume_for_gates_sol {
        return None;
    }
    let sell_vol = f.sell_volume_window_sol;
    let buy_vol = f.buy_volume_sol;
    let fee_flow = sell_vol / buy_vol;

    if sell_vol < cfg.min_sell_volume_window_sol {
        return Some("low_sell_side_volume");
    }
    if fee_flow < cfg.min_fee_flow_ratio {
        return Some("low_fee_flow_ratio");
    }
    if f.sell_pressure_score < cfg.min_sell_pressure_score {
        return Some("low_sell_pressure");
    }
    if f.buy_to_sell_ratio >= cfg.buy_to_sell_max_without_min_sell_vol
        && sell_vol < cfg.buy_to_sell_min_sell_vol_sol
    {
        return Some("fake_buy_to_sell_ratio");
    }
    None
}

/// Downgrade A+ on thin low-mcap pumps unless volume is large enough.
pub fn cap_tier_for_low_mcap(f: &TokenFeatures, cfg: &AntiRugConfig, tier: Tier) -> Tier {
    if !cfg.enabled || tier != Tier::APlus {
        return tier;
    }
    if f.peak_mcap_sol < cfg.low_mcap_peak_sol
        && f.buy_volume_sol < cfg.low_mcap_min_buy_volume_sol
    {
        Tier::A
    } else {
        tier
    }
}

/// Whether `buy_to_sell_ratio_high` score points may apply.
pub fn rewards_buy_to_sell_ratio(f: &TokenFeatures, cfg: &AntiRugConfig, ratio_high: f64) -> bool {
    if f.buy_to_sell_ratio < ratio_high {
        return false;
    }
    if !cfg.enabled {
        return true;
    }
    f.sell_volume_window_sol >= cfg.buy_to_sell_min_sell_vol_sol
        || f.buy_to_sell_ratio < cfg.buy_to_sell_max_without_min_sell_vol
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scoring::anti_bundle::BundleStats;
    use crate::scoring::dev_ranker::{DevCategory, DevRecord};
    use crate::scoring::features::EarlyBuyersSnapshot;
    use solana_address::Address;

    fn sample_features(buy_vol: f64, sell_vol: f64, b2s: f64, sp: f64) -> TokenFeatures {
        TokenFeatures {
            mint: Address::new_from_array([0; 32]),
            dev: Address::new_from_array([1; 32]),
            dev_has_history: false,
            dev_total_coins: 0,
            dev_pnl_avg: 0.0,
            dev_holders_avg: 0,
            dev_volume_avg: 0.0,
            dev_trades_avg: 0,
            dev_buy_to_sell_ratio: 0.0,
            dev_category: DevCategory::Neutral,
            dev_rank_score: 0.0,
            dev_rank_record: DevRecord::default(),
            is_spam_dev: false,
            current_mcap_sol: 60.0,
            initial_mcap_sol: 50.0,
            peak_mcap_sol: 58.0,
            buyers: EarlyBuyersSnapshot::default(),
            regular_buyer_count: 10,
            sniper_count: 5,
            buy_volume_sol: buy_vol,
            still_long_count: 10,
            already_sold_count: 1,
            buy_to_sell_ratio: b2s,
            bundle: BundleStats::empty(),
            smart_wallet_count: 0,
            buyer_velocity_new_per_slice: vec![5, 6],
            buyer_velocity_persistence: 0.72,
            sell_pressure_score: sp,
            absorb_quality_score: 0.99,
            sell_events_window: 2,
            sell_volume_window_sol: sell_vol,
            repeat_dump_slices: 0,
            smart_wallet_early_exits: 0,
        }
    }

    #[test]
    fn skips_low_sell_side_pump() {
        let cfg = AntiRugConfig::default();
        let f = sample_features(47.0, 0.35, 13.0, 0.001);
        assert_eq!(entry_skip_reason(&f, &cfg), Some("low_sell_side_volume"));
    }

    #[test]
    fn passes_two_sided_market() {
        let cfg = AntiRugConfig::default();
        let f = sample_features(40.0, 5.0, 4.0, 0.12);
        assert_eq!(entry_skip_reason(&f, &cfg), None);
    }

    #[test]
    fn caps_a_plus_on_low_mcap_thin_vol() {
        let cfg = AntiRugConfig::default();
        let f = sample_features(15.0, 3.0, 3.0, 0.1);
        assert_eq!(cap_tier_for_low_mcap(&f, &cfg, Tier::APlus), Tier::A);
    }
}
