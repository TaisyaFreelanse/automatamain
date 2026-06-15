use serde::{Deserialize, Serialize};

use crate::persistence::creators::CreatorStatistics;

fn default_spam_skip_coins() -> Option<u64> {
    Some(100)
}

fn default_spam_dev_whitelist() -> Vec<String> {
    Vec::new()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreatorStatisticsFilter {
    pub min_total_coins: Option<u64>,
    pub max_total_coins: Option<u64>,

    /// Early spam-dev cutoff: if a dev has launched more than this many coins,
    /// skip the token *before* running the expensive creator-stats aggregation
    /// (which would otherwise scan hundreds of thousands of trade rows). Such
    /// prolific devs are almost always spam/serial ruggers. Checked with a cheap
    /// capped `count` so cost is independent of how many coins the dev has.
    /// `None` disables the early gate. Defaults to 100.
    #[serde(default = "default_spam_skip_coins")]
    pub spam_skip_coins: Option<u64>,

    /// Prolific devs (> `spam_skip_coins`) that showed strong post-skip runners in
    /// production audits: full creator-stats path, no `spam_dev_penalty`, no A+-only
    /// buy gate. Pubkey base58 strings.
    #[serde(default = "default_spam_dev_whitelist")]
    pub spam_dev_whitelist: Vec<String>,

    pub min_median_market_cap: Option<f64>,
    pub min_trader_pnl_average: Option<f64>,
    pub min_total_holders_average: Option<f64>,
    pub min_average_volume: Option<f64>,
    pub min_median_total_trades: Option<f64>,
    pub min_average_unique_buy_to_sell_ratio: Option<f64>,
    pub min_average_buy_trader_size: Option<f64>,
}

impl CreatorStatisticsFilter {
    /// True when this dev is treated like a normal creator despite high coin count.
    pub fn is_spam_dev_whitelisted(&self, dev: &str) -> bool {
        self.spam_dev_whitelist
            .iter()
            .any(|w| w == dev)
    }

    pub fn filter(&self, s: &CreatorStatistics) -> bool {
        self.reject_reasons(s).is_empty()
    }

    /// Which configured thresholds failed for this creator snapshot.
    pub fn reject_reasons(&self, s: &CreatorStatistics) -> Vec<&'static str> {
        let mut out = Vec::new();

        if let Some(min) = self.min_total_coins
            && s.total_coins < min
        {
            out.push("min_total_coins");
        }

        if let Some(max) = self.max_total_coins
            && s.total_coins > max
        {
            out.push("max_total_coins");
        }

        if let Some(min) = self.min_median_market_cap
            && s.median_market_cap.amount().to_float() < min
        {
            out.push("min_median_market_cap");
        }

        if let Some(min) = self.min_trader_pnl_average
            && s.trader_pnl_average < min
        {
            out.push("min_trader_pnl_average");
        }

        if let Some(min) = self.min_total_holders_average
            && (s.total_holders_average as f64) < min
        {
            out.push("min_total_holders_average");
        }

        if let Some(min) = self.min_average_volume
            && s.average_volume < min
        {
            out.push("min_average_volume");
        }

        if let Some(min) = self.min_median_total_trades
            && (s.median_total_trades as f64) < min
        {
            out.push("min_median_total_trades");
        }

        if let Some(min) = self.min_average_unique_buy_to_sell_ratio
            && s.average_unique_buy_to_sell_ratio < min
        {
            out.push("min_average_unique_buy_to_sell_ratio");
        }

        if let Some(min) = self.min_average_buy_trader_size
            && s.average_buy_trader_size.amount().to_float() < min
        {
            out.push("min_average_buy_trader_size");
        }

        out
    }
}

/// True when every failed threshold is an early-stats field (immature single-coin
/// aggregate), not a structural creator-quality gate.
pub fn is_early_stats_only_reject(reasons: &[&str]) -> bool {
    if reasons.is_empty() {
        return false;
    }
    const EARLY: &[&str] = &[
        "min_median_market_cap",
        "min_trader_pnl_average",
        "min_total_holders_average",
        "min_average_volume",
        "min_median_total_trades",
    ];
    reasons.iter().all(|r| EARLY.contains(r))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generalize::general_commands::Currency;

    fn sample_stats() -> CreatorStatistics {
        CreatorStatistics {
            median_market_cap: Currency::from_float_native(100.0),
            trader_pnl_average: 10.0,
            total_holders_average: 60,
            average_volume: 100.0,
            median_total_trades: 60,
            average_unique_buy_to_sell_ratio: 1.0,
            average_buy_trader_size: Currency::from_float_native(1.0),
            total_coins: 1,
        }
    }

    #[test]
    fn reject_reasons_early_stats_only() {
        let f = CreatorStatisticsFilter {
            min_total_coins: None,
            max_total_coins: None,
            spam_skip_coins: Some(100),
            spam_dev_whitelist: vec![],
            min_median_market_cap: Some(90.0),
            min_trader_pnl_average: Some(4.0),
            min_total_holders_average: Some(50.0),
            min_average_volume: Some(90.0),
            min_median_total_trades: Some(50.0),
            min_average_unique_buy_to_sell_ratio: None,
            min_average_buy_trader_size: None,
        };
        let mut s = sample_stats();
        s.total_holders_average = 5;
        s.median_total_trades = 3;
        s.average_volume = 2.0;
        let reasons = f.reject_reasons(&s);
        assert!(!reasons.is_empty());
        assert!(is_early_stats_only_reject(&reasons));
    }

    #[test]
    fn reject_reasons_structural_not_watchlist() {
        let f = CreatorStatisticsFilter {
            min_total_coins: Some(2),
            max_total_coins: None,
            spam_skip_coins: Some(100),
            spam_dev_whitelist: vec![],
            min_median_market_cap: Some(90.0),
            min_trader_pnl_average: None,
            min_total_holders_average: None,
            min_average_volume: None,
            min_median_total_trades: None,
            min_average_unique_buy_to_sell_ratio: None,
            min_average_buy_trader_size: None,
        };
        let s = sample_stats();
        let reasons = f.reject_reasons(&s);
        assert!(reasons.contains(&"min_total_coins"));
        assert!(!is_early_stats_only_reject(&reasons));
    }

    #[test]
    fn reject_reasons_mixed_not_watchlist() {
        let f = CreatorStatisticsFilter {
            min_total_coins: Some(2),
            max_total_coins: None,
            spam_skip_coins: Some(100),
            spam_dev_whitelist: vec![],
            min_median_market_cap: Some(90.0),
            min_trader_pnl_average: None,
            min_total_holders_average: Some(50.0),
            min_average_volume: None,
            min_median_total_trades: None,
            min_average_unique_buy_to_sell_ratio: None,
            min_average_buy_trader_size: None,
        };
        let mut s = sample_stats();
        s.total_holders_average = 5;
        let reasons = f.reject_reasons(&s);
        assert!(!is_early_stats_only_reject(&reasons));
    }

    #[test]
    fn spam_dev_whitelist_matches_dev_pubkey() {
        let f = CreatorStatisticsFilter {
            min_total_coins: None,
            max_total_coins: None,
            spam_skip_coins: Some(100),
            spam_dev_whitelist: vec!["CXoUJHtBGnuNR6zJ3zm8nTnNzCwXxFViXh29d1kTJQmK".into()],
            min_median_market_cap: None,
            min_trader_pnl_average: None,
            min_total_holders_average: None,
            min_average_volume: None,
            min_median_total_trades: None,
            min_average_unique_buy_to_sell_ratio: None,
            min_average_buy_trader_size: None,
        };
        assert!(f.is_spam_dev_whitelisted("CXoUJHtBGnuNR6zJ3zm8nTnNzCwXxFViXh29d1kTJQmK"));
        assert!(!f.is_spam_dev_whitelisted("OtherDev1111111111111111111111111111111"));
    }
}
