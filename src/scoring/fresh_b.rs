//! Tier-B fresh-dev subtypes for stats and learning attribution.

use crate::persistence::creators::CreatorStatistics;

/// How we classified a fresh-dev Tier B candidate.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum FreshBSubtype {
    /// `count_prior_coins == 0` confirmed (e.g. Fresh Watchlist after early-stats reject).
    TrueFresh,
    /// No reliable creator stats yet (`fresh_dev_b_lane`); cannot assert the dev is new.
    Unknown,
}

impl FreshBSubtype {
    pub const TRUE_FRESH: &'static str = "B_TRUE_FRESH";
    pub const UNKNOWN: &'static str = "B_UNKNOWN";

    pub fn as_str(self) -> &'static str {
        match self {
            Self::TrueFresh => Self::TRUE_FRESH,
            Self::Unknown => Self::UNKNOWN,
        }
    }

    /// Classify from the create / watchlist entry path (before scoring).
    pub fn for_path(
        dev_stats: Option<&CreatorStatistics>,
        from_fresh_watchlist: bool,
    ) -> Option<Self> {
        if from_fresh_watchlist {
            Some(Self::TrueFresh)
        } else if dev_stats.is_none() {
            Some(Self::Unknown)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watchlist_path_is_true_fresh() {
        assert_eq!(
            FreshBSubtype::for_path(None, true),
            Some(FreshBSubtype::TrueFresh)
        );
    }

    #[test]
    fn no_history_lane_is_unknown() {
        assert_eq!(
            FreshBSubtype::for_path(None, false),
            Some(FreshBSubtype::Unknown)
        );
    }

    #[test]
    fn dev_with_stats_has_no_fresh_subtype() {
        use crate::generalize::general_commands::Currency;
        use crate::persistence::creators::CreatorStatistics;
        let s = CreatorStatistics {
            median_market_cap: Currency::from_float_native(1.0),
            trader_pnl_average: 1.0,
            total_holders_average: 1,
            average_volume: 1.0,
            median_total_trades: 1,
            average_unique_buy_to_sell_ratio: 1.0,
            average_buy_trader_size: Currency::from_float_native(1.0),
            total_coins: 1,
        };
        assert_eq!(FreshBSubtype::for_path(Some(&s), false), None);
    }
}
