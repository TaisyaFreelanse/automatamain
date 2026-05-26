//! Dev cooldown after cliff/rug exits on **our** bot trades.

use crate::autobuy::filters::config::DevBlacklistConfig;

/// Short mint prefix for logs (e.g. `61aCte`).
pub fn short_mint_prefix(mint: &str) -> &str {
    if mint.len() > 6 { &mint[..6] } else { mint }
}

/// Human-readable skip line: `previous SL CRASH -78% on 61aCte`.
pub fn format_skip_detail(active_reason: &str, trigger_mint: &str) -> String {
    format!(
        "previous {} on {}",
        active_reason,
        short_mint_prefix(trigger_mint)
    )
}

/// PnL % for display from SOL amounts.
pub fn pnl_pct_from_sol(spent_sol: f64, pnl_sol: f64) -> f64 {
    if spent_sol > 0.0 {
        (pnl_sol / spent_sol) * 100.0
    } else {
        0.0
    }
}

/// Summary tag stored in `dev_blacklist.reason` and shown on skip.
pub fn cliff_reason_tag(close_reason: &str) -> &'static str {
    if close_reason.starts_with("SL CRASH") {
        "SL CRASH"
    } else if close_reason.starts_with("SL ") {
        "SL"
    } else {
        "cliff exit"
    }
}

fn parse_tick_drop_pct(close_reason: &str) -> Option<f64> {
    let needle = "tick_drop=";
    let idx = close_reason.find(needle)?;
    let rest = &close_reason[idx + needle.len()..];
    let end = rest.find('%')?;
    rest[..end].trim().parse().ok()
}

/// Whether this **closed bot trade** should blacklist the dev.
pub fn should_blacklist_dev(close_reason: &str, pnl_pct_sol: f64, cfg: &DevBlacklistConfig) -> bool {
    if !cfg.enabled {
        return false;
    }
    if close_reason.starts_with("SL CRASH") {
        return true;
    }
    if !close_reason.starts_with("SL ") {
        return false;
    }
    if pnl_pct_sol <= cfg.min_pnl_pct_for_sl {
        return true;
    }
    parse_tick_drop_pct(close_reason).is_some_and(|d| d >= cfg.min_tick_drop_pct)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> DevBlacklistConfig {
        DevBlacklistConfig {
            enabled: true,
            cooldown_secs: 3600,
            min_pnl_pct_for_sl: -30.0,
            min_tick_drop_pct: 40.0,
        }
    }

    #[test]
    fn sl_crash_always_blacklists() {
        assert!(should_blacklist_dev(
            "SL CRASH trigger_pnl=-52.8% tick_drop=66.5%",
            -48.0,
            &cfg()
        ));
    }

    #[test]
    fn sl_deep_loss_blacklists() {
        assert!(should_blacklist_dev(
            "SL trigger_pnl=-25.4% floor=-16.0%",
            -31.0,
            &cfg()
        ));
    }

    #[test]
    fn sl_tick_drop_blacklists() {
        assert!(should_blacklist_dev(
            "SL trigger_pnl=-18.0% tick_drop=45.0%",
            -20.0,
            &cfg()
        ));
    }

    #[test]
    fn mild_sl_skips_blacklist() {
        assert!(!should_blacklist_dev(
            "SL trigger_pnl=-18.0% tick_drop=10.0%",
            -20.0,
            &cfg()
        ));
    }
}
