//! Short-lived mint quarantine after **mcap vs SOL** divergence exits (glitchy bonding WS).

use std::collections::HashMap;

use solana_address::Address;

use crate::autobuy::filters::config::CurveQuarantineConfig;

/// In-memory mint cooldown (survives until process restart).
#[derive(Default)]
pub struct CurveQuarantineCache {
    expires_at: HashMap<Address, u64>,
}

impl CurveQuarantineCache {
    pub fn is_active(&self, mint: &Address, now_secs: u64) -> bool {
        self.expires_at
            .get(mint)
            .is_some_and(|exp| *exp > now_secs)
    }

    pub fn insert(&mut self, mint: Address, expires_at: u64) {
        self.expires_at.insert(mint, expires_at);
    }

    pub fn prune(&mut self, now_secs: u64) {
        self.expires_at.retain(|_, exp| *exp > now_secs);
    }
}

/// Gap between median-filtered mcap PnL and realized SOL PnL on close.
pub fn mcap_sol_pnl_divergence_pct(filt_pnl_pct: f64, pnl_sol_pct: f64) -> f64 {
    filt_pnl_pct - pnl_sol_pct
}

/// After close: quarantine mint when exit looked fine on mcap tape but SOL was badly negative.
pub fn should_quarantine_mint(
    close_reason: &str,
    pnl_sol_pct: f64,
    filt_pnl_pct: f64,
    raw_pnl_pct: f64,
    cfg: &CurveQuarantineConfig,
) -> bool {
    if !cfg.enabled {
        return false;
    }
    if pnl_sol_pct > cfg.max_pnl_sol_pct {
        return false;
    }
    let div = mcap_sol_pnl_divergence_pct(filt_pnl_pct, pnl_sol_pct);
    if div >= cfg.min_mcap_sol_divergence_pct {
        return true;
    }
    if close_reason.starts_with("SL CRASH")
        && filt_pnl_pct >= cfg.sl_crash_false_positive_min_filt_pnl
        && raw_pnl_pct <= cfg.sl_crash_false_positive_max_raw_pnl
    {
        return true;
    }
    false
}

pub fn format_skip_detail(trigger_mint: &str) -> String {
    format!(
        "curve_quarantine (mcap/SOL glitch on {})",
        crate::autobuy::dev_blacklist::short_mint_prefix(trigger_mint)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> CurveQuarantineConfig {
        CurveQuarantineConfig::default()
    }

    #[test]
    fn b4lhn_pattern_quarantines() {
        assert!(should_quarantine_mint(
            "SL CRASH trigger_pnl=0.0% filt_pnl=23.3% raw_pnl=0.0%",
            -21.41,
            23.3,
            0.0,
            &cfg(),
        ));
    }

    #[test]
    fn honest_loss_not_quarantined() {
        assert!(!should_quarantine_mint(
            "SL trigger_pnl=-22.0% filt_pnl=-20.0% raw_pnl=-21.0%",
            -22.0,
            -20.0,
            -21.0,
            &cfg(),
        ));
    }
}
