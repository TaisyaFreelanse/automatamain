//! Dev cooldown after cliff/rug exits on **our** bot trades.
//!
//! Rug spikes (`SL CRASH`, huge `tick_drop`, mcap collapse) → **permanent** ban
//! (`expires_at = 0`). Plain deep `SL` / moderate tick drop → timed cooldown only.

use crate::autobuy::filters::config::DevBlacklistConfig;

/// `expires_at` value meaning “never expires” in `dev_blacklist`.
pub const DEV_BLACKLIST_NEVER_EXPIRES: i64 = 0;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DevBlacklistTier {
    /// Rug spike — block dev forever.
    PermanentRug,
    /// Deep / choppy SL — cooldown only.
    Timed,
}

/// Short mint prefix for logs (e.g. `61aCte`).
pub fn short_mint_prefix(mint: &str) -> &str {
    if mint.len() > 6 {
        &mint[..6]
    } else {
        mint
    }
}

pub fn is_permanent_expires(expires_at: i64) -> bool {
    expires_at == DEV_BLACKLIST_NEVER_EXPIRES
}

/// Human-readable skip line for timed rows: `previous SL CRASH -78% on 61aCte`.
pub fn format_skip_detail(active_reason: &str, trigger_mint: &str) -> String {
    format!(
        "previous {} on {}",
        active_reason,
        short_mint_prefix(trigger_mint)
    )
}

/// Filter log label + detail for an active blacklist row.
pub fn format_filter_skip(active_reason: &str, trigger_mint: &str, expires_at: i64) -> (String, String) {
    if is_permanent_expires(expires_at) || active_reason.starts_with("dev_blacklist_permanent:") {
        let rug_tag = active_reason
            .strip_prefix("dev_blacklist_permanent:")
            .map(str::trim)
            .unwrap_or("rug");
        (
            "dev_blacklist_permanent".to_string(),
            format!(
                "previous {} on {}",
                rug_tag,
                short_mint_prefix(trigger_mint)
            ),
        )
    } else {
        (
            "dev_blacklist".to_string(),
            format_skip_detail(active_reason, trigger_mint),
        )
    }
}

/// PnL % for display from SOL amounts.
pub fn pnl_pct_from_sol(spent_sol: f64, pnl_sol: f64) -> f64 {
    if spent_sol > 0.0 {
        (pnl_sol / spent_sol) * 100.0
    } else {
        0.0
    }
}

/// Summary tag for timed blacklist rows (stored in `dev_blacklist.reason`).
pub fn cliff_reason_tag(close_reason: &str) -> &'static str {
    if close_reason.starts_with("SL CRASH") {
        "SL CRASH"
    } else if close_reason.starts_with("SL ") {
        "SL"
    } else {
        "cliff exit"
    }
}

fn parse_f64_after(close_reason: &str, needle: &str) -> Option<f64> {
    let idx = close_reason.find(needle)?;
    let rest = &close_reason[idx + needle.len()..];
    let end = rest
        .find(|c: char| c.is_whitespace() || c == '|' || c == '%')
        .unwrap_or(rest.len());
    rest[..end].trim().parse().ok()
}

pub fn parse_tick_drop_pct(close_reason: &str) -> Option<f64> {
    parse_f64_after(close_reason, "tick_drop=")
}

/// Drop from filtered vs raw mcap in the close reason (`filt_mcap=` / `raw_mcap=`).
pub fn parse_mcap_drop_pct(close_reason: &str) -> Option<f64> {
    let filt = parse_f64_after(close_reason, "filt_mcap=")?;
    let raw = parse_f64_after(close_reason, "raw_mcap=")?;
    if filt > 0.0 && raw < filt {
        Some((filt - raw) / filt * 100.0)
    } else {
        None
    }
}

/// Rug spike exits → permanent dev ban (not TIME KILL / MOMENTUM DECAY / mild SL).
pub fn is_permanent_rug_exit(close_reason: &str, cfg: &DevBlacklistConfig) -> bool {
    if close_reason.starts_with("SL CRASH") {
        return true;
    }
    if parse_tick_drop_pct(close_reason).is_some_and(|d| d >= cfg.permanent_min_tick_drop_pct) {
        return true;
    }
    if parse_mcap_drop_pct(close_reason).is_some_and(|d| d >= cfg.permanent_min_mcap_drop_pct) {
        return true;
    }
    false
}

/// Timed cooldown for deep plain `SL` (never for rug-tier exits).
pub fn is_timed_sl_blacklist(close_reason: &str, pnl_pct_sol: f64, cfg: &DevBlacklistConfig) -> bool {
    if is_permanent_rug_exit(close_reason, cfg) {
        return false;
    }
    if !close_reason.starts_with("SL ") {
        return false;
    }
    if pnl_pct_sol <= cfg.min_pnl_pct_for_sl {
        return true;
    }
    parse_tick_drop_pct(close_reason).is_some_and(|d| d >= cfg.min_tick_drop_pct)
}

/// Whether this closed trade should blacklist the dev, and which tier applies.
pub fn classify_dev_blacklist(
    close_reason: &str,
    pnl_pct_sol: f64,
    cfg: &DevBlacklistConfig,
) -> Option<DevBlacklistTier> {
    if !cfg.enabled {
        return None;
    }
    if is_permanent_rug_exit(close_reason, cfg) {
        return Some(DevBlacklistTier::PermanentRug);
    }
    if is_timed_sl_blacklist(close_reason, pnl_pct_sol, cfg) {
        return Some(DevBlacklistTier::Timed);
    }
    None
}

fn permanent_rug_label(close_reason: &str) -> &'static str {
    if close_reason.starts_with("SL CRASH") {
        "SL CRASH rug"
    } else if parse_tick_drop_pct(close_reason).is_some_and(|d| d >= 55.0) {
        "instant dump rug"
    } else {
        "mcap collapse rug"
    }
}

/// Stored in `dev_blacklist.reason`.
pub fn build_blacklist_reason(
    tier: DevBlacklistTier,
    close_reason: &str,
    pnl_pct_sol: f64,
) -> String {
    match tier {
        DevBlacklistTier::PermanentRug => {
            format!(
                "dev_blacklist_permanent: {}",
                permanent_rug_label(close_reason)
            )
        }
        DevBlacklistTier::Timed => {
            format!("{} {:.0}%", cliff_reason_tag(close_reason), pnl_pct_sol.round())
        }
    }
}

/// Back-compat wrapper.
pub fn should_blacklist_dev(close_reason: &str, pnl_pct_sol: f64, cfg: &DevBlacklistConfig) -> bool {
    classify_dev_blacklist(close_reason, pnl_pct_sol, cfg).is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> DevBlacklistConfig {
        DevBlacklistConfig::default()
    }

    #[test]
    fn sl_crash_is_permanent() {
        let c = cfg();
        assert_eq!(
            classify_dev_blacklist(
                "SL CRASH trigger_pnl=-75.6% | instant tick_drop=72.4%",
                -76.0,
                &c
            ),
            Some(DevBlacklistTier::PermanentRug)
        );
        assert!(is_permanent_rug_exit(
            "SL CRASH trigger_pnl=-75.6% | instant tick_drop=72.4%",
            &c
        ));
    }

    #[test]
    fn large_tick_drop_sl_is_permanent() {
        let c = cfg();
        assert_eq!(
            classify_dev_blacklist("SL trigger_pnl=-18.0% tick_drop=55.0%", -20.0, &c),
            Some(DevBlacklistTier::PermanentRug)
        );
    }

    #[test]
    fn moderate_sl_is_timed_only() {
        let c = cfg();
        assert_eq!(
            classify_dev_blacklist("SL trigger_pnl=-25.0% tick_drop=45.0%", -26.0, &c),
            Some(DevBlacklistTier::Timed)
        );
    }

    #[test]
    fn mild_sl_not_blacklisted() {
        let c = cfg();
        assert_eq!(
            classify_dev_blacklist("SL trigger_pnl=-18.0% tick_drop=10.0%", -20.0, &c),
            None
        );
    }

    #[test]
    fn time_kill_not_blacklisted() {
        let c = cfg();
        assert_eq!(classify_dev_blacklist("TIME KILL weak", -40.0, &c), None);
    }

    #[test]
    fn permanent_reason_format() {
        let r = build_blacklist_reason(
            DevBlacklistTier::PermanentRug,
            "SL CRASH tick_drop=72%",
            -76.0,
        );
        assert_eq!(r, "dev_blacklist_permanent: SL CRASH rug");
    }

    #[test]
    fn filter_skip_permanent_label() {
        let (label, detail) = format_filter_skip(
            "dev_blacklist_permanent: SL CRASH rug",
            "7XptJKHsJxL5ooppSQuKwjXVHvTX3rCuUsPtgmSBpump",
            DEV_BLACKLIST_NEVER_EXPIRES,
        );
        assert_eq!(label, "dev_blacklist_permanent");
        assert!(detail.contains("SL CRASH rug"));
        assert!(detail.contains("7XptJK"));
    }
}
