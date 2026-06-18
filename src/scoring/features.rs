//! Snapshots everything we know about a token at scoring time. Pure data —
//! no decisions, no I/O beyond the swarm/registry queries.

use solana_address::Address;
use tokio::sync::{mpsc, oneshot};

use crate::helper::Amount;
use crate::launchpads::{
    pump::launchpad::PumpLaunchpadCommand,
    token_bucket::TokenBucket,
};
use crate::persistence::creators::CreatorStatistics;
use crate::scoring::anti_bundle::{compute_bundle_stats, BundleStats};
use crate::scoring::config::{
    AntiParabolicConfig, ContinuationConfig, ContinuationSecondLookConfig, FeatureThresholds,
    WeakATierGateConfig,
};
use crate::scoring::dev_ranker::{DevCategory, DevRecord};
use crate::scoring::score_engine::Tier;
use crate::trading::trader::TraderType;

/// Captured at scoring-window snapshot time. Stored on the open Position so
/// we can later credit/debit dev_ranker and smart_money on close.
#[derive(Clone, Debug, Default)]
pub struct EarlyBuyersSnapshot {
    pub regulars: Vec<Address>,
    pub snipers: Vec<Address>,
}

impl EarlyBuyersSnapshot {
    pub fn all(&self) -> Vec<Address> {
        let mut out = Vec::with_capacity(self.regulars.len() + self.snipers.len());
        out.extend(self.regulars.iter().copied());
        out.extend(self.snipers.iter().copied());
        out
    }
}

#[derive(Clone, Debug)]
pub struct TokenFeatures {
    pub mint: Address,
    pub dev: Address,

    // Dev history (creator_stats from on-chain history table)
    pub dev_has_history: bool,
    pub dev_total_coins: u64,
    pub dev_pnl_avg: f64,
    pub dev_holders_avg: u64,
    pub dev_volume_avg: f64,
    pub dev_trades_avg: u64,
    pub dev_buy_to_sell_ratio: f64,

    // Dev ranking (our own past trades)
    pub dev_category: DevCategory,
    pub dev_rank_score: f64,
    pub dev_rank_record: DevRecord,

    /// Prolific serial launcher (> `spam_skip_coins`). We skip the heavy
    /// creator-stats SQL for these devs and let the token compete on tape
    /// strength, but the scorer applies `spam_dev_penalty` so only an
    /// exceptional tape survives.
    pub is_spam_dev: bool,

    // Pool / market state
    pub current_mcap_sol: f64,
    pub initial_mcap_sol: f64,
    /// Max mcap seen across scoring-window tape samples (for peak momentum).
    pub peak_mcap_sol: f64,

    // Early buyers (the snapshot itself)
    pub buyers: EarlyBuyersSnapshot,
    pub regular_buyer_count: u64,
    pub sniper_count: u64,
    pub buy_volume_sol: f64,
    pub still_long_count: u64,
    pub already_sold_count: u64,
    pub buy_to_sell_ratio: f64,

    // Anti-bundle
    pub bundle: BundleStats,

    // Smart money
    pub smart_wallet_count: u32,

    // --- Early-window tape (buyer velocity + sell pressure / absorb) ------
    /// New unique buyers per scoring sub-slice (len = slices-1 when slices>=2).
    pub buyer_velocity_new_per_slice: Vec<u64>,
    /// 0 = strong decay, ~0.5 neutral, 1 = strictly increasing new-buyer cadence.
    pub buyer_velocity_persistence: f64,
    /// 0..1 normalized early sell intensity vs mcap.
    pub sell_pressure_score: f64,
    /// 0..1 mcap absorption quality after sells (higher = tape held bid).
    pub absorb_quality_score: f64,
    pub sell_events_window: u64,
    pub sell_volume_window_sol: f64,
    /// Slices with clustered sell events + meaningful SOL (distribution pockets).
    pub repeat_dump_slices: u32,
    /// Smart-registry wallets among early buyers that already fully sold during the window.
    pub smart_wallet_early_exits: u32,
}

impl TokenFeatures {
    pub fn buyer_count(&self) -> u64 {
        self.regular_buyer_count + self.sniper_count
    }
}

/// One sample of the early bonding-curve tape (buyers + sells + mcap).
#[derive(Clone, Debug, Default)]
pub struct EarlyTapePoint {
    pub buyer_count: u64,
    pub still_long: u64,
    pub already_sold: u64,
    pub buy_volume_sol: f64,
    pub cum_sell_raw: u64,
    pub cum_sell_events: u64,
    pub mcap_sol: f64,
}

/// Aggregated metrics from `observe_early_tape_points` + smart-wallet exit pass.
#[derive(Clone, Debug, Default)]
pub struct ScoringTapeDerived {
    pub buyer_velocity_new_per_slice: Vec<u64>,
    pub buyer_velocity_persistence: f64,
    pub sell_pressure_score: f64,
    pub absorb_quality_score: f64,
    pub sell_events_window: u64,
    pub sell_volume_window_sol: f64,
    pub repeat_dump_slices: u32,
    pub smart_wallet_early_exits: u32,
}

fn lamports_to_sol(raw: u64) -> f64 {
    Amount::from_raw_native(raw).to_float()
}

/// Snapshot counts + cumulative sells for Regular+Sniper participants.
pub async fn snapshot_early_tape(bucket: &TokenBucket) -> EarlyTapePoint {
    let regulars = bucket.swarm().get_traders_by_type(TraderType::Regular).await;
    let snipers = bucket.swarm().get_traders_by_type(TraderType::Sniper).await;

    let mut still_long: u64 = 0;
    let mut already_sold: u64 = 0;
    let mut buy_volume_sol: f64 = 0.0;
    let mut cum_sell_raw: u64 = 0;
    let mut cum_sell_events: u64 = 0;

    let mut scan = |list: &[(Address, crate::trading::trader::Trader)]| {
        for (_addr, trader) in list {
            let spent = trader.total_spent().to_float();
            buy_volume_sol += spent;
            if trader.holdings().raw() > 0 {
                still_long += 1;
            } else if spent > 0.0 {
                already_sold += 1;
            }
            cum_sell_raw = cum_sell_raw.saturating_add(trader.sell_proceeds_raw());
            cum_sell_events += u64::from(trader.sell_event_count());
        }
    };
    scan(&regulars);
    scan(&snipers);

    let buyer_count = regulars.len() as u64 + snipers.len() as u64;
    let mcap_sol = bucket.pool().market_cap().amount().to_float();

    EarlyTapePoint {
        buyer_count,
        still_long,
        already_sold,
        buy_volume_sol,
        cum_sell_raw,
        cum_sell_events,
        mcap_sol,
    }
}

/// Peak mcap % vs window start (used for `momentum_good` / early exit).
pub fn momentum_peak_pct(initial_mcap_sol: f64, peak_mcap_sol: f64) -> f64 {
    if initial_mcap_sol > 0.0 {
        (peak_mcap_sol / initial_mcap_sol - 1.0) * 100.0
    } else {
        0.0
    }
}

/// Result of the live early-tape observer (may end before full `window_ms`).
#[derive(Clone)]
pub struct EarlyTapeObserveResult {
    pub points: Vec<EarlyTapePoint>,
    pub initial_mcap_sol: f64,
    pub peak_mcap_sol: f64,
    pub current_mcap_sol: f64,
    pub exited_early: bool,
    /// Last launchpad bucket fetched during the observe window (avoids a duplicate fetch).
    pub last_bucket: Option<TokenBucket>,
}

/// Live bucket from launchpad storage (pool state includes trades since create).
pub async fn fetch_live_bucket(
    launchpad: &mpsc::Sender<PumpLaunchpadCommand>,
    mint: Address,
) -> Option<TokenBucket> {
    let (tx, rx) = oneshot::channel();
    launchpad
        .send(PumpLaunchpadCommand::GetBucket {
            mint,
            respond_to: tx,
        })
        .await
        .ok()?;
    rx.await.ok()
}

fn finalize_tape_observe(
    points: Vec<EarlyTapePoint>,
    exited_early: bool,
    last_bucket: Option<TokenBucket>,
) -> EarlyTapeObserveResult {
    let initial_mcap_sol = points.first().map(|p| p.mcap_sol).unwrap_or(0.0);
    let peak_mcap_sol = points
        .iter()
        .map(|p| p.mcap_sol)
        .fold(initial_mcap_sol, f64::max);
    let current_mcap_sol = points.last().map(|p| p.mcap_sol).unwrap_or(peak_mcap_sol);
    EarlyTapeObserveResult {
        points,
        initial_mcap_sol,
        peak_mcap_sol,
        current_mcap_sol,
        exited_early,
        last_bucket,
    }
}

fn tape_hit_momentum_low(initial: f64, peak: f64, low_pct: f64) -> bool {
    initial > 0.0 && momentum_peak_pct(initial, peak) >= low_pct
}

/// Like `observe_early_tape_points`, but each sample uses a fresh bucket from
/// launchpad storage so `mcap_sol` tracks bonding-curve trades during the window.
///
/// When `early_exit_momentum_low_pct` is set, stops sleeping as soon as peak mcap
/// vs the first sample reaches that % (fast pump path).
pub async fn observe_early_tape_points_live(
    launchpad: &mpsc::Sender<PumpLaunchpadCommand>,
    mint: Address,
    window_ms: u64,
    slices: usize,
    early_exit_momentum_low_pct: Option<f64>,
) -> EarlyTapeObserveResult {
    let s = slices.max(1) as u64;
    let window_ms = window_ms.max(1);
    if s <= 1 {
        tokio::time::sleep(std::time::Duration::from_millis(window_ms)).await;
        let (points, last_bucket) = if let Some(bucket) = fetch_live_bucket(launchpad, mint).await {
            (vec![snapshot_early_tape(&bucket).await], Some(bucket))
        } else {
            (Vec::new(), None)
        };
        return finalize_tape_observe(points, false, last_bucket);
    }

    let slice_ms = (window_ms / s).max(1);
    let mut out: Vec<EarlyTapePoint> = Vec::with_capacity(s as usize);
    let mut initial_mcap_sol = 0.0_f64;
    let mut peak_mcap_sol = 0.0_f64;
    let mut last_bucket: Option<TokenBucket> = None;

    for i in 0..s {
        if i > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(slice_ms)).await;
        }
        let Some(bucket) = fetch_live_bucket(launchpad, mint).await else {
            continue;
        };
        last_bucket = Some(bucket.clone());
        let point = snapshot_early_tape(&bucket).await;
        if out.is_empty() {
            initial_mcap_sol = point.mcap_sol;
            peak_mcap_sol = point.mcap_sol;
        } else {
            peak_mcap_sol = peak_mcap_sol.max(point.mcap_sol);
        }
        out.push(point);

        if let Some(low) = early_exit_momentum_low_pct {
            if tape_hit_momentum_low(initial_mcap_sol, peak_mcap_sol, low) {
                return finalize_tape_observe(out, true, last_bucket);
            }
        }
    }

    let used = slice_ms.saturating_mul(s.saturating_sub(1));
    if window_ms > used {
        tokio::time::sleep(std::time::Duration::from_millis(window_ms - used)).await;
        if let Some(bucket) = fetch_live_bucket(launchpad, mint).await {
            last_bucket = Some(bucket.clone());
            let point = snapshot_early_tape(&bucket).await;
            if out.is_empty() {
                return finalize_tape_observe(vec![point], false, last_bucket);
            }
            if let Some(last) = out.last_mut() {
                *last = point;
            }
        }
    }

    let mut result = finalize_tape_observe(out, false, last_bucket);
    if let Some(low) = early_exit_momentum_low_pct {
        if tape_hit_momentum_low(result.initial_mcap_sol, result.peak_mcap_sol, low) {
            result.exited_early = true;
        }
    }
    result
}

/// Continuation Validation Layer (doc 2.1 / 2.2 / 2.3). Pure decision over a
/// freshly observed confirmation tape, given the scoring-window baseline.
/// Returns `Ok(())` to proceed with the buy, or `Err(reason)` to skip.
///
/// `baseline_b2s` / `baseline_buyer_count` come from the scoring window.
/// `confirm` is the tape sampled during the confirmation window (chronological).
pub fn evaluate_continuation(
    cfg: &ContinuationConfig,
    baseline_b2s: f64,
    baseline_buyer_count: u64,
    confirm: &[EarlyTapePoint],
    window_ms: u64,
    is_a_plus: bool,
) -> Result<(), &'static str> {
    // Not enough samples to judge continuation: fail safe (skip).
    if confirm.len() < 2 {
        return Err("cont_no_data");
    }
    let first = &confirm[0];
    let last = confirm.last().unwrap();

    // Net mcap direction across the confirm window (== mcap_now vs mcap_init in
    // the [BUY] skip log). A window that closes flat-or-up is healthy even when
    // the discrete per-slice upticks are sparse: many real runners pause/dip for
    // a slice before continuing, and the short 1.5s window would otherwise cut
    // them as "no uptick".
    let mcap_net_nonneg = last.mcap_sol >= first.mcap_sol;
    let mcap_rising = last.mcap_sol > first.mcap_sol;

    // --- price upticks across confirm slices (doc 2.1) ---
    let mut upticks: u32 = 0;
    for w in confirm.windows(2) {
        if w[1].mcap_sol > w[0].mcap_sol {
            upticks += 1;
        }
    }
    // Only treat as dead momentum when upticks are sparse AND the window closed
    // below where it opened. Flat-or-up windows pass regardless of uptick count.
    if upticks < cfg.min_upticks && !mcap_net_nonneg {
        return Err("cont_no_uptick");
    }

    // --- new unique buyers during the window (doc 2.3) ---
    let new_buyers = last.buyer_count.saturating_sub(baseline_buyer_count);
    if new_buyers < cfg.min_new_unique_buyers {
        return Err("cont_no_new_buyers");
    }

    // --- sustained buys/sec (doc 2.2) ---
    if cfg.min_buys_per_sec > 0.0 {
        let secs = (window_ms.max(1) as f64) / 1000.0;
        let buys_per_sec = new_buyers as f64 / secs;
        if buys_per_sec < cfg.min_buys_per_sec {
            return Err("cont_low_buys_per_sec");
        }
    }

    // --- buy/sell deltas across the confirm window ---
    let buy_delta = (last.buy_volume_sol - first.buy_volume_sol).max(0.0);
    let sell_delta_sol = lamports_to_sol(last.cum_sell_raw.saturating_sub(first.cum_sell_raw));

    // --- sell absorption: sells overwhelming buys in-window (doc 2.1 / 2.2) ---
    if buy_delta > 0.0 {
        let absorption = sell_delta_sol / buy_delta;
        if absorption > cfg.max_sell_absorption_ratio {
            return Err("cont_sell_absorption");
        }
    }

    // --- buy/sell ratio must not worsen vs scoring baseline (doc 2.1) ---
    if baseline_b2s > 0.0 {
        let confirm_b2s = if sell_delta_sol > 0.0 {
            buy_delta / sell_delta_sol
        } else if buy_delta > 0.0 {
            // no sells in-window: ratio is at least as healthy as baseline.
            baseline_b2s
        } else {
            0.0
        };
        if confirm_b2s < baseline_b2s * cfg.max_b2s_drop_ratio {
            // Soften for strong (A+) setups whose mcap is still rising in-window:
            // a temporary b2s dip on a climbing A+ runner is healthy churn, not a
            // fade. Lower tiers (and flat/declining A+) still get cut here.
            if !(is_a_plus && mcap_rising) {
                return Err("cont_b2s_worsening");
            }
        }
    }

    Ok(())
}

/// A+ at high mcap, no smart wallets, mcap at/near local peak (7XptJK-style top-buy risk).
pub fn aplus_peak_no_smart_guard(
    cfg: &crate::scoring::config::ContinuationAplusPeakGuardConfig,
    is_a_plus: bool,
    smart_count: u32,
    current_mcap: f64,
    peak_mcap: f64,
) -> bool {
    if !cfg.enabled || !is_a_plus || smart_count > 0 {
        return false;
    }
    if peak_mcap < cfg.min_mcap_sol || current_mcap < cfg.min_mcap_sol {
        return false;
    }
    peak_mcap > 0.0 && current_mcap >= peak_mcap * cfg.near_peak_ratio
}

/// After A+ peak-guard defer/recheck, block buy when tape collapsed vs score-time peak.
pub fn aplus_peak_recheck_mcap_acceptable(
    cfg: &crate::scoring::config::ContinuationAplusPeakGuardConfig,
    score_peak_mcap: f64,
    score_current_mcap: f64,
    recheck_mcap: f64,
) -> bool {
    if !cfg.enabled || recheck_mcap <= 0.0 {
        return recheck_mcap > 0.0;
    }
    let anchor = score_peak_mcap.max(score_current_mcap);
    if anchor <= 0.0 {
        return true;
    }
    recheck_mcap >= anchor * cfg.recheck_min_vs_peak_ratio
}

/// Strong enough first confirm to skip the deferred A+ peak re-check.
pub fn continuation_confirm_strong(
    cfg: &crate::scoring::config::ContinuationAplusPeakGuardConfig,
    confirm: &[EarlyTapePoint],
    baseline_buyer_count: u64,
) -> bool {
    if confirm.len() < 2 {
        return false;
    }
    let first = &confirm[0];
    let last = confirm.last().unwrap();
    if last.mcap_sol < first.mcap_sol {
        return false;
    }
    let (upticks, new_buyers) = continuation_strength(confirm, baseline_buyer_count);
    upticks >= cfg.strong_upticks && new_buyers >= cfg.strong_new_buyers
}

fn score_items_has(name: &str, items: &[(&'static str, i32)]) -> bool {
    items.iter().any(|(n, _)| *n == name)
}

/// Weak tier-A profile: low score, no smart wallets, neutral dev ranker and/or weak dev history.
pub fn weak_a_profile_match(
    cfg: &WeakATierGateConfig,
    tier: Tier,
    score: i32,
    smart_wallet_count: u32,
    dev_category: DevCategory,
    items: &[(&'static str, i32)],
) -> bool {
    if !cfg.enabled || tier != Tier::A {
        return false;
    }
    if score > cfg.max_score || smart_wallet_count > cfg.max_smart_wallets {
        return false;
    }
    let dev_weak = (cfg.block_dev_neutral
        && matches!(dev_category, DevCategory::Neutral | DevCategory::Stale))
        || (cfg.block_dev_history_weak && score_items_has("dev_history_weak", items));
    dev_weak
}

/// Minimum peak mcap velocity (%) for tier-A live buys (export: weak A setups cluster 0–5%).
pub const TIER_A_MIN_VELOCITY_PCT: f64 = 3.0;

/// Tier A with near-flat momentum in the scoring window.
pub fn tier_a_low_velocity_skip_reason(tier: Tier, velocity_pct: f64) -> Option<&'static str> {
    if tier == Tier::A && velocity_pct < TIER_A_MIN_VELOCITY_PCT {
        Some("low_velocity_a_setup")
    } else {
        None
    }
}

/// Live A+ entry gates (serial rug pattern).
pub fn aplus_rug_gate_skip_reason(
    tier: Tier,
    smart_wallet_count: u32,
    buy_to_sell_ratio: f64,
) -> Option<&'static str> {
    if tier != Tier::APlus {
        return None;
    }
    if smart_wallet_count < 1 {
        return Some("aplus_no_smart_required");
    }
    // Belt-and-suspenders if min-smart is ever relaxed in config.
    if smart_wallet_count == 0 && buy_to_sell_ratio > 10.0 {
        return Some("aplus_no_smart_fake_b2s");
    }
    None
}

/// Hard skip: tier-A weak dev history, no smart, inflated b2s, no sell flow (synthetic pump).
pub fn weak_a_synthetic_pump_skip_reason(
    cfg: &crate::scoring::config::WeakASyntheticPumpConfig,
    tier: Tier,
    smart_wallet_count: u32,
    f: &TokenFeatures,
    items: &[(&'static str, i32)],
) -> Option<&'static str> {
    if !cfg.enabled || tier != Tier::A {
        return None;
    }
    if smart_wallet_count != 0 {
        return None;
    }
    if !score_items_has("dev_history_weak", items) {
        return None;
    }
    if f.buy_to_sell_ratio <= cfg.buy_to_sell_ratio_gt {
        return None;
    }
    if f.sell_volume_window_sol >= cfg.sell_volume_window_sol_lt {
        return None;
    }
    Some("weak_a_synthetic_pump")
}

/// Hard skip before continuation poll: dump slice and/or low buy volume on weak A.
pub fn weak_a_hard_skip_reason(
    cfg: &WeakATierGateConfig,
    tier: Tier,
    score: i32,
    smart_wallet_count: u32,
    f: &TokenFeatures,
    items: &[(&'static str, i32)],
) -> Option<&'static str> {
    if !weak_a_profile_match(cfg, tier, score, smart_wallet_count, f.dev_category, items) {
        return None;
    }
    if f.repeat_dump_slices >= cfg.block_repeat_dump_slices_ge {
        return Some("weak_a_dump_in_tape");
    }
    if f.buy_volume_sol < cfg.min_buy_volume_sol {
        return Some("weak_a_low_volume");
    }
    None
}

/// Whether a failed first confirm may defer skip for a second-look poll.
pub fn continuation_second_look_eligible(
    cfg: &crate::scoring::config::ContinuationSecondLookConfig,
    is_a_plus: bool,
    score: i32,
    first_fail_reason: &str,
) -> bool {
    if !cfg.enabled || !(is_a_plus || score >= cfg.min_score) {
        return false;
    }
    match first_fail_reason {
        "cont_b2s_worsening" | "cont_sell_absorption" => true,
        // A+ dip-then-rip: short confirm dip before continuation (4fqNB5-style).
        "cont_no_uptick" => is_a_plus,
        _ => false,
    }
}

/// Strong setups use `continuation_second_look_eligible`; fragile weak A also defers on any fail.
pub fn continuation_second_look_eligible_for_buy(
    sl_cfg: &ContinuationSecondLookConfig,
    weak_cfg: &WeakATierGateConfig,
    tier: Tier,
    is_a_plus: bool,
    score: i32,
    smart_wallet_count: u32,
    f: &TokenFeatures,
    items: &[(&'static str, i32)],
    first_fail_reason: &str,
) -> bool {
    if continuation_second_look_eligible(sl_cfg, is_a_plus, score, first_fail_reason) {
        return true;
    }
    if !sl_cfg.enabled || !weak_cfg.enabled || tier != Tier::A {
        return false;
    }
    if !weak_a_profile_match(weak_cfg, tier, score, smart_wallet_count, f.dev_category, items) {
        return false;
    }
    score <= weak_cfg.continuation_second_look_max_score
}

/// After `wait_ms`, re-poll the tape. Skip if mcap keeps falling with no demand;
/// pass if mcap recovers, an uptick appears, or new unique buyers show up.
pub fn evaluate_continuation_second_look(
    ref_mcap_sol: f64,
    baseline_buyer_count: u64,
    recheck: &[EarlyTapePoint],
) -> Result<(), &'static str> {
    if recheck.len() < 2 {
        return Err("cont_second_look_no_data");
    }
    let first = &recheck[0];
    let last = recheck.last().unwrap();
    let (upticks, new_buyers) = continuation_strength(recheck, baseline_buyer_count);

    let mcap_below_ref = last.mcap_sol < ref_mcap_sol;
    let mcap_falling_in_window = last.mcap_sol < first.mcap_sol;
    if mcap_below_ref && mcap_falling_in_window && upticks == 0 && new_buyers == 0 {
        return Err("cont_second_look_mcap_fall");
    }

    let recovery = last.mcap_sol > ref_mcap_sol || upticks >= 1 || new_buyers >= 1;
    if recovery {
        Ok(())
    } else {
        Err("cont_second_look_no_recovery")
    }
}

/// Strength of the confirmation poll: `(upticks, new_unique_buyers)`.
/// `upticks` = number of confirm slices where mcap rose vs the previous slice;
/// `new_buyers` = unique buyers gained from the scoring baseline to window end.
pub fn continuation_strength(confirm: &[EarlyTapePoint], baseline_buyer_count: u64) -> (u32, u64) {
    if confirm.len() < 2 {
        return (0, 0);
    }
    let mut upticks: u32 = 0;
    for w in confirm.windows(2) {
        if w[1].mcap_sol > w[0].mcap_sol {
            upticks += 1;
        }
    }
    let new_buyers = confirm
        .last()
        .map(|p| p.buyer_count.saturating_sub(baseline_buyer_count))
        .unwrap_or(0);
    (upticks, new_buyers)
}

/// Anti-parabolic "bought the local top" suspect (doc: entry-on-peak fix).
/// True only for weak A-tier entries with no smart money entered while mcap is
/// at/near the local peak. A+ tier, any smart money, or a score above
/// `weak_score_max` are never flagged (returns false). Caller still gives the
/// entry a reprieve if the continuation poll shows strong fresh demand.
pub fn parabolic_peak_suspect(
    cfg: &AntiParabolicConfig,
    is_a_plus: bool,
    score: i32,
    smart_count: u32,
    current_mcap: f64,
    peak_mcap: f64,
) -> bool {
    if !cfg.enabled {
        return false;
    }
    if is_a_plus || smart_count > 0 || score > cfg.weak_score_max {
        return false;
    }
    if peak_mcap <= 0.0 {
        return false;
    }
    current_mcap >= peak_mcap * cfg.near_peak_ratio
}

/// `slices` evenly spaced samples across `window_ms` (last point refreshed at window end).
pub async fn observe_early_tape_points(
    bucket: &TokenBucket,
    window_ms: u64,
    slices: usize,
) -> Vec<EarlyTapePoint> {
    let s = slices.max(1) as u64;
    let window_ms = window_ms.max(1);
    if s <= 1 {
        tokio::time::sleep(std::time::Duration::from_millis(window_ms)).await;
        return vec![snapshot_early_tape(bucket).await];
    }

    let slice_ms = (window_ms / s).max(1);
    let mut out: Vec<EarlyTapePoint> = Vec::with_capacity(s as usize);
    for i in 0..s {
        if i > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(slice_ms)).await;
        }
        out.push(snapshot_early_tape(bucket).await);
    }
    let used = slice_ms.saturating_mul(s.saturating_sub(1));
    if window_ms > used {
        tokio::time::sleep(std::time::Duration::from_millis(window_ms - used)).await;
        if let Some(last) = out.last_mut() {
            *last = snapshot_early_tape(bucket).await;
        }
    }
    out
}

fn buyer_persistence_from_deltas(d: &[u64]) -> f64 {
    if d.is_empty() {
        return 0.5;
    }
    if d.len() == 1 {
        return if d[0] > 0 { 0.62 } else { 0.42 };
    }
    let strict_inc = d.windows(2).all(|w| w[1] > w[0]);
    let non_dec = d.windows(2).all(|w| w[1] + 1 >= w[0]);
    let strong_dec = d.windows(2).all(|w| w[1] < w[0]);
    if strict_inc {
        1.0
    } else if non_dec {
        0.72
    } else if strong_dec {
        0.05
    } else {
        0.38
    }
}

impl ScoringTapeDerived {
    pub fn from_tape_points(points: &[EarlyTapePoint], smart_wallet_early_exits: u32) -> Self {
        if points.is_empty() {
            return Self::default();
        }
        let first = &points[0];
        let last = points.last().unwrap();

        let mut new_per_slice: Vec<u64> = Vec::new();
        for i in 1..points.len() {
            new_per_slice.push(
                points[i]
                    .buyer_count
                    .saturating_sub(points[i - 1].buyer_count),
            );
        }
        let persistence = buyer_persistence_from_deltas(&new_per_slice);

        let sell_vol_sol = lamports_to_sol(last.cum_sell_raw.saturating_sub(first.cum_sell_raw));
        let sell_events_window = last.cum_sell_events.saturating_sub(first.cum_sell_events);

        let mcap_ref = last.mcap_sol.max(1.0);
        let sell_pressure_score = ((sell_vol_sol / mcap_ref).min(3.0) / 3.0).clamp(0.0, 1.0);

        let mut acc = 0.0f64;
        let mut wsum = 0.0f64;
        for i in 1..points.len() {
            let d_lam = points[i]
                .cum_sell_raw
                .saturating_sub(points[i - 1].cum_sell_raw);
            let d_sol = lamports_to_sol(d_lam);
            if d_sol < 1e-9 {
                continue;
            }
            let dm = points[i].mcap_sol - points[i - 1].mcap_sol;
            let ratio = dm / d_sol;
            let w = d_sol.min(2.0);
            acc += ratio * w;
            wsum += w;
        }
        let absorb_raw = if wsum > 0.0 { acc / wsum } else { 0.0 };
        let absorb_quality_score = ((absorb_raw.tanh() + 1.0) * 0.5).clamp(0.0, 1.0);

        let mut repeat_dump_slices: u32 = 0;
        for i in 1..points.len() {
            let ds = points[i]
                .cum_sell_events
                .saturating_sub(points[i - 1].cum_sell_events);
            let d_lam = points[i]
                .cum_sell_raw
                .saturating_sub(points[i - 1].cum_sell_raw);
            let d_sol = lamports_to_sol(d_lam);
            let m_mid = (points[i].mcap_sol + points[i - 1].mcap_sol) * 0.5;
            if ds >= 2 && d_sol > m_mid * 0.008 {
                repeat_dump_slices += 1;
            }
        }

        Self {
            buyer_velocity_new_per_slice: new_per_slice,
            buyer_velocity_persistence: persistence,
            sell_pressure_score,
            absorb_quality_score,
            sell_events_window,
            sell_volume_window_sol: sell_vol_sol,
            repeat_dump_slices,
            smart_wallet_early_exits,
        }
    }
}

/// Sweeps the swarm for buyer information. Returns counts, list of wallets,
/// volume in SOL, and per-wallet aggregated buy sizes for bundle analysis.
pub async fn snapshot_early_buyers(
    bucket: &TokenBucket,
    thresholds: &FeatureThresholds,
) -> (
    EarlyBuyersSnapshot,
    Vec<f64>,
    f64,
    u64,
    u64,
    BundleStats,
) {
    let regulars = bucket.swarm().get_traders_by_type(TraderType::Regular).await;
    let snipers = bucket.swarm().get_traders_by_type(TraderType::Sniper).await;

    let mut buy_sizes_sol: Vec<f64> = Vec::with_capacity(regulars.len() + snipers.len());
    let mut still_long: u64 = 0;
    let mut already_sold: u64 = 0;
    let mut total_volume_sol: f64 = 0.0;

    let push = |list: &[(Address, crate::trading::trader::Trader)],
                buy_sizes_sol: &mut Vec<f64>,
                still_long: &mut u64,
                already_sold: &mut u64,
                total_volume_sol: &mut f64| {
        for (_addr, trader) in list {
            let spent = trader.total_spent().to_float();
            *total_volume_sol += spent;
            if spent > 0.0 {
                buy_sizes_sol.push(spent);
            }
            if trader.holdings().raw() > 0 {
                *still_long += 1;
            } else if spent > 0.0 {
                *already_sold += 1;
            }
        }
    };
    push(
        &regulars,
        &mut buy_sizes_sol,
        &mut still_long,
        &mut already_sold,
        &mut total_volume_sol,
    );
    push(
        &snipers,
        &mut buy_sizes_sol,
        &mut still_long,
        &mut already_sold,
        &mut total_volume_sol,
    );

    let bundle = compute_bundle_stats(&buy_sizes_sol, thresholds.bundle_similar_tolerance);

    let snapshot = EarlyBuyersSnapshot {
        regulars: regulars.iter().map(|(a, _)| *a).collect(),
        snipers: snipers.iter().map(|(a, _)| *a).collect(),
    };

    (
        snapshot,
        buy_sizes_sol,
        total_volume_sol,
        still_long,
        already_sold,
        bundle,
    )
}

/// Build the full feature vector. `initial_mcap_sol` is the market cap at
/// the moment of `Action::Create` (before the scoring window).
#[allow(clippy::too_many_arguments)]
pub fn assemble(
    mint: Address,
    dev: Address,
    dev_stats: Option<&CreatorStatistics>,
    dev_category: DevCategory,
    dev_rank_record: DevRecord,
    initial_mcap_sol: f64,
    current_mcap_sol: f64,
    peak_mcap_sol: f64,
    buyers: EarlyBuyersSnapshot,
    regular_buyer_count: u64,
    sniper_count: u64,
    buy_volume_sol: f64,
    still_long_count: u64,
    already_sold_count: u64,
    bundle: BundleStats,
    smart_wallet_count: u32,
    tape: ScoringTapeDerived,
) -> TokenFeatures {
    let dev_has_history = dev_stats.is_some();
    let dev_total_coins = dev_stats.map(|s| s.total_coins).unwrap_or(0);
    let dev_pnl_avg = dev_stats.map(|s| s.trader_pnl_average).unwrap_or(0.0);
    let dev_holders_avg = dev_stats.map(|s| s.total_holders_average).unwrap_or(0);
    let dev_volume_avg = dev_stats.map(|s| s.average_volume).unwrap_or(0.0);
    let dev_trades_avg = dev_stats.map(|s| s.median_total_trades).unwrap_or(0);
    let dev_buy_to_sell_ratio = dev_stats
        .map(|s| s.average_unique_buy_to_sell_ratio)
        .unwrap_or(0.0);

    let buy_to_sell_ratio = if already_sold_count == 0 {
        if still_long_count > 0 {
            still_long_count as f64
        } else {
            0.0
        }
    } else {
        still_long_count as f64 / already_sold_count as f64
    };

    TokenFeatures {
        mint,
        dev,
        dev_has_history,
        dev_total_coins,
        dev_pnl_avg,
        dev_holders_avg,
        dev_volume_avg,
        dev_trades_avg,
        dev_buy_to_sell_ratio,
        dev_category,
        dev_rank_score: dev_rank_record.score,
        dev_rank_record,
        // Set by the caller after assembly; the feature builder has no view of
        // the spam-dev gate.
        is_spam_dev: false,
        current_mcap_sol,
        initial_mcap_sol,
        peak_mcap_sol,
        buyers,
        regular_buyer_count,
        sniper_count,
        buy_volume_sol,
        still_long_count,
        already_sold_count,
        buy_to_sell_ratio,
        bundle,
        smart_wallet_count,
        buyer_velocity_new_per_slice: tape.buyer_velocity_new_per_slice,
        buyer_velocity_persistence: tape.buyer_velocity_persistence,
        sell_pressure_score: tape.sell_pressure_score,
        absorb_quality_score: tape.absorb_quality_score,
        sell_events_window: tape.sell_events_window,
        sell_volume_window_sol: tape.sell_volume_window_sol,
        repeat_dump_slices: tape.repeat_dump_slices,
        smart_wallet_early_exits: tape.smart_wallet_early_exits,
    }
}

/// Tier B lane: `dev_cat = Fresh` only (includes no-history fresh path in main).
pub fn tier_b_dev_eligible(
    f: &TokenFeatures,
    cfg: &crate::scoring::config::TierBGateConfig,
) -> bool {
    if !cfg.enabled || f.is_spam_dev {
        return false;
    }
    f.dev_category == DevCategory::Fresh
}

/// Tier B entry gates (in addition to all live protections).
pub fn tier_b_base_gates_ok(
    cfg: &crate::scoring::config::TierBGateConfig,
    f: &TokenFeatures,
) -> bool {
    cfg.enabled
        && f.smart_wallet_count >= cfg.min_smart_wallets
        && f.buyer_count() >= cfg.min_buyers
        && f.buy_volume_sol >= cfg.min_buy_volume_sol
}

pub fn has_momentum_good(items: &[(&'static str, i32)]) -> bool {
    items.iter().any(|(name, _)| *name == "momentum_good")
}

pub fn has_momentum_overheated(items: &[(&'static str, i32)]) -> bool {
    items.iter().any(|(name, _)| *name == "momentum_overheated")
}

pub fn tier_b_velocity_ok(
    cfg: &crate::scoring::config::TierBGateConfig,
    f: &TokenFeatures,
) -> bool {
    fresh_b_velocity_pct(f) >= cfg.min_velocity_pct
}

/// Peak mcap % vs scoring-window start (same as score engine momentum).
pub fn fresh_b_velocity_pct(f: &TokenFeatures) -> f64 {
    let peak = f.peak_mcap_sol.max(f.current_mcap_sol);
    momentum_peak_pct(f.initial_mcap_sol, peak)
}

/// Hot-fresh override: exceptional fresh impulse when only momentum gate fails.
pub fn fresh_b_hot_override_ok(
    cfg: &crate::scoring::config::TierBGateConfig,
    f: &TokenFeatures,
    items: &[(&'static str, i32)],
) -> bool {
    let ho = &cfg.hot_fresh_override;
    if !ho.enabled || !is_momentum_only_tier_b_fail(cfg, f, items) {
        return false;
    }
    let vel = fresh_b_velocity_pct(f);
    f.buyer_count() >= ho.min_buyers
        && f.buy_volume_sol >= ho.min_buy_volume_sol
        && vel >= ho.min_velocity_pct
}

/// Standard B gates pass except `momentum_good` (overheated or above band).
pub fn is_momentum_only_tier_b_fail(
    cfg: &crate::scoring::config::TierBGateConfig,
    f: &TokenFeatures,
    items: &[(&'static str, i32)],
) -> bool {
    tier_b_dev_eligible(f, cfg)
        && tier_b_base_gates_ok(cfg, f)
        && tier_b_velocity_ok(cfg, f)
        && !has_momentum_good(items)
}

/// Tier A / A+: only `momentum_overheated` blocks the live `require_momentum_good` gate.
pub fn is_momentum_only_aa_overheated(items: &[(&'static str, i32)]) -> bool {
    has_momentum_overheated(items) && !has_momentum_good(items)
}

/// Whether tier A or A+ may bypass `require_momentum_good` for overheated momentum only.
pub fn aa_momentum_override_for_tier(
    cfg: &crate::scoring::config::AAMomentumOverrideConfig,
    tier: crate::scoring::score_engine::Tier,
    items: &[(&'static str, i32)],
) -> bool {
    use crate::scoring::score_engine::Tier;
    if !cfg.enabled || !is_momentum_only_aa_overheated(items) {
        return false;
    }
    match tier {
        Tier::A => true,
        Tier::APlus => true,
        _ => false,
    }
}

/// Result of a tier-B poll (watchlist or scoring).
#[derive(Clone, Debug)]
pub struct TierBGatePoll {
    pub entry_ok: bool,
    pub hot_override: bool,
    pub momentum_only_fail: bool,
    pub velocity_pct: f64,
    pub buyers: u64,
    pub buy_volume_sol: f64,
    pub momentum_overheated: bool,
}

pub fn evaluate_tier_b_poll(
    cfg: &crate::scoring::config::TierBGateConfig,
    f: &TokenFeatures,
    items: &[(&'static str, i32)],
) -> TierBGatePoll {
    let entry_ok = tier_b_entry_ok(cfg, f, items);
    let hot_override = !entry_ok && fresh_b_hot_override_ok(cfg, f, items);
    TierBGatePoll {
        entry_ok,
        hot_override,
        momentum_only_fail: is_momentum_only_tier_b_fail(cfg, f, items),
        velocity_pct: fresh_b_velocity_pct(f),
        buyers: f.buyer_count(),
        buy_volume_sol: f.buy_volume_sol,
        momentum_overheated: has_momentum_overheated(items),
    }
}

pub fn tier_b_poll_passes(poll: &TierBGatePoll) -> bool {
    poll.entry_ok || poll.hot_override
}

/// Tier B entry gates (in addition to all live protections).
pub fn tier_b_entry_ok(
    cfg: &crate::scoring::config::TierBGateConfig,
    f: &TokenFeatures,
    items: &[(&'static str, i32)],
) -> bool {
    tier_b_base_gates_ok(cfg, f) && has_momentum_good(items) && tier_b_velocity_ok(cfg, f)
}

/// Which tier-B gate failed for a fresh-dev cap lane (for skip logging).
pub fn fresh_b_gate_fail_reason(
    cfg: &crate::scoring::config::TierBGateConfig,
    f: &TokenFeatures,
    items: &[(&'static str, i32)],
) -> Option<&'static str> {
    if !cfg.enabled {
        return Some("fresh_b_disabled");
    }
    if !tier_b_base_gates_ok(cfg, f) {
        if f.smart_wallet_count < cfg.min_smart_wallets {
            return Some("fresh_b_no_smart");
        }
        if f.buyer_count() < cfg.min_buyers {
            return Some("fresh_b_low_buyers");
        }
        if f.buy_volume_sol < cfg.min_buy_volume_sol {
            return Some("fresh_b_low_volume");
        }
        return None;
    }
    if !has_momentum_good(items) {
        return Some("fresh_b_no_momentum");
    }
    if !tier_b_velocity_ok(cfg, f) {
        return Some("fresh_b_low_velocity");
    }
    None
}

/// Strong tier-A bypass for the live `require_momentum_good` gate: score >= configured
/// minimum with strong early tape (buyers, volume, absorb, buyer-velocity persistence)
/// but **without** requiring smart wallets. Weak A (score 6–7, fading tape) stays out.
pub fn strong_a_momentum_bypass_ok(
    cfg: &crate::scoring::config::MomentumGoodStrongABypassConfig,
    tier: crate::scoring::score_engine::Tier,
    score: i32,
    f: &TokenFeatures,
    items: &[(&'static str, i32)],
) -> bool {
    use crate::scoring::score_engine::Tier;

    if !cfg.enabled || tier != Tier::A || score < cfg.min_score {
        return false;
    }

    let has = |name: &str| items.iter().any(|(n, _)| *n == name);

    // Tape red flags: do not loosen weak / distribution setups.
    if has("buyer_velocity_fading")
        || has("momentum_overheated")
        || has("repeat_dump_cluster")
        || has("sell_pressure_high")
        || has("bundle_identical")
        || has("bundle_similar")
    {
        return false;
    }

    if f.buyer_count() < cfg.min_buyers {
        return false;
    }
    if f.buy_volume_sol < cfg.min_buy_volume_sol {
        return false;
    }
    if f.buy_to_sell_ratio < cfg.min_buy_to_sell_ratio {
        return false;
    }

    let absorb_ok = !cfg.require_absorb_strong
        || has("absorb_strong")
        || f.absorb_quality_score >= cfg.min_absorb_quality;
    let velocity_ok = !cfg.require_buyer_velocity_persistent
        || has("buyer_velocity_persistent")
        || f.buyer_velocity_persistence >= cfg.min_buyer_velocity_persistence;

    absorb_ok && velocity_ok
}

#[cfg(test)]
mod continuation_tests {
    use super::*;
    use crate::scoring::config::ContinuationConfig;

    fn pt(buyers: u64, buy_vol: f64, cum_sell_raw: u64, mcap: f64) -> EarlyTapePoint {
        EarlyTapePoint {
            buyer_count: buyers,
            still_long: buyers,
            already_sold: 0,
            buy_volume_sol: buy_vol,
            cum_sell_raw,
            cum_sell_events: 0,
            mcap_sol: mcap,
        }
    }

    fn cfg() -> ContinuationConfig {
        ContinuationConfig {
            enabled: true,
            ..ContinuationConfig::default()
        }
    }

    #[test]
    fn healthy_continuation_passes() {
        // rising mcap, new buyers, modest sells -> Ok
        let confirm = vec![pt(20, 10.0, 0, 60.0), pt(24, 13.0, 200_000_000, 66.0)];
        assert_eq!(
            evaluate_continuation(&cfg(), 3.0, 20, &confirm, 1500, false),
            Ok(())
        );
    }

    #[test]
    fn declining_mcap_no_uptick() {
        // window closes below where it opened and upticks are sparse -> dead.
        let confirm = vec![pt(20, 10.0, 0, 60.0), pt(24, 13.0, 0, 58.0)];
        assert_eq!(
            evaluate_continuation(&cfg(), 3.0, 20, &confirm, 1500, false),
            Err("cont_no_uptick")
        );
    }

    #[test]
    fn flat_mcap_passes_uptick_gate() {
        // mcap_now == mcap_init (net non-negative): sparse upticks are allowed,
        // so the uptick gate must not fire (other gates may still pass it).
        let confirm = vec![pt(20, 10.0, 0, 60.0), pt(24, 13.0, 0, 60.0)];
        assert_eq!(
            evaluate_continuation(&cfg(), 3.0, 20, &confirm, 1500, false),
            Ok(())
        );
    }

    #[test]
    fn no_new_buyers_is_fake_momentum() {
        let confirm = vec![pt(20, 10.0, 0, 60.0), pt(20, 11.0, 0, 64.0)];
        assert_eq!(
            evaluate_continuation(&cfg(), 3.0, 20, &confirm, 1500, false),
            Err("cont_no_new_buyers")
        );
    }

    #[test]
    fn sell_absorption_blocks() {
        // tiny buy delta, huge sell delta in-window
        let confirm = vec![pt(20, 10.0, 0, 60.0), pt(24, 10.2, 5_000_000_000, 64.0)];
        assert_eq!(
            evaluate_continuation(&cfg(), 3.0, 20, &confirm, 1500, false),
            Err("cont_sell_absorption")
        );
    }

    #[test]
    fn b2s_worsening_blocks() {
        // baseline b2s very high; in-window sells make confirm b2s collapse,
        // but keep absorption under the cap so the b2s gate is what fires.
        let c = ContinuationConfig {
            max_sell_absorption_ratio: 100.0,
            ..cfg()
        };
        let confirm = vec![pt(20, 10.0, 0, 60.0), pt(24, 13.0, 2_500_000_000, 64.0)];
        assert_eq!(
            evaluate_continuation(&c, 50.0, 20, &confirm, 1500, false),
            Err("cont_b2s_worsening")
        );
    }

    #[test]
    fn b2s_worsening_relaxed_for_rising_a_plus() {
        // same worsening b2s, but A+ with mcap rising in-window -> allowed.
        let c = ContinuationConfig {
            max_sell_absorption_ratio: 100.0,
            ..cfg()
        };
        let confirm = vec![pt(20, 10.0, 0, 60.0), pt(24, 13.0, 2_500_000_000, 64.0)];
        assert_eq!(
            evaluate_continuation(&c, 50.0, 20, &confirm, 1500, true),
            Ok(())
        );
    }

    #[test]
    fn insufficient_data_fails_safe() {
        let confirm = vec![pt(20, 10.0, 0, 60.0)];
        assert_eq!(
            evaluate_continuation(&cfg(), 3.0, 20, &confirm, 1500, false),
            Err("cont_no_data")
        );
    }

    fn parab_cfg() -> AntiParabolicConfig {
        AntiParabolicConfig {
            enabled: true,
            ..AntiParabolicConfig::default()
        }
    }

    #[test]
    fn parabolic_weak_peak_no_smart_is_suspect() {
        // weak A score, no smart, mcap_now == mcap_peak -> flagged.
        assert!(parabolic_peak_suspect(&parab_cfg(), false, 8, 0, 65.2, 65.2));
    }

    #[test]
    fn parabolic_exempts_a_plus_smart_and_strong_score() {
        let c = parab_cfg();
        // A+ never flagged
        assert!(!parabolic_peak_suspect(&c, true, 8, 0, 65.2, 65.2));
        // smart money present never flagged
        assert!(!parabolic_peak_suspect(&c, false, 8, 1, 65.2, 65.2));
        // strong score (> weak_score_max) never flagged
        assert!(!parabolic_peak_suspect(&c, false, 12, 0, 65.2, 65.2));
    }

    #[test]
    fn parabolic_not_at_peak_is_safe() {
        // mcap_now well below peak -> not a peak entry.
        assert!(!parabolic_peak_suspect(&parab_cfg(), false, 8, 0, 50.0, 65.2));
    }

    #[test]
    fn parabolic_disabled_never_flags() {
        let c = AntiParabolicConfig::default(); // enabled = false
        assert!(!parabolic_peak_suspect(&c, false, 8, 0, 65.2, 65.2));
    }

    #[test]
    fn continuation_strength_counts_upticks_and_buyers() {
        let confirm = vec![pt(20, 10.0, 0, 60.0), pt(25, 13.0, 0, 66.0)];
        let (upticks, new_buyers) = continuation_strength(&confirm, 20);
        assert_eq!(upticks, 1);
        assert_eq!(new_buyers, 5);
    }

    #[test]
    fn continuation_strength_empty_is_zero() {
        let (upticks, new_buyers) = continuation_strength(&[], 20);
        assert_eq!((upticks, new_buyers), (0, 0));
    }

    #[test]
    fn second_look_eligible_only_strong_and_retry_reasons() {
        use crate::scoring::config::ContinuationSecondLookConfig;
        let cfg = ContinuationSecondLookConfig::default();
        assert!(continuation_second_look_eligible(
            &cfg,
            true,
            8,
            "cont_b2s_worsening"
        ));
        assert!(continuation_second_look_eligible(
            &cfg,
            false,
            10,
            "cont_sell_absorption"
        ));
        assert!(!continuation_second_look_eligible(
            &cfg,
            false,
            9,
            "cont_b2s_worsening"
        ));
        assert!(continuation_second_look_eligible(
            &cfg,
            true,
            10,
            "cont_no_uptick"
        ));
        assert!(!continuation_second_look_eligible(
            &cfg,
            false,
            10,
            "cont_no_uptick"
        ));
    }

    #[test]
    fn aplus_peak_recheck_rejects_collapsed_mcap() {
        use crate::scoring::config::ContinuationAplusPeakGuardConfig;
        let cfg = ContinuationAplusPeakGuardConfig::default();
        assert!(!aplus_peak_recheck_mcap_acceptable(&cfg, 101.8, 101.8, 32.7));
        assert!(aplus_peak_recheck_mcap_acceptable(&cfg, 108.8, 108.8, 98.3));
    }

    #[test]
    fn second_look_passes_on_recovery_uptick() {
        let ref_mcap = 90.0;
        let recheck = vec![pt(30, 20.0, 0, 88.0), pt(33, 22.0, 0, 92.0)];
        assert_eq!(
            evaluate_continuation_second_look(ref_mcap, 30, &recheck),
            Ok(())
        );
    }

    #[test]
    fn second_look_fails_when_mcap_keeps_falling() {
        let ref_mcap = 90.0;
        let recheck = vec![pt(30, 20.0, 0, 88.0), pt(30, 20.1, 0, 85.0)];
        assert_eq!(
            evaluate_continuation_second_look(ref_mcap, 30, &recheck),
            Err("cont_second_look_mcap_fall")
        );
    }

    #[test]
    fn second_look_passes_on_new_buyers_despite_dip() {
        let ref_mcap = 90.0;
        let recheck = vec![pt(30, 20.0, 0, 88.0), pt(35, 21.0, 0, 87.0)];
        assert_eq!(
            evaluate_continuation_second_look(ref_mcap, 30, &recheck),
            Ok(())
        );
    }

    #[test]
    fn tier_a_low_velocity_skip_blocks_flat_momentum() {
        assert_eq!(
            tier_a_low_velocity_skip_reason(Tier::A, 2.9),
            Some("low_velocity_a_setup")
        );
        assert_eq!(tier_a_low_velocity_skip_reason(Tier::A, 3.0), None);
        assert_eq!(tier_a_low_velocity_skip_reason(Tier::APlus, 0.0), None);
    }

    #[test]
    fn aplus_rug_gate_requires_smart_and_blocks_fake_b2s() {
        assert_eq!(
            aplus_rug_gate_skip_reason(Tier::APlus, 0, 5.0),
            Some("aplus_no_smart_required")
        );
        assert_eq!(aplus_rug_gate_skip_reason(Tier::APlus, 1, 15.0), None);
        assert_eq!(aplus_rug_gate_skip_reason(Tier::APlus, 2, 10.0), None);
        assert_eq!(
            aplus_rug_gate_skip_reason(Tier::A, 0, 20.0),
            None
        );
    }

    #[test]
    fn aplus_peak_guard_flags_high_peak_no_smart() {
        use crate::scoring::config::ContinuationAplusPeakGuardConfig;
        let cfg = ContinuationAplusPeakGuardConfig::default();
        assert!(aplus_peak_no_smart_guard(&cfg, true, 0, 120.0, 120.0));
        assert!(!aplus_peak_no_smart_guard(&cfg, true, 1, 120.0, 120.0));
        assert!(!aplus_peak_no_smart_guard(&cfg, false, 0, 120.0, 120.0));
        assert!(!aplus_peak_no_smart_guard(&cfg, true, 0, 80.0, 120.0));
    }

    #[test]
    fn continuation_confirm_strong_requires_upticks_and_buyers() {
        use crate::scoring::config::ContinuationAplusPeakGuardConfig;
        let cfg = ContinuationAplusPeakGuardConfig::default();
        let weak = vec![pt(20, 10.0, 0, 100.0), pt(22, 11.0, 0, 101.0)];
        assert!(!continuation_confirm_strong(&cfg, &weak, 20));
        let strong = vec![
            pt(20, 10.0, 0, 100.0),
            pt(23, 12.0, 0, 103.0),
            pt(26, 14.0, 0, 106.0),
        ];
        assert!(continuation_confirm_strong(&cfg, &strong, 20));
    }
}

#[cfg(test)]
mod strong_a_bypass_tests {
    use super::*;
    use crate::scoring::config::MomentumGoodStrongABypassConfig;
    use crate::scoring::score_engine::Tier;

    fn cfg() -> MomentumGoodStrongABypassConfig {
        MomentumGoodStrongABypassConfig::default()
    }

    fn strong_f() -> TokenFeatures {
        let f = TokenFeatures {
            mint: Address::default(),
            dev: Address::default(),
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
            current_mcap_sol: 50.0,
            initial_mcap_sol: 40.0,
            peak_mcap_sol: 55.0,
            buyers: EarlyBuyersSnapshot::default(),
            regular_buyer_count: 12,
            sniper_count: 0,
            buy_volume_sol: 20.0,
            still_long_count: 12,
            already_sold_count: 0,
            buy_to_sell_ratio: 2.0,
            bundle: BundleStats::empty(),
            smart_wallet_count: 0,
            buyer_velocity_new_per_slice: vec![3, 4, 5],
            buyer_velocity_persistence: 0.8,
            sell_pressure_score: 0.1,
            absorb_quality_score: 0.7,
            sell_events_window: 2,
            sell_volume_window_sol: 1.0,
            repeat_dump_slices: 0,
            smart_wallet_early_exits: 0,
        };
        f
    }

    #[test]
    fn strong_a_score_8_passes_without_smart() {
        let f = strong_f();
        let items = [
            ("buyers_10plus", 2),
            ("volume_ok", 1),
            ("absorb_strong", 2),
            ("buyer_velocity_persistent", 1),
        ];
        assert!(strong_a_momentum_bypass_ok(&cfg(), Tier::A, 8, &f, &items));
    }

    #[test]
    fn weak_a_score_6_rejected() {
        let f = strong_f();
        let items = [("buyers_10plus", 2), ("absorb_strong", 2)];
        assert!(!strong_a_momentum_bypass_ok(&cfg(), Tier::A, 6, &f, &items));
    }

    #[test]
    fn strong_a_fading_tape_rejected() {
        let f = strong_f();
        let items = [
            ("buyers_10plus", 2),
            ("absorb_strong", 2),
            ("buyer_velocity_fading", -2),
        ];
        assert!(!strong_a_momentum_bypass_ok(&cfg(), Tier::A, 9, &f, &items));
    }

    #[test]
    fn weak_a_synthetic_pump_blocks_fake_b2s_no_sell_flow() {
        use crate::scoring::config::WeakASyntheticPumpConfig;
        let cfg = WeakASyntheticPumpConfig::default();
        let mut f = strong_f();
        f.buy_to_sell_ratio = 17.0;
        f.sell_volume_window_sol = 0.06;
        let items = [("dev_history_weak", 2)];
        assert_eq!(
            weak_a_synthetic_pump_skip_reason(&cfg, Tier::A, 0, &f, &items),
            Some("weak_a_synthetic_pump")
        );
    }

    #[test]
    fn weak_a_synthetic_pump_allows_real_sell_flow() {
        use crate::scoring::config::WeakASyntheticPumpConfig;
        let cfg = WeakASyntheticPumpConfig::default();
        let mut f = strong_f();
        f.buy_to_sell_ratio = 17.0;
        f.sell_volume_window_sol = 2.5;
        let items = [("dev_history_weak", 2)];
        assert_eq!(weak_a_synthetic_pump_skip_reason(&cfg, Tier::A, 0, &f, &items), None);
    }

    #[test]
    fn weak_a_synthetic_pump_allows_a_plus_and_smart() {
        use crate::scoring::config::WeakASyntheticPumpConfig;
        let cfg = WeakASyntheticPumpConfig::default();
        let mut f = strong_f();
        f.buy_to_sell_ratio = 17.0;
        f.sell_volume_window_sol = 0.0;
        let items = [("dev_history_weak", 2)];
        assert_eq!(
            weak_a_synthetic_pump_skip_reason(&cfg, Tier::APlus, 0, &f, &items),
            None
        );
        assert_eq!(
            weak_a_synthetic_pump_skip_reason(&cfg, Tier::A, 1, &f, &items),
            None
        );
    }

    #[test]
    fn weak_a_hard_skip_on_dump_slice() {
        let gate = WeakATierGateConfig::default();
        let mut f = strong_f();
        f.buy_volume_sol = 20.0;
        f.repeat_dump_slices = 1;
        let items = [("dev_history_weak", 2)];
        assert_eq!(
            weak_a_hard_skip_reason(&gate, Tier::A, 7, 0, &f, &items),
            Some("weak_a_dump_in_tape")
        );
    }

    #[test]
    fn weak_a_hard_skip_on_low_volume() {
        let gate = WeakATierGateConfig::default();
        let mut f = strong_f();
        f.buy_volume_sol = 7.8;
        f.repeat_dump_slices = 0;
        let items = [("dev_history_weak", 2)];
        assert_eq!(
            weak_a_hard_skip_reason(&gate, Tier::A, 7, 0, &f, &items),
            Some("weak_a_low_volume")
        );
    }

    #[test]
    fn weak_a_second_look_on_any_cont_fail() {
        let sl = ContinuationSecondLookConfig::default();
        let gate = WeakATierGateConfig::default();
        let f = strong_f();
        let items = [("dev_history_weak", 2)];
        assert!(continuation_second_look_eligible_for_buy(
            &sl,
            &gate,
            Tier::A,
            false,
            7,
            0,
            &f,
            &items,
            "cont_no_uptick",
        ));
    }

    #[test]
    fn tier_b_dev_eligible_fresh_only() {
        use crate::scoring::config::TierBGateConfig;
        let cfg = TierBGateConfig::default();
        let mut f = strong_f();
        f.dev_category = DevCategory::Fresh;
        assert!(tier_b_dev_eligible(&f, &cfg));
        f.dev_total_coins = 0;
        f.dev_category = DevCategory::Stale;
        assert!(!tier_b_dev_eligible(&f, &cfg));
        f.dev_category = DevCategory::Neutral;
        assert!(!tier_b_dev_eligible(&f, &cfg));
        f.is_spam_dev = true;
        f.dev_category = DevCategory::Fresh;
        assert!(!tier_b_dev_eligible(&f, &cfg));
    }

    #[test]
    fn tier_b_entry_requires_momentum_velocity_and_thresholds() {
        use crate::scoring::config::TierBGateConfig;
        let cfg = TierBGateConfig::default();
        let mut f = strong_f();
        f.smart_wallet_count = 0;
        f.regular_buyer_count = 10;
        f.buy_volume_sol = 10.0;
        f.initial_mcap_sol = 40.0;
        f.peak_mcap_sol = 44.0;
        f.current_mcap_sol = 44.0;
        let items = [("momentum_good", 3)];
        assert!(tier_b_entry_ok(&cfg, &f, &items));
        let no_mom: [(&str, i32); 0] = [];
        assert!(!tier_b_entry_ok(&cfg, &f, &no_mom));
        f.buy_volume_sol = 9.9;
        assert!(!tier_b_entry_ok(&cfg, &f, &items));
        f.buy_volume_sol = 10.0;
        f.peak_mcap_sol = 41.0;
        f.current_mcap_sol = 41.0;
        assert!(!tier_b_entry_ok(&cfg, &f, &items));
    }

    #[test]
    fn fresh_b_gate_fail_reason_reports_first_miss() {
        use crate::scoring::config::TierBGateConfig;
        let cfg = TierBGateConfig::default();
        let mut f = strong_f();
        f.smart_wallet_count = 0;
        f.regular_buyer_count = 5;
        f.buy_volume_sol = 10.0;
        f.initial_mcap_sol = 40.0;
        f.peak_mcap_sol = 44.0;
        let items = [("momentum_good", 2)];
        assert_eq!(
            fresh_b_gate_fail_reason(&cfg, &f, &items),
            Some("fresh_b_low_buyers")
        );
        f.regular_buyer_count = 10;
        f.buy_volume_sol = 5.0;
        assert_eq!(
            fresh_b_gate_fail_reason(&cfg, &f, &items),
            Some("fresh_b_low_volume")
        );
    }

    #[test]
    fn fresh_dev_caps_a_to_b_or_skip() {
        use crate::scoring::config::{FeatureThresholds, ScoringConfig, TierBGateConfig};
        use crate::scoring::score_engine::{ScoreEngine, Tier};

        let mut cfg = ScoringConfig::default();
        cfg.tier_b = TierBGateConfig::default();
        let thr = FeatureThresholds::default();
        let engine = ScoreEngine::new(&cfg);

        let mut f = strong_f();
        f.dev_category = DevCategory::Fresh;
        f.smart_wallet_count = 0;
        f.regular_buyer_count = 10;
        f.buy_volume_sol = 10.0;
        f.initial_mcap_sol = 40.0;
        f.peak_mcap_sol = 48.0;
        f.current_mcap_sol = 48.0;

        let bd = engine.score(&f, &thr);
        assert_eq!(
            bd.tier,
            Tier::B,
            "fresh dev with B gates must cap to B, not A; score={} items={:?}",
            bd.total,
            bd.items
        );

        f.buy_volume_sol = 5.0;
        let bd = engine.score(&f, &thr);
        assert_eq!(bd.tier, Tier::Skip, "fresh dev below B volume must skip");
    }

    #[test]
    fn hot_fresh_override_assigns_tier_b_on_overheated_momentum_only() {
        use crate::scoring::config::{
            FeatureThresholds, HotFreshOverrideConfig, ScoringConfig, TierBGateConfig,
        };
        use crate::scoring::score_engine::{ScoreEngine, Tier};

        let mut cfg = ScoringConfig::default();
        cfg.tier_b = TierBGateConfig {
            enabled: true,
            min_smart_wallets: 0,
            min_buyers: 10,
            min_buy_volume_sol: 10.0,
            min_velocity_pct: 5.0,
            fresh_watchlist: Default::default(),
            hot_fresh_override: HotFreshOverrideConfig {
                enabled: true,
                min_buyers: 25,
                min_buy_volume_sol: 25.0,
                min_velocity_pct: 100.0,
            },
        };
        let thr = FeatureThresholds::default();
        let engine = ScoreEngine::new(&cfg);

        let mut f = strong_f();
        f.dev_category = DevCategory::Fresh;
        f.smart_wallet_count = 0;
        f.regular_buyer_count = 30;
        f.buy_volume_sol = 30.0;
        f.initial_mcap_sol = 30.0;
        f.peak_mcap_sol = 90.0;
        f.current_mcap_sol = 85.0;

        let bd = engine.score(&f, &thr);
        assert_eq!(bd.tier, Tier::B);
        assert!(bd.fresh_b_hot_override);
        assert!(super::has_momentum_overheated(&bd.items));

        f.regular_buyer_count = 20;
        let bd = engine.score(&f, &thr);
        assert_eq!(bd.tier, Tier::Skip);
        assert!(!bd.fresh_b_hot_override);
    }

    #[test]
    fn aa_momentum_override_on_overheated_a_and_a_plus() {
        use crate::scoring::config::{AAMomentumOverrideConfig, FeatureThresholds, ScoringConfig};
        use crate::scoring::score_engine::{ScoreEngine, Tier};

        let cfg_on = AAMomentumOverrideConfig { enabled: true };
        let items = [("momentum_overheated", -3)];
        assert!(super::aa_momentum_override_for_tier(&cfg_on, Tier::A, &items));
        assert!(super::aa_momentum_override_for_tier(&cfg_on, Tier::APlus, &items));
        assert!(!super::aa_momentum_override_for_tier(&cfg_on, Tier::Skip, &items));

        let mut cfg = ScoringConfig::default();
        cfg.tier_b.enabled = false;
        cfg.aa_momentum_override = cfg_on;
        let engine = ScoreEngine::new(&cfg);

        let mut f = strong_f();
        f.dev_has_history = true;
        f.dev_total_coins = 3;
        f.dev_pnl_avg = 5.0;
        f.dev_category = DevCategory::A;
        f.smart_wallet_count = 1;
        f.regular_buyer_count = 20;
        f.buy_volume_sol = 30.0;
        f.sell_volume_window_sol = 2.5;
        f.absorb_quality_score = 0.9;
        f.initial_mcap_sol = 35.0;
        f.peak_mcap_sol = 70.0;
        f.current_mcap_sol = 68.0;

        let bd = engine.score(&f, &FeatureThresholds::default());
        assert_eq!(bd.tier, Tier::A, "score={} items={:?}", bd.total, bd.items);
        assert!(bd.a_momentum_override);
        assert!(!bd.a_plus_momentum_override);
        assert!(super::is_momentum_only_aa_overheated(&bd.items));
    }

    #[test]
    fn aa_momentum_override_disabled_leaves_gate_blocked() {
        use crate::scoring::config::{AAMomentumOverrideConfig, FeatureThresholds, ScoringConfig};
        use crate::scoring::score_engine::ScoreEngine;

        let mut cfg = ScoringConfig::default();
        cfg.tier_b.enabled = false;
        cfg.aa_momentum_override = AAMomentumOverrideConfig { enabled: false };
        let engine = ScoreEngine::new(&cfg);
        let mut f = strong_f();
        f.dev_category = DevCategory::Neutral;
        f.initial_mcap_sol = 30.0;
        f.peak_mcap_sol = 60.0;
        f.current_mcap_sol = 58.0;
        let bd = engine.score(&f, &FeatureThresholds::default());
        assert!(!bd.a_momentum_override);
        assert!(!bd.a_plus_momentum_override);
    }
}
