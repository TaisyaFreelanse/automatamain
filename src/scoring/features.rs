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
use crate::scoring::config::{AntiParabolicConfig, ContinuationConfig, FeatureThresholds};
use crate::scoring::dev_ranker::{DevCategory, DevRecord};
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
#[derive(Clone, Debug)]
pub struct EarlyTapeObserveResult {
    pub points: Vec<EarlyTapePoint>,
    pub initial_mcap_sol: f64,
    pub peak_mcap_sol: f64,
    pub current_mcap_sol: f64,
    pub exited_early: bool,
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
        let points = if let Some(bucket) = fetch_live_bucket(launchpad, mint).await {
            vec![snapshot_early_tape(&bucket).await]
        } else {
            Vec::new()
        };
        return finalize_tape_observe(points, false);
    }

    let slice_ms = (window_ms / s).max(1);
    let mut out: Vec<EarlyTapePoint> = Vec::with_capacity(s as usize);
    let mut initial_mcap_sol = 0.0_f64;
    let mut peak_mcap_sol = 0.0_f64;

    for i in 0..s {
        if i > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(slice_ms)).await;
        }
        let Some(bucket) = fetch_live_bucket(launchpad, mint).await else {
            continue;
        };
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
                return finalize_tape_observe(out, true);
            }
        }
    }

    let used = slice_ms.saturating_mul(s.saturating_sub(1));
    if window_ms > used {
        tokio::time::sleep(std::time::Duration::from_millis(window_ms - used)).await;
        if let Some(bucket) = fetch_live_bucket(launchpad, mint).await {
            let point = snapshot_early_tape(&bucket).await;
            if out.is_empty() {
                return finalize_tape_observe(vec![point], false);
            }
            if let Some(last) = out.last_mut() {
                *last = point;
            }
        }
    }

    let mut result = finalize_tape_observe(out, false);
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
        let mut f = TokenFeatures {
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
}
