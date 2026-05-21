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
use crate::scoring::config::FeatureThresholds;
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

    // Pool / market state
    pub current_mcap_sol: f64,
    pub initial_mcap_sol: f64,

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

/// Like `observe_early_tape_points`, but each sample uses a fresh bucket from
/// launchpad storage so `mcap_sol` tracks bonding-curve trades during the window.
pub async fn observe_early_tape_points_live(
    launchpad: &mpsc::Sender<PumpLaunchpadCommand>,
    mint: Address,
    window_ms: u64,
    slices: usize,
) -> Vec<EarlyTapePoint> {
    let s = slices.max(1) as u64;
    let window_ms = window_ms.max(1);
    if s <= 1 {
        tokio::time::sleep(std::time::Duration::from_millis(window_ms)).await;
        if let Some(bucket) = fetch_live_bucket(launchpad, mint).await {
            return vec![snapshot_early_tape(&bucket).await];
        }
        return Vec::new();
    }

    let slice_ms = (window_ms / s).max(1);
    let mut out: Vec<EarlyTapePoint> = Vec::with_capacity(s as usize);
    for i in 0..s {
        if i > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(slice_ms)).await;
        }
        if let Some(bucket) = fetch_live_bucket(launchpad, mint).await {
            out.push(snapshot_early_tape(&bucket).await);
        }
    }
    let used = slice_ms.saturating_mul(s.saturating_sub(1));
    if window_ms > used {
        tokio::time::sleep(std::time::Duration::from_millis(window_ms - used)).await;
        if let Some(bucket) = fetch_live_bucket(launchpad, mint).await {
            if let Some(last) = out.last_mut() {
                *last = snapshot_early_tape(&bucket).await;
            } else {
                out.push(snapshot_early_tape(&bucket).await);
            }
        }
    }
    out
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
        current_mcap_sol,
        initial_mcap_sol,
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
