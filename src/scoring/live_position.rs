//! In-position live tape snapshots from an up-to-date `TokenBucket`.

use solana_address::Address;

use crate::{
    launchpads::token_bucket::TokenBucket,
    scoring::{
        anti_bundle::compute_bundle_stats,
        features::{snapshot_early_tape, EarlyTapePoint, ScoringTapeDerived},
        smart_money::SmartMoneyHandle,
    },
    trading::trader::TraderType,
};

/// Count smart-registry wallets still holding among Regular+Sniper traders.
pub async fn count_smart_wallets_still_long(
    bucket: &TokenBucket,
    smart_money: Option<&SmartMoneyHandle>,
) -> u32 {
    let Some(sm) = smart_money else {
        return 0;
    };
    let regulars = bucket.swarm().get_traders_by_type(TraderType::Regular).await;
    let snipers = bucket.swarm().get_traders_by_type(TraderType::Sniper).await;
    let mut addrs: Vec<Address> = Vec::new();
    for (a, t) in regulars.iter().chain(snipers.iter()) {
        if t.holdings().raw() > 0 {
            addrs.push(*a);
        }
    }
    if addrs.is_empty() {
        return 0;
    }
    sm.filter_smart_wallets(addrs).await.len() as u32
}

/// Smart wallets that were in early_buyers but no longer hold (exit signal).
pub async fn count_smart_early_exits(
    bucket: &TokenBucket,
    smart_money: Option<&SmartMoneyHandle>,
    early_buyers: &[Address],
) -> u32 {
    let Some(sm) = smart_money else {
        return 0;
    };
    if early_buyers.is_empty() {
        return 0;
    }
    let smart_addrs = sm.filter_smart_wallets(early_buyers.to_vec()).await;
    if smart_addrs.is_empty() {
        return 0;
    }
    let regulars = bucket.swarm().get_traders_by_type(TraderType::Regular).await;
    let snipers = bucket.swarm().get_traders_by_type(TraderType::Sniper).await;
    let mut holding: std::collections::HashSet<Address> = std::collections::HashSet::new();
    for (a, t) in regulars.iter().chain(snipers.iter()) {
        if t.holdings().raw() > 0 {
            holding.insert(*a);
        }
    }
    smart_addrs
        .iter()
        .filter(|a| !holding.contains(a))
        .count() as u32
}

/// Live snapshot: current tape point + derived sell pressure from last two points.
pub async fn snapshot_live_position(
    bucket: &TokenBucket,
    prev: Option<&EarlyTapePoint>,
    held_secs: u64,
    smart_money: Option<&SmartMoneyHandle>,
    early_buyers: &[Address],
    bundle_tolerance: f64,
) -> LivePositionSnapshot {
    let curr = snapshot_early_tape(bucket).await;
    let smart_count = count_smart_wallets_still_long(bucket, smart_money).await;
    let smart_exits = count_smart_early_exits(bucket, smart_money, early_buyers).await;

    let dt = held_secs.max(1) as f64;
    let (buyers_per_sec, holder_growth, volume_delta, liquidity_delta) =
        if let Some(p) = prev {
            let db = curr.buyer_count.saturating_sub(p.buyer_count) as f64 / dt;
            let dh = curr.still_long.saturating_sub(p.still_long) as f64 / dt;
            let dv = (curr.buy_volume_sol - p.buy_volume_sol) / dt;
            let dl = (curr.mcap_sol - p.mcap_sol) / dt;
            (db, dh, dv, dl)
        } else {
            (curr.buyer_count as f64 / dt, 0.0, 0.0, 0.0)
        };

    let tape_derived = if let Some(p) = prev {
        ScoringTapeDerived::from_tape_points(&[p.clone(), curr.clone()], smart_exits)
    } else {
        ScoringTapeDerived::from_tape_points(&[curr.clone()], smart_exits)
    };

    let mut buy_sizes: Vec<f64> = Vec::new();
    let regulars = bucket.swarm().get_traders_by_type(TraderType::Regular).await;
    let snipers = bucket.swarm().get_traders_by_type(TraderType::Sniper).await;
    for (_a, t) in regulars.iter().chain(snipers.iter()) {
        let s = t.total_spent().to_float();
        if s > 0.0 {
            buy_sizes.push(s);
        }
    }
    let bundle = compute_bundle_stats(&buy_sizes, bundle_tolerance);

    let b2s = if curr.already_sold == 0 {
        curr.still_long as f64
    } else {
        curr.still_long as f64 / curr.already_sold as f64
    };

    let mcap_sol = curr.mcap_sol;
    LivePositionSnapshot {
        tape: curr,
        buyers_per_sec,
        holder_growth_rate: holder_growth,
        volume_delta,
        liquidity_delta,
        smart_wallet_count: smart_count,
        smart_wallet_exits: smart_exits,
        buy_sell_ratio: b2s,
        sell_pressure_score: tape_derived.sell_pressure_score,
        bundle_similar: bundle.similar_size_ratio,
        bundle_identical: bundle.identical_size_ratio,
        mcap_sol,
    }
}

#[derive(Clone, Debug)]
pub struct LivePositionSnapshot {
    pub tape: EarlyTapePoint,
    pub buyers_per_sec: f64,
    pub holder_growth_rate: f64,
    pub volume_delta: f64,
    pub liquidity_delta: f64,
    pub smart_wallet_count: u32,
    pub smart_wallet_exits: u32,
    pub buy_sell_ratio: f64,
    pub sell_pressure_score: f64,
    pub bundle_similar: f64,
    pub bundle_identical: f64,
    pub mcap_sol: f64,
}
