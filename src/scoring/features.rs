//! Snapshots everything we know about a token at scoring time. Pure data —
//! no decisions, no I/O beyond the swarm/registry queries.

use solana_address::Address;

use crate::launchpads::token_bucket::TokenBucket;
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
}

impl TokenFeatures {
    pub fn buyer_count(&self) -> u64 {
        self.regular_buyer_count + self.sniper_count
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
    }
}
