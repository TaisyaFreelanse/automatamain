//! Background mcap samples after a full position close (30m window).
//! Primary: pump bonding curve; fallback: Jupiter when curve is migrated.

use std::{sync::Arc, time::Duration};

use solana_address::Address as SolAddress;
use solana_client::nonblocking::rpc_client::RpcClient;

use crate::{
    autobuy::{
        jupiter_sell::jupiter_implied_mcap_sol,
        pump_brocker::probe_bonding_mcap_sol,
    },
    persistence::bot_trade_post_exit::{
        BotTradePostExitRepository, PostExitMetrics, PostExitSampleState,
        POST_EXIT_CHECKPOINT_SECS,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PostExitMcapSource {
    Bonding,
    Jupiter,
}

impl PostExitMcapSource {
    fn as_str(self) -> &'static str {
        match self {
            PostExitMcapSource::Bonding => "bonding",
            PostExitMcapSource::Jupiter => "jupiter",
        }
    }
}

/// Bonding curve first; Jupiter Price v3 / quote if curve complete or missing.
async fn sample_post_exit_mcap(
    rpc: &RpcClient,
    mint: &SolAddress,
    mint_str: &str,
) -> Option<(f64, PostExitMcapSource)> {
    if let Some(mcap) = probe_bonding_mcap_sol(rpc, mint).await {
        return Some((mcap, PostExitMcapSource::Bonding));
    }
    jupiter_implied_mcap_sol(mint_str)
        .await
        .map(|mcap| (mcap, PostExitMcapSource::Jupiter))
}

async fn sleep_until(elapsed: u64, target_secs: u64) {
    if target_secs > elapsed {
        tokio::time::sleep(Duration::from_secs(target_secs - elapsed)).await;
    }
}

/// Scheduled probes: dense 10s–300s, then 10m / 15m / 30m; track max/min + time of extrema.
pub fn spawn_post_exit_tracking(
    rpc: Arc<RpcClient>,
    repo: Arc<dyn BotTradePostExitRepository + Send + Sync>,
    trade_id: i64,
    mint: String,
    exit_mcap_sol: f64,
) {
    tokio::spawn(async move {
        let mint_addr = match mint.parse::<SolAddress>() {
            Ok(a) => a,
            Err(e) => {
                eprintln!("[POST-EXIT] {mint}: invalid mint address: {e}");
                return;
            }
        };

        let mut samples = PostExitSampleState::default();
        let mut bonding_samples = 0u32;
        let mut jupiter_samples = 0u32;
        let mut missed_samples = 0u32;

        samples.record_exit_baseline(exit_mcap_sol);

        let started = std::time::Instant::now();

        for &checkpoint in POST_EXIT_CHECKPOINT_SECS {
            let elapsed = started.elapsed().as_secs();
            sleep_until(elapsed, checkpoint).await;

            match sample_post_exit_mcap(&rpc, &mint_addr, &mint).await {
                Some((mcap, source)) => {
                    samples.set_checkpoint(checkpoint, mcap);
                    match source {
                        PostExitMcapSource::Bonding => bonding_samples += 1,
                        PostExitMcapSource::Jupiter => jupiter_samples += 1,
                    }
                    eprintln!(
                        "[POST-EXIT] {mint} id={trade_id} t={checkpoint}s mcap={mcap:.2} source={}",
                        source.as_str()
                    );
                }
                None => {
                    missed_samples += 1;
                    eprintln!(
                        "[POST-EXIT] {mint} id={trade_id} t={checkpoint}s mcap=unavailable (bonding+jupiter)"
                    );
                }
            }
        }

        let metrics = PostExitMetrics::from_samples(exit_mcap_sol, &samples);
        if let Err(e) = repo.update_post_exit_metrics(trade_id, &metrics).await {
            eprintln!("[POST-EXIT] trade_id={trade_id} mint={mint}: save failed: {e:?}");
            return;
        }
        eprintln!(
            "[POST-EXIT] {mint} id={trade_id} exit_mcap={exit_mcap_sol:.1} \
             samples bonding={bonding_samples} jupiter={jupiter_samples} missed={missed_samples} \
             max={:?}% @ {:?}s min={:?}% @ {:?}s | 100s={:?}% 300s={:?}% 10m={:?}%",
            metrics.post_exit_max_pct,
            metrics.post_exit_time_to_max_secs,
            metrics.post_exit_min_pct,
            metrics.post_exit_time_to_min_secs,
            metrics.post_exit_pct_100s,
            metrics.post_exit_pct_300s,
            metrics.post_exit_pct_10m,
        );
    });
}
