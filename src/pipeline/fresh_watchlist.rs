//! Fresh watchlist: deferred re-evaluation for fresh devs that fail
//! `creator_config` only on immature early statistics.

use solana_address::Address;
use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::mpsc;

use crate::{
    feed::metrics::BotMetrics,
    launchpads::{pump::launchpad::PumpLaunchpadCommand, token_bucket::TokenBucket},
    learning::{merge_thresholds, LearningLogPg, LearningOverridesFile},
    pipeline::score_buy::{run_scoring_and_buy, ScoringPipelineDeps, ScoringPipelineInput},
    scoring::{
        config::FreshWatchlistConfig,
        dev_ranker::{DevCategory, DevRecord},
        features::{self, ScoringTapeDerived},
        fresh_b::FreshBSubtype,
        score_engine::ScoreEngine,
        smart_money::SmartMoneyHandle,
    },
};
use tokio::sync::RwLock;

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TryAddError {
    Disabled,
    Duplicate,
    CapFull,
}

pub struct FreshWatchlistManager {
    active: Mutex<HashSet<Address>>,
    running: AtomicUsize,
}

impl FreshWatchlistManager {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            active: Mutex::new(HashSet::new()),
            running: AtomicUsize::new(0),
        })
    }

    /// Defer a fresh-dev token for poll-based B-gate re-evaluation.
    #[allow(clippy::too_many_arguments)]
    pub fn try_add(
        self: &Arc<Self>,
        cfg: &FreshWatchlistConfig,
        mint: Address,
        dev: Address,
        initial_mcap_sol: f64,
        failed_reasons: Vec<String>,
        bucket_for_score: TokenBucket,
        pipeline_t0: Instant,
        pipeline_deps: ScoringPipelineDeps,
    learning_log: Option<LearningLogPg>,
    bot_metrics: Arc<BotMetrics>,
) -> Result<(), TryAddError> {
        if !cfg.enabled {
            return Err(TryAddError::Disabled);
        }

        {
            let mut active = self.active.lock().expect("fresh watchlist lock");
            if active.contains(&mint) {
                return Err(TryAddError::Duplicate);
            }
            if self.running.load(Ordering::Relaxed) >= cfg.max_concurrent {
                return Err(TryAddError::CapFull);
            }
            active.insert(mint);
        }
        self.running.fetch_add(1, Ordering::Relaxed);

        bot_metrics.note_fresh_watchlist_added();
        eprintln!(
            "Fresh Watchlist Added {} dev={} fresh_b_subtype={} initial_mcap={:.2} reasons={:?}",
            mint,
            dev,
            FreshBSubtype::TrueFresh.as_str(),
            initial_mcap_sol,
            failed_reasons,
        );
        if let Some(ref log) = learning_log {
            let log = log.clone();
            let mint_s = mint.to_string();
            let dev_s = dev.to_string();
            let ts = unix_now();
            let payload = serde_json::json!({
                "fresh_b_subtype": FreshBSubtype::TrueFresh.as_str(),
                "failed_reasons": failed_reasons,
                "initial_mcap_sol": initial_mcap_sol,
            });
            tokio::spawn(async move {
                let _ = log
                    .log_skipped(
                        &mint_s,
                        Some(dev_s.as_str()),
                        "fresh_watchlist",
                        "watchlist_added",
                        payload,
                        ts,
                    )
                    .await;
            });
        }

        let mgr = Arc::clone(self);
        let poll_ms = cfg.poll_interval_ms.max(1);
        let max_wait_ms = cfg.max_wait_ms.max(poll_ms);
        let tier_b = pipeline_deps.filter_config.scoring.tier_b.clone();
        let scoring = pipeline_deps.filter_config.scoring.clone();
        let launchpad = pipeline_deps.launchpad_for_score.clone();
        let smart_money = pipeline_deps.smart_money.clone();
        let learning_overrides = pipeline_deps.learning_overrides.clone();

        tokio::spawn(async move {
            let deadline = Instant::now() + Duration::from_millis(max_wait_ms);
            let mut passed = false;

            while Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(poll_ms)).await;

                if poll_b_gates_ready(
                    &launchpad,
                    &smart_money,
                    &learning_overrides,
                    mint,
                    dev,
                    initial_mcap_sol,
                    &scoring,
                    &tier_b,
                )
                .await
                {
                    passed = true;
                    break;
                }
            }

            {
                let mut active = mgr.active.lock().expect("fresh watchlist lock");
                active.remove(&mint);
            }
            mgr.running.fetch_sub(1, Ordering::Relaxed);

            if passed {
                bot_metrics.note_fresh_watchlist_passed();
                eprintln!(
                    "Fresh Watchlist Passed {} fresh_b_subtype={}",
                    mint,
                    FreshBSubtype::TrueFresh.as_str(),
                );
                if let Some(ref log) = learning_log {
                    let log = log.clone();
                    let mint_s = mint.to_string();
                    let dev_s = dev.to_string();
                    let ts = unix_now();
                    let payload = serde_json::json!({
                        "fresh_b_subtype": FreshBSubtype::TrueFresh.as_str(),
                    });
                    tokio::spawn(async move {
                        let _ = log
                            .log_skipped(
                                &mint_s,
                                Some(dev_s.as_str()),
                                "fresh_watchlist",
                                "watchlist_passed",
                                payload,
                                ts,
                            )
                            .await;
                    });
                }

                run_scoring_and_buy(
                    &pipeline_deps,
                    ScoringPipelineInput {
                        mint,
                        dev,
                        dev_stats: None,
                        is_spam_dev: false,
                        bucket_for_score,
                        pipeline_t0,
                        from_fresh_watchlist: true,
                    },
                )
                .await;
            } else {
                bot_metrics.note_fresh_watchlist_rejected();
                eprintln!(
                    "Fresh Watchlist Rejected {} fresh_b_subtype={}",
                    mint,
                    FreshBSubtype::TrueFresh.as_str(),
                );
                if let Some(ref log) = learning_log {
                    let log = log.clone();
                    let mint_s = mint.to_string();
                    let dev_s = dev.to_string();
                    let ts = unix_now();
                    let payload = serde_json::json!({
                        "fresh_b_subtype": FreshBSubtype::TrueFresh.as_str(),
                        "failed_reasons": failed_reasons,
                        "max_wait_ms": max_wait_ms,
                    });
                    tokio::spawn(async move {
                        let _ = log
                            .log_skipped(
                                &mint_s,
                                Some(dev_s.as_str()),
                                "fresh_watchlist",
                                "watchlist_rejected",
                                payload,
                                ts,
                            )
                            .await;
                    });
                }
            }
        });

        Ok(())
    }
}

async fn poll_b_gates_ready(
    launchpad: &mpsc::Sender<PumpLaunchpadCommand>,
    smart_money: &SmartMoneyHandle,
    learning_overrides: &Arc<RwLock<LearningOverridesFile>>,
    mint: Address,
    dev: Address,
    initial_mcap_sol: f64,
    scoring_cfg: &crate::scoring::config::ScoringConfig,
    tier_b: &crate::scoring::config::TierBGateConfig,
) -> bool {
    let Some(bucket) = features::fetch_live_bucket(launchpad, mint).await else {
        return false;
    };

    let tape_point = features::snapshot_early_tape(&bucket).await;
    let current_mcap_sol = tape_point.mcap_sol;
    let peak_mcap_sol = initial_mcap_sol.max(current_mcap_sol);

    let merged_thr = merge_thresholds(
        &scoring_cfg.thresholds,
        &learning_overrides.read().await.patch,
    );
    let thr = if scoring_cfg.legacy_scoring {
        &scoring_cfg.thresholds
    } else {
        &merged_thr
    };

    let (early_buyers, _, buy_volume_sol, still_long, sold, bundle) =
        features::snapshot_early_buyers(&bucket, thr).await;
    let buyers_for_position = early_buyers.all();
    let smart_count = smart_money.count_smart(buyers_for_position).await;
    let regular_buyer_count = early_buyers.regulars.len() as u64;
    let sniper_count = early_buyers.snipers.len() as u64;

    let tape = ScoringTapeDerived::from_tape_points(&[tape_point], 0);
    let token_features = features::assemble(
        mint,
        dev,
        None,
        DevCategory::Fresh,
        DevRecord::default(),
        initial_mcap_sol,
        current_mcap_sol,
        peak_mcap_sol,
        early_buyers,
        regular_buyer_count,
        sniper_count,
        buy_volume_sol,
        still_long,
        sold,
        bundle,
        smart_count,
        tape,
    );

    let engine = ScoreEngine::new(scoring_cfg);
    let breakdown = engine.score(&token_features, thr);
    features::tier_b_entry_ok(tier_b, &token_features, &breakdown.items)
}
