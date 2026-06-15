//! Scoring window, gates, and buy dispatch (extracted from main create handler).

use solana_address::Address;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::{Instant, SystemTime};
use tokio::sync::{mpsc, RwLock};

use crate::{
    autobuy::{
        execution::ExecutionMode,
        filters::config::Config,
        manager::{OpenReason, PositionMessage},
        performance_tracker::CreatorRegistryHandle,
    },
    feed::metrics::BotMetrics,
    launchpads::{
        pump::launchpad::PumpLaunchpadCommand,
        token_bucket::TokenBucket,
    },
    learning::{
        merge_thresholds, LearningLogPg, LearningOverridesFile, LearningTradeSnapshot,
    },
    persistence::creators::CreatorStatistics,
    scoring::{
        anti_rug::entry_skip_reason,
        config::MinBuyTier,
        dev_ranker::{DevCategory, DevRankerHandle, DevRecord},
        features,
        fresh_b::FreshBSubtype,
        score_engine::{ScoreBreakdown, ScoreEngine, Tier},
        smart_money::SmartMoneyHandle,
    },
    telemetry::buy_latency::BuyLatencyRegistry,
};

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn learning_snap(
    f: &features::TokenFeatures,
    breakdown: &ScoreBreakdown,
    dev_stats: Option<&CreatorStatistics>,
    from_fresh_watchlist: bool,
) -> LearningTradeSnapshot {
    let fresh_watchlist = from_fresh_watchlist.then_some("passed");
    LearningTradeSnapshot::from_scoring_fresh_b(
        f,
        breakdown,
        FreshBSubtype::for_path(dev_stats, from_fresh_watchlist),
        fresh_watchlist,
    )
}

/// Shared handles for the scoring + buy pipeline.
pub struct ScoringPipelineDeps {
    pub manager_tx: mpsc::Sender<PositionMessage>,
    pub filter_config: Arc<Config>,
    pub launchpad_for_score: mpsc::Sender<PumpLaunchpadCommand>,
    pub dev_ranker: DevRankerHandle,
    pub smart_money: SmartMoneyHandle,
    pub learning_log: Option<LearningLogPg>,
    pub learning_overrides: Arc<RwLock<LearningOverridesFile>>,
    pub bot_metrics: Arc<BotMetrics>,
    pub buy_latency: Arc<BuyLatencyRegistry>,
    pub buy_cap: Arc<AtomicU64>,
    pub registry: Option<CreatorRegistryHandle>,
}

pub struct ScoringPipelineInput {
    pub mint: Address,
    pub dev: Address,
    pub dev_stats: Option<CreatorStatistics>,
    pub is_spam_dev: bool,
    pub bucket_for_score: TokenBucket,
    pub pipeline_t0: Instant,
    pub from_fresh_watchlist: bool,
}

pub async fn run_scoring_and_buy(deps: &ScoringPipelineDeps, input: ScoringPipelineInput) {
    let ScoringPipelineInput {
        mint,
        dev,
        dev_stats,
        is_spam_dev,
        bucket_for_score,
        pipeline_t0,
        from_fresh_watchlist,
    } = input;

    if from_fresh_watchlist {
        eprintln!(
            "[FILTER] {} resumed from fresh_watchlist fresh_b_subtype={}",
            mint,
            FreshBSubtype::TrueFresh.as_str(),
        );
        deps.bot_metrics.note_passed_filter();
    }
    // --- Stage 2: scoring window + early tape ---------
    let window_ms = deps.filter_config.scoring.scoring_window_ms;
    let tape_slices = deps.filter_config
        .scoring
        .buyer_velocity_slices
        .max(1);
    let momentum_low_pct = deps.filter_config
        .scoring
        .thresholds
        .momentum_good_low_pct;
    let tape_observe = features::observe_early_tape_points_live(
        &deps.launchpad_for_score,
        mint,
        window_ms,
        tape_slices,
        Some(momentum_low_pct),
    )
    .await;
    eprintln!(
        "[LATENCY] {} stage=scoring_window ms={} window_ms={} exited_early={}",
        mint,
        pipeline_t0.elapsed().as_millis(),
        window_ms,
        tape_observe.exited_early,
    );
    let tape_points = tape_observe.points;

    let scoring_bucket = tape_observe
        .last_bucket
        .clone()
        .unwrap_or(bucket_for_score.clone());

    let pool_mcap = |b: &crate::launchpads::token_bucket::TokenBucket| {
        b.pool().market_cap().amount().to_float()
    };
    let initial_mcap_sol = if tape_observe.initial_mcap_sol > 0.0 {
        tape_observe.initial_mcap_sol
    } else {
        pool_mcap(&scoring_bucket)
    };
    let current_mcap_sol = tape_observe.current_mcap_sol;
    let peak_mcap_sol = tape_observe.peak_mcap_sol.max(current_mcap_sol);

    let merged_thr = merge_thresholds(
        &deps.filter_config.scoring.thresholds,
        &deps.learning_overrides.read().await.patch,
    );
    let thr_snapshot = if deps.filter_config.scoring.legacy_scoring {
        &deps.filter_config.scoring.thresholds
    } else {
        &merged_thr
    };

    // --- Stage 3: snapshot features ---------------------
    let (early_buyers, _buy_sizes_sol, buy_volume_sol, still_long, sold, bundle) =
        features::snapshot_early_buyers(&scoring_bucket, thr_snapshot).await;

    let (dev_category, dev_record) = if dev_stats.is_none()
        && deps.filter_config.scoring.tier_b.enabled
        && !is_spam_dev
    {
        (DevCategory::Fresh, DevRecord::default())
    } else {
        deps.dev_ranker.category(dev).await
    };
    let buyers_for_position = early_buyers.all();
    let smart_count = deps.smart_money
        .count_smart(buyers_for_position.clone())
        .await;

    let smart_addrs = deps.smart_money
        .filter_smart_wallets(buyers_for_position.clone())
        .await;
    let mut smart_wallet_early_exits: u32 = 0;
    for a in smart_addrs {
        if let Some(t) = scoring_bucket.swarm().get_trader(a).await {
            if t.holdings().raw() == 0 && t.total_spent().raw() > 0 {
                smart_wallet_early_exits += 1;
            }
        }
    }

    let tape = features::ScoringTapeDerived::from_tape_points(
        &tape_points,
        smart_wallet_early_exits,
    );

    let regular_buyer_count = early_buyers.regulars.len() as u64;
    let sniper_count = early_buyers.snipers.len() as u64;

    let mut token_features = features::assemble(
        mint,
        dev,
        dev_stats.as_ref(),
        dev_category,
        dev_record,
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
    token_features.is_spam_dev = is_spam_dev;

    let engine = ScoreEngine::new(&deps.filter_config.scoring);
    let thr_score = if deps.filter_config.scoring.legacy_scoring {
        &deps.filter_config.scoring.thresholds
    } else {
        &merged_thr
    };
    let breakdown = engine.score(&token_features, thr_score);

    eprintln!(
        "[SCORE] {} tier={:?} score={} buyers={}+{} vol={:.2} \
         mcap_init={:.1} mcap_peak={:.1} mcap_now={:.1} early_exit={} \
         bundle_sim={:.2} \
         bundle_id={:.2} dev_cat={:?} smart={} bv_persist={:.2} \
         sell_press={:.2} absorb={:.2} dumps={} sm_exits={} items={:?}",
        mint,
        breakdown.tier,
        breakdown.total,
        regular_buyer_count,
        sniper_count,
        buy_volume_sol,
        initial_mcap_sol,
        peak_mcap_sol,
        current_mcap_sol,
        tape_observe.exited_early,
        token_features.bundle.similar_size_ratio,
        token_features.bundle.identical_size_ratio,
        dev_category,
        smart_count,
        token_features.buyer_velocity_persistence,
        token_features.sell_pressure_score,
        token_features.absorb_quality_score,
        token_features.repeat_dump_slices,
        token_features.smart_wallet_early_exits,
        breakdown.items,
    );

    if matches!(breakdown.tier, Tier::Skip) {
        deps.bot_metrics.note_score_skip();
        let fresh_b_lane = features::tier_b_dev_eligible(
            &token_features,
            &deps.filter_config.scoring.tier_b,
        );
        let fail_reason = fresh_b_lane.then(|| {
            features::fresh_b_gate_fail_reason(
                &deps.filter_config.scoring.tier_b,
                &token_features,
                &breakdown.items,
            )
        }).flatten();
        if let Some(reason) = fail_reason {
            eprintln!(
                "[BUY] {} skipped (fresh_b_gate): {} | score={} smart={} \
                 buyers={} vol={:.2} dev_cat={:?}",
                mint,
                reason,
                breakdown.total,
                smart_count,
                token_features.buyer_count(),
                token_features.buy_volume_sol,
                token_features.dev_category,
            );
        }
        let (skip_stage, skip_reason) = if let Some(r) = fail_reason {
            ("fresh_b_gate", r)
        } else {
            ("score_skip", "tier_skip")
        };
        if let Some(ref log) = deps.learning_log {
            let log = log.clone();
            let mint_s = mint.to_string();
            let dev_s = dev.to_string();
            let snap = learning_snap(&token_features, &breakdown, dev_stats.as_ref(), from_fresh_watchlist);
            let payload = serde_json::to_value(&snap).unwrap_or_else(|_| serde_json::json!({}));
            let ts = unix_now();
            let stage = skip_stage.to_string();
            let reason = skip_reason.to_string();
            tokio::spawn(async move {
                let _ = log
                    .log_skipped(
                        &mint_s,
                        Some(dev_s.as_str()),
                        &stage,
                        &reason,
                        payload,
                        ts,
                    )
                    .await;
            });
        }
        return;
    }

    if features::tier_b_dev_eligible(
        &token_features,
        &deps.filter_config.scoring.tier_b,
    ) && breakdown.tier == Tier::B
    {
        if breakdown.fresh_b_hot_override {
            eprintln!(
                "Fresh Hot Override Passed {} fresh_b_hot_override=true \
                 reason=momentum_overheated_only buyers={} buy_volume_sol={:.2} \
                 velocity_pct={:.1} momentum_overheated={} tier=B",
                mint,
                token_features.buyer_count(),
                token_features.buy_volume_sol,
                features::fresh_b_velocity_pct(&token_features),
                features::has_momentum_overheated(&breakdown.items),
            );
        }
        if let Some(sub) = FreshBSubtype::for_path(dev_stats.as_ref(), from_fresh_watchlist) {
            eprintln!(
                "[SCORE] {} tier=B fresh_b_subtype={} score={} (fresh dev — A/A+ blocked)",
                mint,
                sub.as_str(),
                breakdown.total,
            );
        } else {
            eprintln!(
                "[SCORE] {} fresh_dev_cap: tier=B score={} (fresh dev — A/A+ blocked)",
                mint,
                breakdown.total,
            );
        }
    }

    // Live-only gates: avoid A-tier noise entries with flat
    // mcap (no `momentum_good`) and optionally require A+.
    if deps.filter_config.execution.mode == ExecutionMode::Live {
        let has_momentum_good = breakdown
            .items
            .iter()
            .any(|(name, _)| *name == "momentum_good");

        // Strong smart money is itself a momentum signal: let
        // such tokens reach the continuation layer instead of
        // being cut here for a missing `momentum_good` item.
        let smart_bypass = deps.filter_config.scoring.momentum_good_smart_bypass;
        // A+ specific, stricter-scoped bypass: a top-tier (A+)
        // smart setup with >= configured smart wallets is given
        // a chance at the continuation/parabolic layer rather
        // than being cut here. Weak A (score 6-7, smart=0, dev
        // Bad) is intentionally NOT loosened.
        let aplus_smart_bypass =
            deps.filter_config.scoring.momentum_good_aplus_smart_bypass;
        let aplus_smart_ok = aplus_smart_bypass > 0
            && breakdown.tier == Tier::APlus
            && smart_count >= aplus_smart_bypass;
        let strong_a_ok = features::strong_a_momentum_bypass_ok(
            &deps.filter_config.scoring.momentum_good_strong_a,
            breakdown.tier,
            breakdown.total,
            &token_features,
            &breakdown.items,
        );
        let momentum_good_satisfied = has_momentum_good
            || (smart_bypass > 0 && smart_count >= smart_bypass)
            || aplus_smart_ok
            || strong_a_ok
            || (breakdown.tier == Tier::B && breakdown.fresh_b_hot_override);

        if breakdown.tier == Tier::B && breakdown.fresh_b_hot_override && !has_momentum_good {
            eprintln!(
                "[BUY] {} tier=B fresh_b_hot_override bypass require_momentum_good \
                 (reason=momentum_overheated_only)",
                mint,
            );
        }

        if strong_a_ok && !has_momentum_good {
            eprintln!(
                "[BUY] {} strong_A bypass momentum_good: score={} \
                 buyers={} vol={:.1} b2s={:.2} absorb={:.2} bv_persist={:.2}",
                mint,
                breakdown.total,
                token_features.buyer_count(),
                token_features.buy_volume_sol,
                token_features.buy_to_sell_ratio,
                token_features.absorb_quality_score,
                token_features.buyer_velocity_persistence,
            );
        }

        if deps.filter_config.scoring.require_momentum_good && !momentum_good_satisfied {
            eprintln!(
                "[BUY] {} skipped (live): require_momentum_good=true but no \
                 momentum_good in items={:?}",
                mint,
                breakdown.items
            );
            if let Some(ref log) = deps.learning_log {
                let log = log.clone();
                let mint_s = mint.to_string();
                let dev_s = dev.to_string();
                let snap =
                    learning_snap(&token_features, &breakdown, dev_stats.as_ref(), from_fresh_watchlist);
                let payload =
                    serde_json::to_value(&snap).unwrap_or_else(|_| serde_json::json!({}));
                let ts = unix_now();
                tokio::spawn(async move {
                    let _ = log
                        .log_skipped(
                            &mint_s,
                            Some(dev_s.as_str()),
                            "live_gate_momentum",
                            "require_momentum_good",
                            payload,
                            ts,
                        )
                        .await;
                });
            }
            return;
        }

        if let Some(reason) = entry_skip_reason(
            &token_features,
            &deps.filter_config.scoring.anti_rug,
        ) {
            eprintln!(
                "[BUY] {} skipped (anti_rug): {} | sell_vol={:.2} buy_vol={:.2} \
                 sp={:.3} b2s={:.1}",
                mint,
                reason,
                token_features.sell_volume_window_sol,
                token_features.buy_volume_sol,
                token_features.sell_pressure_score,
                token_features.buy_to_sell_ratio,
            );
            if let Some(ref log) = deps.learning_log {
                let log = log.clone();
                let mint_s = mint.to_string();
                let dev_s = dev.to_string();
                let snap =
                    learning_snap(&token_features, &breakdown, dev_stats.as_ref(), from_fresh_watchlist);
                let payload =
                    serde_json::to_value(&snap).unwrap_or_else(|_| serde_json::json!({}));
                let ts = unix_now();
                tokio::spawn(async move {
                    let _ = log
                        .log_skipped(
                            &mint_s,
                            Some(dev_s.as_str()),
                            "anti_rug",
                            reason,
                            payload,
                            ts,
                        )
                        .await;
                });
            }
            return;
        }

        if deps.filter_config.scoring.minimum_tier_for_buy == MinBuyTier::APlus
            && breakdown.tier != Tier::APlus
            && breakdown.tier != Tier::B
        {
            eprintln!(
                "[BUY] {} skipped (live): minimum_tier_for_buy=APlus but tier={:?}",
                mint,
                breakdown.tier
            );
            if let Some(ref log) = deps.learning_log {
                let log = log.clone();
                let mint_s = mint.to_string();
                let dev_s = dev.to_string();
                let snap =
                    learning_snap(&token_features, &breakdown, dev_stats.as_ref(), from_fresh_watchlist);
                let payload =
                    serde_json::to_value(&snap).unwrap_or_else(|_| serde_json::json!({}));
                let ts = unix_now();
                tokio::spawn(async move {
                    let _ = log
                        .log_skipped(
                            &mint_s,
                            Some(dev_s.as_str()),
                            "live_gate_tier",
                            "minimum_tier_APlus",
                            payload,
                            ts,
                        )
                        .await;
                });
            }
            return;
        }

        // Spam-dev tape gate: prolific serial launchers
        // bypassed the creator-stats hard filter, so we only
        // let them buy when the tape is exceptional (A+),
        // never plain A. This keeps the rare real runners
        // from such devs without re-admitting the trash.
        if is_spam_dev
            && deps.filter_config.scoring.spam_dev_require_a_plus
            && breakdown.tier != Tier::APlus
        {
            eprintln!(
                "[BUY] {} skipped (spam_dev): tier={:?} but spam devs require A+",
                mint,
                breakdown.tier
            );
            deps.bot_metrics.note_spam_dev_skip();
            if let Some(ref log) = deps.learning_log {
                let log = log.clone();
                let mint_s = mint.to_string();
                let dev_s = dev.to_string();
                let snap =
                    learning_snap(&token_features, &breakdown, dev_stats.as_ref(), from_fresh_watchlist);
                let payload =
                    serde_json::to_value(&snap).unwrap_or_else(|_| serde_json::json!({}));
                let ts = unix_now();
                tokio::spawn(async move {
                    let _ = log
                        .log_skipped(
                            &mint_s,
                            Some(dev_s.as_str()),
                            "spam_dev_weak",
                            "spam_dev_require_a_plus",
                            payload,
                            ts,
                        )
                        .await;
                });
            }
            return;
        }
    }

    if deps.filter_config.execution.mode == ExecutionMode::Live {
        if let Some(reason) = features::weak_a_hard_skip_reason(
            &deps.filter_config.scoring.weak_a_gate,
            breakdown.tier,
            breakdown.total,
            smart_count,
            &token_features,
            &breakdown.items,
        ) {
            eprintln!(
                "[BUY] {} skipped (weak_a_gate): {} | tier={:?} score={} smart={} \
                 vol={:.2} dumps={} dev_cat={:?}",
                mint,
                reason,
                breakdown.tier,
                breakdown.total,
                smart_count,
                token_features.buy_volume_sol,
                token_features.repeat_dump_slices,
                token_features.dev_category,
            );
            if let Some(ref log) = deps.learning_log {
                let log = log.clone();
                let mint_s = mint.to_string();
                let dev_s = dev.to_string();
                let snap = learning_snap(
                    &token_features,
                    &breakdown,
                    dev_stats.as_ref(),
                    from_fresh_watchlist,
                );
                let payload = serde_json::to_value(&snap)
                    .unwrap_or_else(|_| serde_json::json!({}));
                let ts = unix_now();
                tokio::spawn(async move {
                    let _ = log
                        .log_skipped(
                            &mint_s,
                            Some(dev_s.as_str()),
                            "weak_a_gate",
                            reason,
                            payload,
                            ts,
                        )
                        .await;
                });
            }
            return;
        }

        if let Some(reason) = features::weak_a_synthetic_pump_skip_reason(
            &deps.filter_config.scoring.weak_a_gate.synthetic_pump,
            breakdown.tier,
            smart_count,
            &token_features,
            &breakdown.items,
        ) {
            eprintln!(
                "[BUY] {} skipped (weak_a_synthetic_pump): {} | tier={:?} score={} \
                 smart={} b2s={:.1} sell_vol={:.2}",
                mint,
                reason,
                breakdown.tier,
                breakdown.total,
                smart_count,
                token_features.buy_to_sell_ratio,
                token_features.sell_volume_window_sol,
            );
            if let Some(ref log) = deps.learning_log {
                let log = log.clone();
                let mint_s = mint.to_string();
                let dev_s = dev.to_string();
                let snap = learning_snap(
                    &token_features,
                    &breakdown,
                    dev_stats.as_ref(),
                    from_fresh_watchlist,
                );
                let payload = serde_json::to_value(&snap)
                    .unwrap_or_else(|_| serde_json::json!({}));
                let ts = unix_now();
                tokio::spawn(async move {
                    let _ = log
                        .log_skipped(
                            &mint_s,
                            Some(dev_s.as_str()),
                            "weak_a_synthetic_pump",
                            reason,
                            payload,
                            ts,
                        )
                        .await;
                });
            }
            return;
        }

        if let Some(reason) = features::aplus_rug_gate_skip_reason(
            breakdown.tier,
            smart_count,
            token_features.buy_to_sell_ratio,
        ) {
            eprintln!(
                "[BUY] {} skipped (aplus_rug_gate): {} | tier={:?} score={} \
                 smart={} b2s={:.1}",
                mint,
                reason,
                breakdown.tier,
                breakdown.total,
                smart_count,
                token_features.buy_to_sell_ratio,
            );
            if let Some(ref log) = deps.learning_log {
                let log = log.clone();
                let mint_s = mint.to_string();
                let dev_s = dev.to_string();
                let snap = learning_snap(
                    &token_features,
                    &breakdown,
                    dev_stats.as_ref(),
                    from_fresh_watchlist,
                );
                let payload = serde_json::to_value(&snap)
                    .unwrap_or_else(|_| serde_json::json!({}));
                let ts = unix_now();
                tokio::spawn(async move {
                    let _ = log
                        .log_skipped(
                            &mint_s,
                            Some(dev_s.as_str()),
                            "aplus_rug_gate",
                            reason,
                            payload,
                            ts,
                        )
                        .await;
                });
            }
            return;
        }

        let velocity_pct = features::momentum_peak_pct(
            token_features.initial_mcap_sol,
            token_features
                .peak_mcap_sol
                .max(token_features.current_mcap_sol),
        );
        if let Some(reason) =
            features::tier_a_low_velocity_skip_reason(breakdown.tier, velocity_pct)
        {
            eprintln!(
                "[BUY] {} skipped (velocity_gate): {} | tier={:?} score={} \
                 velocity_pct={:.2} smart={} buyers={} vol={:.2}",
                mint,
                reason,
                breakdown.tier,
                breakdown.total,
                velocity_pct,
                smart_count,
                token_features.buyer_count(),
                token_features.buy_volume_sol,
            );
            if let Some(ref log) = deps.learning_log {
                let log = log.clone();
                let mint_s = mint.to_string();
                let dev_s = dev.to_string();
                let snap = learning_snap(
                    &token_features,
                    &breakdown,
                    dev_stats.as_ref(),
                    from_fresh_watchlist,
                );
                let mut payload = serde_json::to_value(&snap)
                    .unwrap_or_else(|_| serde_json::json!({}));
                if let Some(obj) = payload.as_object_mut() {
                    obj.insert(
                        "velocity_pct".into(),
                        serde_json::json!(velocity_pct),
                    );
                }
                let ts = unix_now();
                tokio::spawn(async move {
                    let _ = log
                        .log_skipped(
                            &mint_s,
                            Some(dev_s.as_str()),
                            "velocity_gate",
                            reason,
                            payload,
                            ts,
                        )
                        .await;
                });
            }
            return;
        }
    }

    match breakdown.tier {
        Tier::A => deps.bot_metrics.note_score_a(),
        Tier::APlus => deps.bot_metrics.note_score_a_plus(),
        Tier::B => deps.bot_metrics.note_score_b(),
        Tier::Skip => unreachable!("tier Skip filtered above"),
    }

    // --- Stage 4: dispatch to manager (which still
    // applies the StrategyController gate) -------------
    let operator_cap =
        f64::from_bits(deps.buy_cap.load(std::sync::atomic::Ordering::Relaxed));
    let amount_sol = breakdown
        .recommended_size_sol
        .min(operator_cap)
        .max(0.0);
    if amount_sol <= f64::EPSILON {
        eprintln!(
            "[BUY] {} skipped: tier size {:.4} capped to {:.4} (operator cap)",
            mint, breakdown.recommended_size_sol, operator_cap
        );
        if let Some(ref log) = deps.learning_log {
            let log = log.clone();
            let mint_s = mint.to_string();
            let dev_s = dev.to_string();
            let snap = learning_snap(&token_features, &breakdown, dev_stats.as_ref(), from_fresh_watchlist);
            let payload = serde_json::to_value(&snap).unwrap_or_else(|_| serde_json::json!({}));
            let ts = unix_now();
            tokio::spawn(async move {
                let _ = log
                    .log_skipped(
                        &mint_s,
                        Some(dev_s.as_str()),
                        "size_zero",
                        "operator_cap",
                        payload,
                        ts,
                    )
                    .await;
            });
        }
        return;
    }
    eprintln!(
        "[BUY GATE] {} tier={:?} tier_sol={:.6} operator_cap={:.6} final_amount_sol={:.6}",
        mint,
        breakdown.tier,
        breakdown.recommended_size_sol,
        operator_cap,
        amount_sol,
    );
    // --- Confirmation poll: Continuation (doc 2.1/2.2/2.3) +
    // Anti-parabolic peak gate (bought-the-top fix) ----------
    // After scoring + gates pass, observe one short confirm
    // window (shared by both gates) and abort transient /
    // fake-momentum and parabolic-peak entries.
    let cont_enabled = deps.filter_config.execution.mode == ExecutionMode::Live
        && deps.filter_config.scoring.continuation.enabled;
    let parab_enabled = deps.filter_config.execution.mode == ExecutionMode::Live
        && deps.filter_config.scoring.anti_parabolic.enabled;
    if cont_enabled || parab_enabled {
        let cont_cfg = &deps.filter_config.scoring.continuation;
        let parab_cfg = &deps.filter_config.scoring.anti_parabolic;
        let baseline_buyers = regular_buyer_count + sniper_count;
        // Use the continuation window when it is active; else
        // fall back to the anti-parabolic poll settings.
        let (poll_window_ms, poll_slices) = if cont_enabled {
            (cont_cfg.confirm_window_ms, cont_cfg.confirm_slices)
        } else {
            (parab_cfg.confirm_window_ms, parab_cfg.confirm_slices)
        };
        let confirm = features::observe_early_tape_points_live(
            &deps.launchpad_for_score,
            mint,
            poll_window_ms,
            poll_slices,
            None,
        )
        .await;

        // Continuation gate (transient / fake momentum).
        if cont_enabled {
            let is_a_plus = breakdown.tier == Tier::APlus;
            let sl_cfg = &cont_cfg.second_look;
            let peak_cfg = &cont_cfg.aplus_peak_guard;
            let peak_guard = features::aplus_peak_no_smart_guard(
                peak_cfg,
                is_a_plus,
                smart_count,
                current_mcap_sol,
                peak_mcap_sol,
            );
            let first_cont = features::evaluate_continuation(
                cont_cfg,
                token_features.buy_to_sell_ratio,
                baseline_buyers,
                &confirm.points,
                cont_cfg.confirm_window_ms,
                is_a_plus,
            );
            let mut continuation_skip_reason: Option<&'static str> = None;
            let needs_peak_defer = peak_guard
                && (first_cont.is_err()
                    || !features::continuation_confirm_strong(
                        peak_cfg,
                        &confirm.points,
                        baseline_buyers,
                    ));

            if needs_peak_defer {
                let first_note = match &first_cont {
                    Ok(()) => "first_pass_weak",
                    Err(r) => r,
                };
                eprintln!(
                    "[BUY] {} A+ peak no-smart guard: deferring ({}) \
                     smart={} mcap_now={:.1} mcap_peak={:.1} wait_ms={}",
                    mint,
                    first_note,
                    smart_count,
                    current_mcap_sol,
                    peak_mcap_sol,
                    sl_cfg.wait_ms,
                );
                tokio::time::sleep(std::time::Duration::from_millis(
                    sl_cfg.wait_ms.max(1),
                ))
                .await;
                let recheck = features::observe_early_tape_points_live(
                    &deps.launchpad_for_score,
                    mint,
                    sl_cfg.recheck_window_ms,
                    sl_cfg.recheck_slices,
                    None,
                )
                .await;
                let second_cont = features::evaluate_continuation(
                    cont_cfg,
                    token_features.buy_to_sell_ratio,
                    baseline_buyers,
                    &recheck.points,
                    cont_cfg.confirm_window_ms,
                    is_a_plus,
                );
                match second_cont {
                    Ok(()) => {
                        if features::aplus_peak_recheck_mcap_acceptable(
                            peak_cfg,
                            peak_mcap_sol,
                            current_mcap_sol,
                            recheck.current_mcap_sol,
                        ) {
                            eprintln!(
                                "[BUY] {} A+ peak guard recheck passed: \
                                 mcap_now={:.1}",
                                mint,
                                recheck.current_mcap_sol,
                            );
                        } else {
                            eprintln!(
                                "[BUY] {} A+ peak guard recheck rejected: \
                                 mcap_now={:.1} < {:.0}% of score peak {:.1} \
                                 (score_now={:.1})",
                                mint,
                                recheck.current_mcap_sol,
                                peak_cfg.recheck_min_vs_peak_ratio * 100.0,
                                peak_mcap_sol,
                                current_mcap_sol,
                            );
                            continuation_skip_reason =
                                Some("aplus_peak_recheck_mcap_drop");
                        }
                    }
                    Err(reason) => {
                        if features::continuation_second_look_eligible_for_buy(
                            sl_cfg,
                            &deps.filter_config.scoring.weak_a_gate,
                            breakdown.tier,
                            is_a_plus,
                            breakdown.total,
                            smart_count,
                            &token_features,
                            &breakdown.items,
                            reason,
                        ) {
                            match features::evaluate_continuation_second_look(
                                recheck.current_mcap_sol,
                                baseline_buyers,
                                &recheck.points,
                            ) {
                                Ok(()) => {
                                    if features::aplus_peak_recheck_mcap_acceptable(
                                        peak_cfg,
                                        peak_mcap_sol,
                                        current_mcap_sol,
                                        recheck.current_mcap_sol,
                                    ) {
                                        eprintln!(
                                            "[BUY] {} A+ peak guard recovery \
                                             passed (was {}): mcap_now={:.1}",
                                            mint,
                                            reason,
                                            recheck.current_mcap_sol,
                                        );
                                    } else {
                                        eprintln!(
                                            "[BUY] {} A+ peak guard recovery \
                                             rejected (mcap collapse): \
                                             mcap_now={:.1} score_peak={:.1}",
                                            mint,
                                            recheck.current_mcap_sol,
                                            peak_mcap_sol,
                                        );
                                        continuation_skip_reason = Some(
                                            "aplus_peak_recheck_mcap_drop",
                                        );
                                    }
                                }
                                Err(sl_reason) => {
                                    continuation_skip_reason =
                                        Some(sl_reason);
                                }
                            }
                        } else {
                            continuation_skip_reason = Some(reason);
                        }
                    }
                }
            } else if let Err(reason) = first_cont {
                if features::continuation_second_look_eligible_for_buy(
                    sl_cfg,
                    &deps.filter_config.scoring.weak_a_gate,
                    breakdown.tier,
                    is_a_plus,
                    breakdown.total,
                    smart_count,
                    &token_features,
                    &breakdown.items,
                    reason,
                ) {
                    eprintln!(
                        "[BUY] {} continuation second-look: deferring {} \
                         (tier={:?} score={}) wait_ms={}",
                        mint,
                        reason,
                        breakdown.tier,
                        breakdown.total,
                        sl_cfg.wait_ms,
                    );
                    tokio::time::sleep(std::time::Duration::from_millis(
                        sl_cfg.wait_ms.max(1),
                    ))
                    .await;
                    let ref_mcap = confirm.current_mcap_sol;
                    let recheck = features::observe_early_tape_points_live(
                        &deps.launchpad_for_score,
                        mint,
                        sl_cfg.recheck_window_ms,
                        sl_cfg.recheck_slices,
                        None,
                    )
                    .await;
                    match features::evaluate_continuation_second_look(
                        ref_mcap,
                        baseline_buyers,
                        &recheck.points,
                    ) {
                        Ok(()) => {
                            let (upticks, new_buyers) =
                                features::continuation_strength(
                                    &recheck.points,
                                    baseline_buyers,
                                );
                            eprintln!(
                                "[BUY] {} continuation second-look passed \
                                 (was {}): ref_mcap={:.1} mcap_now={:.1} \
                                 upticks={} new_buyers={}",
                                mint,
                                reason,
                                ref_mcap,
                                recheck.current_mcap_sol,
                                upticks,
                                new_buyers,
                            );
                        }
                        Err(sl_reason) => {
                            continuation_skip_reason =
                                Some(sl_reason);
                        }
                    }
                } else {
                    continuation_skip_reason = Some(reason);
                }
            }
            if let Some(reason) = continuation_skip_reason {
                eprintln!(
                    "[BUY] {} skipped (continuation): {} | mcap_init={:.1} \
                     mcap_now={:.1} baseline_buyers={} b2s={:.2}",
                    mint,
                    reason,
                    confirm.initial_mcap_sol,
                    confirm.current_mcap_sol,
                    baseline_buyers,
                    token_features.buy_to_sell_ratio,
                );
                deps.bot_metrics.note_continuation_skip();
                if let Some(ref log) = deps.learning_log {
                    let log = log.clone();
                    let mint_s = mint.to_string();
                    let dev_s = dev.to_string();
                    let snap = learning_snap(
                        &token_features,
                        &breakdown,
                        dev_stats.as_ref(),
                        from_fresh_watchlist,
                    );
                    let payload = serde_json::to_value(&snap)
                        .unwrap_or_else(|_| serde_json::json!({}));
                    let ts = unix_now();
                    tokio::spawn(async move {
                        let _ = log
                            .log_skipped(
                                &mint_s,
                                Some(dev_s.as_str()),
                                "continuation",
                                reason,
                                payload,
                                ts,
                            )
                            .await;
                    });
                }
                return;
            }
        }

        // Anti-parabolic peak gate: weak A-tier, no smart
        // money, entered at the local peak, without strong
        // fresh demand in the confirm window. A+ / smart /
        // strong-continuation setups are exempt by design.
        if parab_enabled
            && features::parabolic_peak_suspect(
                parab_cfg,
                breakdown.tier == Tier::APlus,
                breakdown.total,
                smart_count,
                current_mcap_sol,
                peak_mcap_sol,
            )
        {
            let (upticks, new_buyers) =
                features::continuation_strength(&confirm.points, baseline_buyers);
            let strong = upticks >= parab_cfg.strong_upticks
                && new_buyers >= parab_cfg.strong_new_buyers;
            if !strong {
                eprintln!(
                    "[BUY] {} skipped (parabolic_peak_entry): score={} smart={} \
                     mcap_now={:.1} mcap_peak={:.1} upticks={} new_buyers={}",
                    mint,
                    breakdown.total,
                    smart_count,
                    current_mcap_sol,
                    peak_mcap_sol,
                    upticks,
                    new_buyers,
                );
                deps.bot_metrics.note_parabolic_skip();
                if let Some(ref log) = deps.learning_log {
                    let log = log.clone();
                    let mint_s = mint.to_string();
                    let dev_s = dev.to_string();
                    let snap = learning_snap(
                        &token_features,
                        &breakdown,
                        dev_stats.as_ref(),
                        from_fresh_watchlist,
                    );
                    let payload = serde_json::to_value(&snap)
                        .unwrap_or_else(|_| serde_json::json!({}));
                    let ts = unix_now();
                    tokio::spawn(async move {
                        let _ = log
                            .log_skipped(
                                &mint_s,
                                Some(dev_s.as_str()),
                                "parabolic_peak_entry",
                                "weak_peak_no_demand",
                                payload,
                                ts,
                            )
                            .await;
                    });
                }
                return;
            }
        }
    }

    let learning_snapshot = learning_snap(
        &token_features,
        &breakdown,
        dev_stats.as_ref(),
        from_fresh_watchlist,
    );
    let open_reason = match dev_stats {
        Some(s) => OpenReason::DevStats(s),
        None => OpenReason::TraderStats,
    };

    deps.buy_latency.on_score_done(mint);
    eprintln!(
        "[LATENCY] {} stage=pre_initiate_buy ms={} (created в†’ InitiateBuy; \
         remaining buy_fanout stagger applies only to wallet 2+)",
        mint,
        pipeline_t0.elapsed().as_millis(),
    );

    if deps.manager_tx
        .send(PositionMessage::InitiateBuy {
            pool: scoring_bucket.pool().clone_box(),
            amount_sol,
            buy_tier: breakdown.tier,
            open_reason,
            dev_address: Some(dev),
            early_buyers: buyers_for_position,
            learning_snapshot: Some(learning_snapshot),
        })
        .await
        .is_ok()
    {
        deps.bot_metrics.note_position_initiated();
    }

}
