//! Periodic analysis: adjust `FeatureThresholdPatch` conservatively, persist JSON,
//! and publish into the shared `RwLock` used at score time.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use tokio::sync::RwLock;

use crate::autobuy::filters::config::LearningConfig;
use crate::learning::db::{LearningLogPg, TradeBatchStats};
use crate::learning::merge::{save_patch, FeatureThresholdPatch, LearningOverridesFile};
use crate::scoring::config::FeatureThresholds;

fn now_unix() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn apply_heuristics(
    base: &FeatureThresholds,
    stats: &TradeBatchStats,
    patch: &mut FeatureThresholdPatch,
) {
    if stats.n < 2 {
        return;
    }
    let n = stats.n as f64;
    let winrate = stats.wins as f64 / n;

    if winrate < 0.48 {
        let target = patch.buyers_low.unwrap_or(base.buyers_low).saturating_add(1);
        patch.buyers_low = Some(target);
    } else if winrate > 0.62 {
        let target = patch
            .buyers_low
            .unwrap_or(base.buyers_low)
            .saturating_sub(1)
            .max(1);
        patch.buyers_low = Some(target);
    }

    if winrate < 0.52 && stats.win_smart > stats.loss_smart + 0.75 {
        let cur = patch
            .smart_wallet_3plus_min
            .unwrap_or(base.smart_wallet_3plus_min);
        patch.smart_wallet_3plus_min = Some((cur + 1).min(8));
    }

    if stats.loss_buyers > 0.0 && stats.win_buyers > 0.0 && stats.loss_buyers > stats.win_buyers + 1.5 {
        let m = patch.volume_ok_sol_mult.unwrap_or(1.0) * 1.05;
        patch.volume_ok_sol_mult = Some(m.min(1.15));
    }
}

/// Background loop: periodically considers whether enough new trades accumulated,
/// then refreshes the persisted patch + in-memory `RwLock`.
pub fn spawn_learning_engine(
    db: LearningLogPg,
    cfg: LearningConfig,
    path: String,
    base_thresholds: FeatureThresholds,
    overrides: Arc<RwLock<LearningOverridesFile>>,
) {
    if !cfg.enabled {
        return;
    }
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(
            cfg.analyze_interval_secs.max(60),
        ));
        let mut last_trade_count_at_optimize: i64 = 0;

        loop {
            ticker.tick().await;
            let cnt = match db.count_trades().await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("[learning] count_trades: {e}");
                    continue;
                }
            };
            if (cnt - last_trade_count_at_optimize) < cfg.analyze_every_trades as i64 {
                continue;
            }

            let stats = match db.stats_all().await {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("[learning] stats_all: {e}");
                    continue;
                }
            };

            if stats.n < cfg.min_trades_for_update {
                last_trade_count_at_optimize = cnt;
                continue;
            }

            let mut file = overrides.read().await.clone();
            apply_heuristics(&base_thresholds, &stats, &mut file.patch);
            file.last_optimized_unix = now_unix();
            file.last_sample_size = stats.n;

            if let Err(e) = save_patch(&path, &file).await {
                eprintln!("[learning] save_patch: {e}");
                continue;
            }
            {
                let mut w = overrides.write().await;
                *w = file;
            }
            last_trade_count_at_optimize = cnt;
            eprintln!(
                "[learning] updated overrides (n={} winrate={:.2} patch={:?})",
                stats.n,
                stats.wins as f64 / stats.n as f64,
                overrides.read().await.patch
            );
        }
    });
}
