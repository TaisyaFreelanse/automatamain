use dotenvy::dotenv;
use futures::{SinkExt, StreamExt};
use loggaper::{
    autobuy::{
        wallet_registry::{build_wallet_registry, WalletRegistry, WalletWire},
        manager::{
            OpenPositionWire, PositionManagerActor, PositionMessage, WsCommand,
            WsFeedMessage,
        },
        performance_tracker::{CreatorRegistryHandle, PerformanceTrackerHandle},
    },
    feed::metrics::{
        BotMetrics, BotSnapshot, FeedHealthEvent, FeedHealthMonitor, FeedMetrics, FeedSnapshot,
        FEED_STALL_MSG_AGE_SECS, FEED_ZERO_RATE_STREAK, new_dedup,
    },
    generalize::general_commands::Action,
    learning::{
        load_patch, spawn_learning_engine, LearningLogPg,
    },
    persistence::{
        bot_trades::BotTradeRow,
        creators::CreatorRepository,
        postgres::creators::CreatorsRepositoryPostgres,
        tokens::TokenRepository,
        traders::{TraderEntry, TraderRepository},
        write_queue::PersistenceWriteQueue,
    },
    telemetry::buy_latency::BuyLatencyRegistry,
    pipelines::pump::PumpPipeline,
    scoring::{
        dev_ranker::{self, DevRankerHandle, DevRankerSnapshot},
        smart_money::{self, SmartMoneyHandle, SmartMoneySnapshot},
        strategy_controller::StrategySnapshot,
    },
    setup::{
        load_config, setup_crypto, setup_logging, setup_postgres_pool, setup_repositories,
        setup_solana_rpc, waiter::DatabaseCreateWaiter,
    },
};
use std::sync::Arc;
use std::{
    collections::HashMap,
    sync::atomic::Ordering,
    time::{Instant, SystemTime},
};
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc, RwLock},
};
use tokio_tungstenite::accept_async;

/// Solana mainnet ~2.5 slots per second (≈400 ms slot time).
const CHART_SLOTS_PER_SEC: f64 = 2.5;
/// Show bonding-curve context before bot entry (terminal-style pre-migration view).
const CHART_PRE_SECS: i64 = 300;
const CHART_POST_SECS: i64 = 120;
/// Max chart span from first sample (avoid multi-hour tails on dead mints).
const CHART_MAX_SPAN_SECS: i64 = 3600;

const CHART_MCAP_ABS_MAX: f64 = 200_000.0;

fn chart_mcap_valid(mcap: f64) -> bool {
    mcap.is_finite() && mcap > 0.0 && mcap <= CHART_MCAP_ABS_MAX
}

fn chart_tape_median(mcaps: &[f64]) -> Option<f64> {
    let mut v: Vec<f64> = mcaps
        .iter()
        .copied()
        .filter(|m| chart_mcap_valid(*m))
        .collect();
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Some(v[v.len() / 2])
}

fn chart_mcap_matches_tape(mcap: f64, median: Option<f64>) -> bool {
    if !chart_mcap_valid(mcap) {
        return false;
    }
    let Some(med) = median else {
        return true;
    };
    if med <= 0.0 {
        return true;
    }
    mcap >= med * 0.02 && mcap <= med * 50.0
}

fn chart_is_unix_secs(t: i64) -> bool {
    t >= 1_000_000_000
}

fn chart_slot_to_sec(slot: i64, slot0: i64) -> i64 {
    let delta = slot.saturating_sub(slot0);
    ((delta as f64) / CHART_SLOTS_PER_SEC).round().max(0.0) as i64
}

fn chart_infer_slot_by_mcap(target: f64, series: &[(i64, f64)]) -> i64 {
    series
        .iter()
        .filter(|(_, m)| chart_mcap_valid(*m))
        .min_by(|a, b| {
            (a.1 - target)
                .abs()
                .partial_cmp(&(b.1 - target).abs())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .map(|(slot, _)| *slot)
        .unwrap_or_else(|| series.first().map(|p| p.0).unwrap_or(0))
}

/// Marker times on the same axis as chart points (`unix_secs` or slot-derived secs).
fn chart_normalize_marker_secs(
    entry_at: i64,
    closed_at: i64,
    entry_mcap: f64,
    exit_mcap: f64,
    series: &[(i64, f64)],
    slot0: i64,
    timeline_unix: bool,
) -> (i64, i64) {
    let entry_unix = entry_at > 0 && chart_is_unix_secs(entry_at);
    let close_unix = closed_at > 0 && chart_is_unix_secs(closed_at);

    if timeline_unix && entry_unix && close_unix && closed_at >= entry_at {
        return (entry_at, closed_at);
    }

    let exit_slot = chart_infer_slot_by_mcap(exit_mcap, series);
    let exit_sec = chart_slot_to_sec(exit_slot, slot0);

    let entry_sec = if entry_at > 0 && !entry_unix {
        chart_slot_to_sec(entry_at, slot0)
    } else if entry_unix {
        entry_at
    } else {
        let entry_slot = chart_infer_slot_by_mcap(entry_mcap, series);
        chart_slot_to_sec(entry_slot, slot0)
    };

    let closed_sec = if closed_at > 0 && !close_unix {
        chart_slot_to_sec(closed_at, slot0)
    } else if close_unix {
        closed_at
    } else {
        exit_sec.max(entry_sec)
    };

    (entry_sec, closed_sec)
}

fn chart_dedup_points(mut points: Vec<(i64, f64)>) -> Vec<(i64, f64)> {
    points.sort_by_key(|(t, _)| *t);
    let mut out: Vec<(i64, f64)> = Vec::new();
    for (t, m) in points {
        if let Some(last) = out.last_mut() {
            if last.0 == t {
                last.1 = m;
                continue;
            }
        }
        out.push((t, m));
    }
    out
}

fn chart_trade_window_bounds(marker_times: &[(i64, i64)], points: &[(i64, f64)]) -> (i64, i64) {
    if !marker_times.is_empty() {
        let entry_min = marker_times.iter().map(|(e, _)| *e).min().unwrap_or(0);
        let exit_max = marker_times.iter().map(|(_, c)| *c).max().unwrap_or(entry_min);
        return (
            entry_min.saturating_sub(CHART_PRE_SECS),
            exit_max + CHART_POST_SECS,
        );
    }
    if let (Some(lo), Some(hi)) = (points.first().map(|p| p.0), points.last().map(|p| p.0)) {
        let span = hi.saturating_sub(lo);
        if span > CHART_MAX_SPAN_SECS {
            return (lo.saturating_sub(CHART_PRE_SECS), lo + CHART_MAX_SPAN_SECS);
        }
        return (lo.saturating_sub(CHART_PRE_SECS), hi + 60);
    }
    (0, CHART_POST_SECS)
}

#[tokio::main]
async fn main() {
    dotenv().ok();
    setup_crypto();
    setup_logging();
    let config = Arc::new(load_config().unwrap());
    let pool = setup_postgres_pool(30).await;
    let analytics_pool = setup_postgres_pool(8).await;
    let buy_latency = Arc::new(BuyLatencyRegistry::default());
    let (_creators_main, tokens, trades, bot_trades_pg, dev_blacklist_pg) =
        setup_repositories(pool.clone()).await;
    let creators = Arc::new(CreatorsRepositoryPostgres::new(analytics_pool));
    let bot_trades_pg = Arc::new(bot_trades_pg);
    let dev_blacklist: Arc<dyn loggaper::persistence::dev_blacklist::DevBlacklistRepository + Send + Sync> =
        Arc::new(dev_blacklist_pg);
    let tokens = Arc::new(tokens);
    let trades: Arc<dyn TraderRepository + Send + Sync> = Arc::new(trades);
    let write_queue = PersistenceWriteQueue::spawn(pool.clone(), trades.clone());
    let bot_trades: Arc<dyn loggaper::persistence::bot_trades::BotTradeRepository + Send + Sync> =
        bot_trades_pg.clone();
    let post_exit_repo: Arc<
        dyn loggaper::persistence::bot_trade_post_exit::BotTradePostExitRepository + Send + Sync,
    > = bot_trades_pg.clone();
    let post_exit_rpc = loggaper::autobuy::execution::build_post_exit_rpc();

    let learn_path = config.persistence.learning_overrides_path.clone();
    let learning_overrides = Arc::new(RwLock::new(
        load_patch(&learn_path).await,
    ));
    let learning_log = if config.learning.enabled {
        Some(LearningLogPg::new(pool.clone()))
    } else {
        None
    };

    #[derive(Clone)]
    struct ApiState {
        pool: sqlx::Pool<sqlx::Postgres>,
        creators: std::sync::Arc<CreatorsRepositoryPostgres>,
        paused: std::sync::Arc<std::sync::atomic::AtomicBool>,
        balance: std::sync::Arc<std::sync::atomic::AtomicU64>,
        buy_size: std::sync::Arc<std::sync::atomic::AtomicU64>,
        pubkey: String,
        wallet_registry: Arc<WalletRegistry>,
        mode: &'static str,
        feed_metrics: Arc<Vec<Arc<FeedMetrics>>>,
        bot_metrics: Arc<BotMetrics>,
        manager_tx: mpsc::Sender<PositionMessage>,
        dev_ranker: DevRankerHandle,
        smart_money: SmartMoneyHandle,
        tx_log: Arc<std::sync::Mutex<std::collections::VecDeque<WsFeedMessage>>>,
        config_path: String,
        live_cfg: loggaper::autobuy::execution::LiveExecutionConfig,
        /// Minimum allowed `PUT /buy-size` (and seed floor): at least 0.4 SOL and >= `a_plus_sol`.
        buy_cap_floor: f64,
        learning_log: Option<LearningLogPg>,
    }

    let (waiter_actor, waiter_handle) = DatabaseCreateWaiter::new();
    tokio::spawn(async move {
        waiter_actor.run().await;
    });

    let (ws_url, commitment_config) = setup_solana_rpc();
    let (general_tx, mut general_rx) = mpsc::channel(2048);
    let (broadcast_tx, _) = broadcast::channel::<WsFeedMessage>(4096);

    let wallet_registry = match build_wallet_registry(
        &config.wallets,
        &config.execution,
        config.start_balance_sol,
    )
    .await
    {
        Ok(r) => Arc::new(r),
        Err(e) => {
            eprintln!("[FATAL] Failed to build wallet registry: {e}");
            std::process::exit(1);
        }
    };
    let pubkey_string = wallet_registry.primary_pubkey();
    println!(
        "[BOOT] Wallets active: {} (mode={})",
        wallet_registry.all().len(),
        wallet_registry.mode_label()
    );
    if config.scoring.legacy_scoring {
        eprintln!(
            "[BOOT] scoring=legacy_pre_v2 (YAML thresholds for snapshot+score; learning merge ignored)"
        );
    }

    // Persistent dev ranking + smart-money registries. Both are actors that
    // own their own state and flush JSON to disk every N seconds.
    let dev_ranker_handle = dev_ranker::spawn(config.persistence.clone());
    let smart_money_handle = smart_money::spawn(config.persistence.clone());

    let initial_balance = wallet_registry.total_balance_sol();
    let (mut manager_actor, manager_tx, mut event_rx, paused_state, balance_state) =
        PositionManagerActor::new(
            wallet_registry.clone(),
            initial_balance,
            config.buy_config.clone(),
            bot_trades,
            dev_blacklist.clone(),
            config.dev_blacklist.clone(),
            config.curve_quarantine.clone(),
            post_exit_repo,
            post_exit_rpc,
            config.strategy.clone(),
            Some(dev_ranker_handle.clone()),
            Some(smart_money_handle.clone()),
            learning_log.clone(),
            Some(buy_latency.clone()),
        );

    if config.learning.enabled {
        if let Some(ref lg) = learning_log {
            spawn_learning_engine(
                lg.clone(),
                config.learning.clone(),
                learn_path,
                config.scoring.thresholds.clone(),
                learning_overrides.clone(),
            );
        }
    }

    // Operator buy cap (SOL): seeded from yaml `buy_config.amount_sol`, then
    // overridable at runtime via `PUT /buy-size` (dashboard). Each live buy
    // uses `min(score_engine recommended tier size, this cap)`.
    const BUY_CAP_ABSOLUTE_FLOOR_SOL: f64 = 0.4;
    let buy_cap_floor = BUY_CAP_ABSOLUTE_FLOOR_SOL.max(config.scoring.size.a_plus_sol);
    let buy_cap_seed = config.buy_config.amount_sol.max(buy_cap_floor);
    let buy_size_state = Arc::new(std::sync::atomic::AtomicU64::new(f64::to_bits(
        buy_cap_seed,
    )));
    if buy_cap_seed > config.buy_config.amount_sol + f64::EPSILON {
        eprintln!(
            "[BOOT] buy cap seed raised from yaml {:.4} to {:.4} SOL (floor {:.4}, a_plus_sol {:.4})",
            config.buy_config.amount_sol,
            buy_cap_seed,
            BUY_CAP_ABSOLUTE_FLOOR_SOL,
            config.scoring.size.a_plus_sol
        );
    }

    // Bounded ring buffer of recent tx events (buy / sell / failed) — both
    // demo and live. Surfaced via `GET /tx-log` and used by the dashboard.
    const TX_LOG_CAPACITY: usize = 200;
    let tx_log: Arc<std::sync::Mutex<std::collections::VecDeque<WsFeedMessage>>> =
        Arc::new(std::sync::Mutex::new(std::collections::VecDeque::with_capacity(
            TX_LOG_CAPACITY,
        )));

    let broadcast_tx_bridge = broadcast_tx.clone();
    let tx_log_bridge = tx_log.clone();
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            if matches!(event, WsFeedMessage::TxEvent { .. })
                && let Ok(mut log) = tx_log_bridge.lock() {
                if log.len() >= TX_LOG_CAPACITY {
                    log.pop_front();
                }
                log.push_back(event.clone());
            }
            let _ = broadcast_tx_bridge.send(event);
        }
    });

    tokio::spawn(async move {
        manager_actor.run().await;
    });

    let ticker_tx = manager_tx.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));
        loop {
            interval.tick().await;
            let _ = ticker_tx.send(PositionMessage::Tick).await;
        }
    });

    // Build feed-metrics infrastructure before starting the pipeline so that
    // both the pipeline and the HTTP /metrics route observe the same Arc.
    let pump_metrics = FeedMetrics::new("pump");
    let pumpswap_metrics = FeedMetrics::new("pumpswap");
    let feed_metrics_vec: Arc<Vec<Arc<FeedMetrics>>> =
        Arc::new(vec![pump_metrics.clone(), pumpswap_metrics.clone()]);
    let dedup = new_dedup(10_000);
    let bot_metrics = BotMetrics::new();

    // PumpSwap consumer is currently disabled (see src/pipelines/pump.rs),
    // so we also disable the WS subscription to stop wasting Helius credits
    // on a feed nothing reads. Flip the last argument to `true` to re-enable.
    let mut pump = PumpPipeline::init(
        ws_url,
        commitment_config.clone(),
        general_tx,
        3,
        false,
        pump_metrics.clone(),
        pumpswap_metrics.clone(),
        dedup.clone(),
        false,
    );
    let launchpad_tx = pump.launchpad().clone();
    tokio::spawn(async move { pump.run() });

    // Periodic feed-metrics logger with simple anomaly alerting: EMA of
    // messages/sec per feed, stall detection (last_msg_age / zero msgs/s),
    // and an [ALERT] line if the latest interval is more than 3x the EMA.
    // Keeps a single log line per feed every 30s.
    {
        let metrics_for_logger = feed_metrics_vec.clone();
        let bot_for_logger = bot_metrics.clone();
        tokio::spawn(async move {
            let interval_secs: u64 = 30;
            let mut last_messages: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();
            let mut ema_msgs: std::collections::HashMap<String, f64> =
                std::collections::HashMap::new();
            let mut feed_health = FeedHealthMonitor::new();
            let alpha = 0.3_f64;

            let mut ticker =
                tokio::time::interval(std::time::Duration::from_secs(interval_secs));
            ticker.tick().await; // skip first immediate tick
            loop {
                ticker.tick().await;
                for m in metrics_for_logger.iter() {
                    let snap = m.snapshot();
                    let prev = *last_messages.get(&snap.name).unwrap_or(&0);
                    let delta = snap.messages.saturating_sub(prev);
                    let rate = delta as f64 / interval_secs as f64;
                    last_messages.insert(snap.name.clone(), snap.messages);

                    let ema = ema_msgs.entry(snap.name.clone()).or_insert(rate);
                    let prior = *ema;
                    *ema = alpha * rate + (1.0 - alpha) * prior;

                    let last_msg_age_s = FeedHealthMonitor::last_msg_age_secs(&snap);

                    println!(
                        "[metrics:{}] msgs/s={:.1} ema={:.1} ev/s={:.1} \
                         bytes/s={:.0} drop_failed={} drop_npd={} drop_self_dup={} \
                         cross_dup={} parse_err={} useful={:.3} subs={} reconn={} \
                         idle_reconn={} last_msg_age_s={}",
                        snap.name,
                        rate,
                        *ema,
                        snap.events_per_sec_avg,
                        snap.bytes_per_sec_avg,
                        snap.dropped_failed_tx,
                        snap.dropped_no_program_data,
                        snap.dropped_self_dup,
                        snap.duplicates_cross_feed,
                        snap.parse_errors,
                        snap.useful_msg_ratio,
                        snap.subscribed,
                        snap.reconnects,
                        snap.idle_reconnects,
                        last_msg_age_s,
                    );

                    match feed_health.evaluate(&snap, rate) {
                        FeedHealthEvent::Stall => {
                            let zero_streak = feed_health.zero_rate_streak(&snap.name);
                            let mut reasons = Vec::new();
                            if last_msg_age_s > FEED_STALL_MSG_AGE_SECS {
                                reasons.push(format!(
                                    "last_msg_age_s={last_msg_age_s}>{FEED_STALL_MSG_AGE_SECS}"
                                ));
                            }
                            if zero_streak >= FEED_ZERO_RATE_STREAK {
                                reasons.push(format!(
                                    "msgs/s_zero_streak={zero_streak}>={FEED_ZERO_RATE_STREAK}"
                                ));
                            }
                            println!(
                                "[WARN:feed:{}] feed stall: {} | msgs/s={:.1} \
                                 reconn={} idle_reconn={} stream_err={} — \
                                 bot may miss new tokens; check WS/RPC",
                                snap.name,
                                reasons.join(", "),
                                rate,
                                snap.reconnects,
                                snap.idle_reconnects,
                                snap.stream_errors,
                            );
                        }
                        FeedHealthEvent::Recovered => {
                            println!(
                                "[OK:feed:{}] feed recovered: msgs/s={:.1} \
                                 last_msg_age_s={last_msg_age_s} reconn={}",
                                snap.name, rate, snap.reconnects,
                            );
                        }
                        FeedHealthEvent::Ok => {}
                    }

                    if prior > 1.0 && rate > prior * 3.0 {
                        println!(
                            "[ALERT:{}] message rate spike: {:.1}/s (ema {:.1}/s)",
                            snap.name, rate, prior
                        );
                    }
                }

                let b = bot_for_logger.snapshot();
                println!(
                    "[metrics:bot] creates={} no_history={} filter_rejected={} \
                     spam_dev_skipped={} passed_filter={} score_skip={} score_a={} \
                     score_a_plus={} score_b={} continuation_skipped={} parabolic_skipped={} \
                     strategy_blocked={} positions_initiated={}",
                    b.creates_total,
                    b.creates_no_history,
                    b.creates_filter_rejected,
                    b.spam_dev_skipped,
                    b.creates_passed_filter,
                    b.score_skipped,
                    b.score_a,
                    b.score_a_plus,
                    b.score_b,
                    b.continuation_skipped,
                    b.parabolic_skipped,
                    b.strategy_blocked,
                    b.positions_initiated,
                );
            }
        });
    }

    let registry = CreatorRegistryHandle::new();
    let tracker = PerformanceTrackerHandle::new(0.8);

    let ws_manager_tx = manager_tx.clone();
    let ws_addr = format!("0.0.0.0:{}", config.ws_port);
    tokio::spawn(async move {
        run_ws_server(&ws_addr, broadcast_tx, ws_manager_tx).await;
    });

    let balance_refresh_secs = config.execution.live.balance_refresh_secs.max(1);
    tokio::spawn({
        let wallets = wallet_registry.clone();
        let balance_state = balance_state.clone();

        async move {
            let mut tick_count: u64 = 0;
            loop {
                tick_count = tick_count.wrapping_add(1);
                wallets
                    .refresh_all_balances(balance_refresh_secs, tick_count)
                    .await;
                let total = wallets.total_balance_sol();
                balance_state.store(total.to_bits(), Ordering::Relaxed);
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        }
    });

    let api_state = ApiState {
        pool: pool.clone(),
        creators: creators.clone(),
        paused: paused_state,
        balance: balance_state,
        buy_size: buy_size_state.clone(),
        pubkey: pubkey_string,
        wallet_registry: wallet_registry.clone(),
        mode: wallet_registry.mode_label(),
        feed_metrics: feed_metrics_vec.clone(),
        bot_metrics: bot_metrics.clone(),
        manager_tx: manager_tx.clone(),
        dev_ranker: dev_ranker_handle.clone(),
        smart_money: smart_money_handle.clone(),
        tx_log: tx_log.clone(),
        config_path: "filter_config.yaml".to_string(),
        live_cfg: config.execution.live.clone(),
        buy_cap_floor,
        learning_log: learning_log.clone(),
    };

    let http_addr = format!("0.0.0.0:{}", config.http_port);
    tokio::spawn(async move {
        use axum::{
            Json, Router,
            extract::{Path, State},
            response::IntoResponse,
            routing::{get, post},
        };

        async fn get_pubkey(State(state): State<ApiState>) -> impl IntoResponse {
            #[derive(serde::Serialize)]
            struct PubkeyResponse {
                pubkey: String,
            }
            Json(PubkeyResponse {
                pubkey: state.pubkey,
            })
        }

        async fn get_bot_trades(
            State(state): State<ApiState>,
            axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
        ) -> impl IntoResponse {
            let wallet_filter = params.get("wallet_id").cloned();
            const Q_ALL: &str = "SELECT id, wallet_id, mint, entry_mcap_sol, invested_sol, realized_pnl_pct, close_reason, \
                 entry_at, closed_at, exit_mcap_sol, entry_meta, \
                 post_exit_mcap_10s, post_exit_mcap_30s, post_exit_mcap_50s, post_exit_mcap_70s, \
                 post_exit_mcap_100s, post_exit_mcap_180s, post_exit_mcap_240s, post_exit_mcap_300s, \
                 post_exit_mcap_5m, post_exit_mcap_10m, post_exit_mcap_15m, post_exit_mcap_30m, \
                 post_exit_max_mcap, post_exit_min_mcap, \
                 post_exit_time_to_max_secs, post_exit_time_to_min_secs, \
                 post_exit_pct_10s, post_exit_pct_30s, post_exit_pct_50s, post_exit_pct_70s, \
                 post_exit_pct_100s, post_exit_pct_180s, post_exit_pct_240s, post_exit_pct_300s, \
                 post_exit_pct_5m, post_exit_pct_10m, post_exit_pct_15m, post_exit_pct_30m, \
                 post_exit_max_pct, post_exit_min_pct, post_exit_tracking_done \
                 FROM bot_trades ORDER BY closed_at DESC";
            const Q_WALLET: &str = "SELECT id, wallet_id, mint, entry_mcap_sol, invested_sol, realized_pnl_pct, close_reason, \
                 entry_at, closed_at, exit_mcap_sol, entry_meta, \
                 post_exit_mcap_10s, post_exit_mcap_30s, post_exit_mcap_50s, post_exit_mcap_70s, \
                 post_exit_mcap_100s, post_exit_mcap_180s, post_exit_mcap_240s, post_exit_mcap_300s, \
                 post_exit_mcap_5m, post_exit_mcap_10m, post_exit_mcap_15m, post_exit_mcap_30m, \
                 post_exit_max_mcap, post_exit_min_mcap, \
                 post_exit_time_to_max_secs, post_exit_time_to_min_secs, \
                 post_exit_pct_10s, post_exit_pct_30s, post_exit_pct_50s, post_exit_pct_70s, \
                 post_exit_pct_100s, post_exit_pct_180s, post_exit_pct_240s, post_exit_pct_300s, \
                 post_exit_pct_5m, post_exit_pct_10m, post_exit_pct_15m, post_exit_pct_30m, \
                 post_exit_max_pct, post_exit_min_pct, post_exit_tracking_done \
                 FROM bot_trades WHERE wallet_id = $1 ORDER BY closed_at DESC";
            let mut last_err = None;
            for attempt in 0..3u8 {
                let q = if let Some(ref wid) = wallet_filter {
                    sqlx::query_as::<_, BotTradeRow>(Q_WALLET)
                        .bind(wid)
                        .fetch_all(&state.pool)
                        .await
                } else {
                    sqlx::query_as::<_, BotTradeRow>(Q_ALL)
                        .fetch_all(&state.pool)
                        .await
                };
                match q {
                    Ok(rows) => return Json(rows).into_response(),
                    Err(e) => {
                        let retryable = e.to_string().contains("timed out");
                        last_err = Some(e);
                        if retryable && attempt < 2 {
                            tokio::time::sleep(std::time::Duration::from_millis(
                                40 * (attempt as u64 + 1),
                            ))
                            .await;
                            continue;
                        }
                        break;
                    }
                }
            }
            if let Some(e) = last_err {
                eprintln!("[HTTP] bot_trades error: {e}");
            }
            axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }

        async fn get_chart(
            State(state): State<ApiState>,
            Path(mint): Path<String>,
        ) -> impl IntoResponse {
            #[derive(sqlx::FromRow)]
            struct ChartPointRow {
                t: i64,
                mcap: f64,
            }
            #[derive(sqlx::FromRow)]
            struct BotTradeMarkerRow {
                entry_at: i64,
                closed_at: i64,
                entry_mcap_sol: f64,
                exit_mcap_sol: f64,
                realized_pnl_pct: f64,
                close_reason: String,
            }
            #[derive(serde::Serialize)]
            struct ChartPoint {
                t: i64,
                mcap: f64,
            }
            #[derive(serde::Serialize)]
            struct ChartMarker {
                entry_at: i64,
                closed_at: i64,
                entry_mcap: f64,
                exit_mcap: f64,
                pnl: f64,
                reason: String,
            }
            #[derive(serde::Serialize)]
            struct ChartResponse {
                t0: i64,
                points: Vec<ChartPoint>,
                markers: Vec<ChartMarker>,
            }

            let tape_rows: Vec<ChartPointRow> = sqlx::query_as::<_, ChartPointRow>(
                "SELECT ts_unix AS t, mcap_sol AS mcap \
                 FROM coin_mcap_tape \
                 WHERE coin_address = $1 \
                 ORDER BY ts_unix ASC \
                 LIMIT 10000",
            )
            .bind(&mint)
            .fetch_all(&state.pool)
            .await
            .unwrap_or_default();

            let use_unix_tape = !tape_rows.is_empty();

            let point_rows = if use_unix_tape {
                tape_rows
            } else {
                match sqlx::query_as::<_, ChartPointRow>(
                    "SELECT CAST(slot_time AS BIGINT) AS t, market_cap::float8 AS mcap \
                     FROM trades \
                     WHERE coin_address = $1 AND currency = 'sol' \
                     ORDER BY slot_time ASC \
                     LIMIT 3000",
                )
                .bind(&mint)
                .fetch_all(&state.pool)
                .await
                {
                    Ok(rows) => rows,
                    Err(e) => {
                        eprintln!("[HTTP] chart price query error: {e}");
                        return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }
                }
            };

            let marker_rows = match sqlx::query_as::<_, BotTradeMarkerRow>(
                "SELECT entry_at, closed_at, entry_mcap_sol, exit_mcap_sol, realized_pnl_pct, close_reason \
                 FROM bot_trades \
                 WHERE mint = $1 \
                 ORDER BY closed_at ASC",
            )
            .bind(&mint)
            .fetch_all(&state.pool)
            .await
            {
                Ok(rows) => rows,
                Err(e) => {
                    eprintln!("[HTTP] chart markers query error: {e}");
                    return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            };

            let mut raw_series: Vec<(i64, f64)> = Vec::new();
            let mut slot_series: Vec<(i64, f64)> = Vec::new();
            for row in point_rows {
                if !chart_mcap_valid(row.mcap) {
                    continue;
                }
                if use_unix_tape {
                    raw_series.push((row.t, row.mcap));
                } else if let Some(last) = slot_series.last_mut() {
                    if last.0 == row.t {
                        last.1 = row.mcap;
                    } else {
                        slot_series.push((row.t, row.mcap));
                    }
                } else {
                    slot_series.push((row.t, row.mcap));
                }
            }

            let median = if use_unix_tape {
                chart_tape_median(&raw_series.iter().map(|(_, m)| *m).collect::<Vec<_>>())
            } else {
                chart_tape_median(&slot_series.iter().map(|(_, m)| *m).collect::<Vec<_>>())
            };

            if use_unix_tape {
                raw_series.retain(|(_, m)| chart_mcap_matches_tape(*m, median));
            } else {
                slot_series.retain(|(_, m)| chart_mcap_matches_tape(*m, median));
            }

            let slot0 = slot_series.first().map(|p| p.0).unwrap_or(0);
            let mut point_series: Vec<(i64, f64)> = if use_unix_tape {
                chart_dedup_points(raw_series)
            } else {
                chart_dedup_points(
                    slot_series
                        .iter()
                        .map(|(slot, mcap)| (chart_slot_to_sec(*slot, slot0), *mcap))
                        .collect(),
                )
            };

            let series_for_markers: &[(i64, f64)] = if use_unix_tape {
                &point_series
            } else {
                &slot_series
            };

            let mut markers: Vec<ChartMarker> = marker_rows
                .into_iter()
                .filter_map(|m| {
                    if !chart_mcap_matches_tape(m.entry_mcap_sol, median)
                        || !chart_mcap_matches_tape(m.exit_mcap_sol, median)
                    {
                        return None;
                    }
                    let (entry_at, closed_at) = chart_normalize_marker_secs(
                        m.entry_at,
                        m.closed_at,
                        m.entry_mcap_sol,
                        m.exit_mcap_sol,
                        series_for_markers,
                        slot0,
                        use_unix_tape,
                    );
                    if closed_at < entry_at {
                        return None;
                    }
                    Some(ChartMarker {
                        entry_at,
                        closed_at,
                        entry_mcap: m.entry_mcap_sol,
                        exit_mcap: m.exit_mcap_sol,
                        pnl: m.realized_pnl_pct,
                        reason: m.close_reason,
                    })
                })
                .collect();

            let marker_times: Vec<(i64, i64)> = markers
                .iter()
                .map(|m| (m.entry_at, m.closed_at))
                .collect();
            let (win_lo, win_hi) = chart_trade_window_bounds(&marker_times, &point_series);
            point_series.retain(|(t, _)| *t >= win_lo && *t <= win_hi);
            let entry_base = markers
                .iter()
                .map(|m| m.entry_at)
                .min()
                .unwrap_or(win_lo);
            let points: Vec<ChartPoint> = point_series
                .into_iter()
                .map(|(t, mcap)| ChartPoint {
                    t: t.saturating_sub(entry_base),
                    mcap,
                })
                .collect();
            for m in &mut markers {
                m.entry_at = m.entry_at.clamp(win_lo, win_hi).saturating_sub(entry_base);
                m.closed_at = m.closed_at.clamp(win_lo, win_hi).saturating_sub(entry_base);
            }

            Json(ChartResponse {
                t0: 0,
                points,
                markers,
            })
            .into_response()
        }

        async fn get_dev_stats(
            State(state): State<ApiState>,
            Path(mint): Path<String>,
        ) -> impl IntoResponse {
            use loggaper::persistence::creators::CreatorRepository;

            let developer = match sqlx::query_scalar::<_, String>(
                "SELECT developer FROM coins WHERE coin_address = $1",
            )
            .bind(&mint)
            .fetch_optional(&state.pool)
            .await
            {
                Ok(Some(d)) => d,
                Ok(None) => return Json(Option::<serde_json::Value>::None).into_response(),
                Err(e) => {
                    eprintln!("[HTTP] coins lookup error: {e}");
                    return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response();
                }
            };

            let dev_addr = match developer.parse::<solana_address::Address>() {
                Ok(a) => a,
                Err(_) => return axum::http::StatusCode::BAD_REQUEST.into_response(),
            };

            match state.creators.get_creator_stats_in_sol(dev_addr).await {
                Ok(stats) => Json(stats).into_response(),
                Err(e) => {
                    eprintln!("[HTTP] creator stats error: {e}");
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }

        async fn get_status(State(state): State<ApiState>) -> impl IntoResponse {
            #[derive(serde::Serialize)]
            struct Status {
                paused: bool,
                balance_sol: f64,
                total_balance_sol: f64,
                mode: &'static str,
                wallet: String,
                wallets: Vec<WalletWire>,
            }
            let total = state.wallet_registry.total_balance_sol();
            Json(Status {
                paused: state.paused.load(std::sync::atomic::Ordering::Relaxed),
                balance_sol: total,
                total_balance_sol: total,
                mode: state.mode,
                wallet: state.pubkey.clone(),
                wallets: state.wallet_registry.wire_snapshots(),
            })
        }

        async fn get_wallets(State(state): State<ApiState>) -> impl IntoResponse {
            Json(state.wallet_registry.wire_snapshots())
        }

        async fn put_wallets(
            State(state): State<ApiState>,
            Json(body): Json<serde_json::Value>,
        ) -> impl IntoResponse {
            let Some(arr) = body.get("wallets").and_then(|v| v.as_array()) else {
                return (
                    axum::http::StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": "body must include \"wallets\": [...]"})),
                )
                    .into_response();
            };
            let mut patches = Vec::new();
            for v in arr {
                let Some(id) = v.get("id").and_then(|x| x.as_str()) else {
                    continue;
                };
                let enabled = v.get("enabled").and_then(|x| x.as_bool());
                let size_sol = v.get("size_sol").and_then(|x| {
                    if x.is_null() {
                        Some(None)
                    } else {
                        x.as_f64().map(Some)
                    }
                });
                if let Some(w) = state.wallet_registry.get(id) {
                    if let Some(on) = enabled {
                        w.set_enabled(on);
                    }
                    if let Some(sz) = size_sol {
                        w.set_size_sol(sz);
                    }
                    patches.push(loggaper::autobuy::wallet_registry::WalletEntryConfig {
                        id: id.to_string(),
                        label: w.label.clone(),
                        enabled: w.is_enabled(),
                        private_key_env: w.private_key_env.clone(),
                        size_sol: w.size_sol(),
                        tier_size: w.tier_size(),
                        demo_balance_sol: None,
                        rpc_url_env: None,
                    });
                }
            }
            let path = &state.config_path;
            if let Err(e) = rewrite_yaml_wallets(path, &patches) {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": e})),
                )
                    .into_response();
            }
            Json(state.wallet_registry.wire_snapshots()).into_response()
        }

        async fn get_pnl(State(state): State<ApiState>) -> impl IntoResponse {
            #[derive(serde::Serialize)]
            struct WalletPnl {
                wallet_id: String,
                open_unrealized_sol: f64,
            }
            #[derive(serde::Serialize)]
            struct PnlResponse {
                combined_realized_sol: f64,
                combined_open_unrealized_sol: f64,
                per_wallet: Vec<WalletPnl>,
            }
            let (tx, rx) = tokio::sync::oneshot::channel();
            if state
                .manager_tx
                .send(PositionMessage::GetOpenPositions { responder: tx })
                .await
                .is_err()
            {
                return Json(PnlResponse {
                    combined_realized_sol: 0.0,
                    combined_open_unrealized_sol: 0.0,
                    per_wallet: vec![],
                })
                .into_response();
            }
            let open = rx.await.unwrap_or_default();
            let mut per_wallet: HashMap<String, f64> = HashMap::new();
            for p in &open {
                *per_wallet.entry(p.wallet_id.clone()).or_insert(0.0) += p.pnl;
            }
            let combined_open: f64 = per_wallet.values().sum();
            let per: Vec<WalletPnl> = per_wallet
                .into_iter()
                .map(|(wallet_id, open_unrealized_sol)| WalletPnl {
                    wallet_id,
                    open_unrealized_sol,
                })
                .collect();
            Json(PnlResponse {
                combined_realized_sol: 0.0,
                combined_open_unrealized_sol: combined_open,
                per_wallet: per,
            })
            .into_response()
        }

        async fn get_buy_size(State(state): State<ApiState>) -> impl IntoResponse {
            #[derive(serde::Serialize)]
            struct BuySizeResponse {
                sol: f64,
            }
            Json(BuySizeResponse {
                sol: f64::from_bits(state.buy_size.load(std::sync::atomic::Ordering::Relaxed)),
            })
        }

        async fn get_metrics(State(state): State<ApiState>) -> impl IntoResponse {
            #[derive(serde::Serialize)]
            struct MetricsResponse {
                feeds: Vec<FeedSnapshot>,
                bot: BotSnapshot,
                strategy: Option<StrategySnapshot>,
                dev_ranker: DevRankerSnapshot,
                smart_money: SmartMoneySnapshot,
            }
            let feeds: Vec<FeedSnapshot> =
                state.feed_metrics.iter().map(|m| m.snapshot()).collect();
            let bot = state.bot_metrics.snapshot();

            let strategy = {
                let (tx, rx) = tokio::sync::oneshot::channel();
                if state
                    .manager_tx
                    .send(PositionMessage::GetStrategySnapshot { responder: tx })
                    .await
                    .is_ok()
                {
                    rx.await.ok()
                } else {
                    None
                }
            };
            let dev_ranker = state.dev_ranker.snapshot().await;
            let smart_money = state.smart_money.snapshot().await;
            Json(MetricsResponse {
                feeds,
                bot,
                strategy,
                dev_ranker,
                smart_money,
            })
        }

        async fn get_tier_stats(State(state): State<ApiState>) -> impl IntoResponse {
            let Some(ref log) = state.learning_log else {
                return Json(serde_json::json!({"error": "learning disabled"})).into_response();
            };
            match log.stats_tier_b_detailed().await {
                Ok(stats) => Json(stats).into_response(),
                Err(e) => {
                    eprintln!("[HTTP] tier-stats error: {e}");
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }

        async fn get_tx_log(State(state): State<ApiState>) -> impl IntoResponse {
            let snapshot: Vec<WsFeedMessage> = match state.tx_log.lock() {
                Ok(log) => log.iter().cloned().collect(),
                Err(_) => Vec::new(),
            };
            Json(snapshot)
        }

        async fn get_open_positions(State(state): State<ApiState>) -> impl IntoResponse {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if state
                .manager_tx
                .send(PositionMessage::GetOpenPositions { responder: tx })
                .await
                .is_err()
            {
                return Json(Vec::<OpenPositionWire>::new()).into_response();
            }
            let rows = rx.await.unwrap_or_default();
            Json(rows).into_response()
        }

        async fn get_mode(State(state): State<ApiState>) -> impl IntoResponse {
            #[derive(serde::Serialize)]
            struct ModeResponse {
                mode: &'static str,
                wallet: String,
                balance_sol: f64,
                live: loggaper::autobuy::execution::LiveExecutionConfig,
            }
            Json(ModeResponse {
                mode: state.mode,
                wallet: state.pubkey.clone(),
                balance_sol: f64::from_bits(
                    state.balance.load(std::sync::atomic::Ordering::Relaxed),
                ),
                live: state.live_cfg.clone(),
            })
        }

        /// PUT /mode { "mode": "demo" | "live" }
        ///
        /// Writes the new mode into `filter_config.yaml` and returns a
        /// `restart_required` flag. Switching to live REQUIRES the request
        /// header `X-Confirm-Live: yes`; without it the bot rejects the
        /// switch — a deliberate safety gate so a stray click in the
        /// dashboard never starts spending real funds.
        async fn put_mode(
            State(state): State<ApiState>,
            headers: axum::http::HeaderMap,
            Json(body): Json<serde_json::Value>,
        ) -> impl IntoResponse {
            let target = match body.get("mode").and_then(|v| v.as_str()) {
                Some("demo") => "demo",
                Some("live") => "live",
                _ => {
                    return (
                        axum::http::StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({"error": "mode must be 'demo' or 'live'"})),
                    )
                        .into_response();
                }
            };

            if target == "live" {
                let confirmed = headers
                    .get("x-confirm-live")
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.eq_ignore_ascii_case("yes"))
                    .unwrap_or(false);
                if !confirmed {
                    return (
                        axum::http::StatusCode::FORBIDDEN,
                        Json(serde_json::json!({
                            "error": "live mode requires X-Confirm-Live: yes header",
                        })),
                    )
                        .into_response();
                }
            }

            // Rewrite filter_config.yaml in place. We only touch the
            // `execution.mode` field — everything else is preserved.
            let path = &state.config_path;
            let content = match std::fs::read_to_string(path) {
                Ok(s) => s,
                Err(e) => {
                    return (
                        axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                        Json(serde_json::json!({"error": format!("read {path}: {e}")})),
                    )
                        .into_response();
                }
            };

            let new_content = rewrite_yaml_mode(&content, target);
            if let Err(e) = std::fs::write(path, &new_content) {
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": format!("write {path}: {e}")})),
                )
                    .into_response();
            }

            let restart_required = state.mode != target;
            (
                axum::http::StatusCode::OK,
                Json(serde_json::json!({
                    "mode": target,
                    "current_mode": state.mode,
                    "restart_required": restart_required,
                })),
            )
                .into_response()
        }

        async fn set_buy_size(
            State(state): State<ApiState>,
            Json(body): Json<serde_json::Value>,
        ) -> impl IntoResponse {
            let sol = match body.get("sol").and_then(|v| v.as_f64()) {
                Some(v) if v > 0.0 => v,
                _ => {
                    eprintln!("[HTTP] set_buy_size: invalid or missing 'sol' field");
                    return axum::http::StatusCode::BAD_REQUEST.into_response();
                }
            };
            if sol < state.buy_cap_floor {
                eprintln!(
                    "[HTTP] set_buy_size: rejected {:.6} SOL (minimum {:.6})",
                    sol, state.buy_cap_floor
                );
                return (
                    axum::http::StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({
                        "error": format!("sol must be >= {} (covers a_plus tier size)", state.buy_cap_floor),
                    })),
                )
                    .into_response();
            }
            state
                .buy_size
                .store(f64::to_bits(sol), std::sync::atomic::Ordering::Relaxed);
            eprintln!("[HTTP] buy size updated to {sol} SOL");
            axum::http::StatusCode::NO_CONTENT.into_response()
        }

        async fn post_positions_abandon(
            State(state): State<ApiState>,
            Json(body): Json<serde_json::Value>,
        ) -> impl IntoResponse {
            let Some(arr) = body.get("mints").and_then(|v| v.as_array()) else {
                return (
                    axum::http::StatusCode::BAD_REQUEST,
                    Json(serde_json::json!({"error": "JSON body must include \"mints\": [\"...\"]"})),
                )
                    .into_response();
            };
            let wallet_id = body
                .get("wallet_id")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let mut abandoned: Vec<String> = Vec::new();
            let mut invalid: Vec<String> = Vec::new();
            for v in arr {
                let Some(s) = v.as_str() else {
                    continue;
                };
                let mint: solana_address::Address = match s.parse() {
                    Ok(m) => m,
                    Err(_) => {
                        invalid.push(s.to_string());
                        continue;
                    }
                };
                if state
                    .manager_tx
                    .send(PositionMessage::AbandonPosition {
                        mint,
                        wallet_id: wallet_id.clone(),
                    })
                    .await
                    .is_err()
                {
                    return (
                        axum::http::StatusCode::SERVICE_UNAVAILABLE,
                        Json(serde_json::json!({"error": "position manager channel closed"})),
                    )
                        .into_response();
                }
                abandoned.push(s.to_string());
            }
            (
                axum::http::StatusCode::OK,
                Json(serde_json::json!({
                    "abandoned": abandoned,
                    "invalid_mint_strings": invalid,
                })),
            )
                .into_response()
        }

        let app = Router::new()
            .route("/bot-trades", get(get_bot_trades))
            .route("/status", get(get_status))
            .route("/pubkey", get(get_pubkey))
            .route("/dev-stats/{mint}", get(get_dev_stats))
            .route("/chart/{mint}", get(get_chart))
            .route("/buy-size", get(get_buy_size).put(set_buy_size))
            .route("/metrics", get(get_metrics))
            .route("/tier-stats", get(get_tier_stats))
            .route("/tx-log", get(get_tx_log))
            .route("/mode", get(get_mode).put(put_mode))
            .route("/positions", get(get_open_positions))
            .route("/positions/abandon", post(post_positions_abandon))
            .route("/wallets", get(get_wallets).put(put_wallets))
            .route("/pnl", get(get_pnl))
            .with_state(api_state);

        let listener = tokio::net::TcpListener::bind(&http_addr)
            .await
            .expect("Failed to bind HTTP server");
        println!("HTTP API active on: {}", http_addr);
        axum::serve(listener, app).await.unwrap();
    });

    let fresh_watchlist =
        loggaper::pipeline::fresh_watchlist::FreshWatchlistManager::new();

    while let Some((slot, event, bucket)) = general_rx.recv().await {
        match event {
            Action::Create(general_create) => {
                if general_create.is_unsupported_quote_mint() {
                    eprintln!(
                        "[FILTER] {} skipped: unsupported_quote_mint ({})",
                        general_create.mint, general_create.quote_mint
                    );
                    bot_metrics.note_filter_rejected();
                    if let Some(ref log) = learning_log {
                        let log = log.clone();
                        let mint_s = general_create.mint.to_string();
                        let dev_s = general_create.user.to_string();
                        let quote_s = general_create.quote_mint.to_string();
                        let ts = SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_secs() as i64)
                            .unwrap_or(0);
                        tokio::spawn(async move {
                            let _ = log
                                .log_skipped(
                                    &mint_s,
                                    Some(dev_s.as_str()),
                                    "filter_quote_mint",
                                    "unsupported_quote_mint",
                                    serde_json::json!({ "quote_mint": quote_s }),
                                    ts,
                                )
                                .await;
                        });
                    }
                    continue;
                }
                println!("created {}", general_create.mint);
                bot_metrics.note_create();
                let creators = creators.clone();
                let tx = manager_tx.clone();
                let filter_config = config.clone();
                let registry = registry.clone();
                let mint_address = general_create.mint;
                let bot_metrics_create = bot_metrics.clone();
                let dev_ranker_for_create = dev_ranker_handle.clone();
                let smart_money_for_create = smart_money_handle.clone();
                let bucket_for_score = bucket.clone();
                let launchpad_for_score = launchpad_tx.clone();
                let buy_cap = buy_size_state.clone();
                let learning_log_create = learning_log.clone();
                let learning_overrides_spawn = learning_overrides.clone();
                let dev_blacklist_create = dev_blacklist.clone();
                let dev_blacklist_cfg_create = config.dev_blacklist.clone();
                let fresh_watchlist_create = fresh_watchlist.clone();

                tokio::spawn({
                    let creators = creators.clone();
                    let buy_latency_create = buy_latency.clone();
                    async move {
                        buy_latency_create.on_created(mint_address);
                        // Fine-grained per-stage pipeline timer (created → buy).
                        let pipeline_t0 = std::time::Instant::now();
                        let unix_now = || {
                            SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs() as i64)
                                .unwrap_or(0)
                        };

                        if dev_blacklist_cfg_create.enabled {
                            let dev_s = general_create.user.to_string();
                            let now = unix_now();
                            match dev_blacklist_create.active_for_dev(&dev_s, now).await {
                                Ok(Some(active)) => {
                                    let (bl_label, detail) =
                                        loggaper::autobuy::dev_blacklist::format_filter_skip(
                                            &active.reason,
                                            &active.mint,
                                            active.expires_at,
                                        );
                                    eprintln!(
                                        "[FILTER] {} skipped: {bl_label} ({detail})",
                                        general_create.mint
                                    );
                                    bot_metrics_create.note_filter_rejected();
                                    if let Some(ref log) = learning_log_create {
                                        let log = log.clone();
                                        let mint_s = general_create.mint.to_string();
                                        let ts = now;
                                        let payload = serde_json::json!({
                                            "dev_wallet": dev_s,
                                            "trigger_mint": active.mint,
                                            "trigger_reason": active.reason,
                                            "trigger_pnl_sol": active.pnl_sol,
                                            "trigger_close_reason": active.close_reason,
                                            "expires_at": active.expires_at,
                                        });
                                        let learning_reason = bl_label.clone();
                                        tokio::spawn(async move {
                                            let _ = log
                                                .log_skipped(
                                                    &mint_s,
                                                    Some(dev_s.as_str()),
                                                    "filter_dev_blacklist",
                                                    &learning_reason,
                                                    payload,
                                                    ts,
                                                )
                                                .await;
                                        });
                                    }
                                    return;
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    eprintln!(
                                        "[FILTER] dev_blacklist lookup failed for {}: {e}",
                                        general_create.user
                                    );
                                }
                            }
                        }

                        // --- Stage 0: cheap spam-dev gate -------------------
                        // Prolific devs (serial spam/ruggers) make the
                        // creator-stats aggregation scan hundreds of thousands
                        // of trade rows. A capped count (cost independent of dev
                        // size) flags them so we can SKIP the heavy analytics
                        // *without* banning the token: it competes on tape
                        // strength alone, with a scoring penalty + an A+-only
                        // buy gate, so rare strong runners from prolific devs
                        // are no longer lost.
                        let dev_pubkey = general_create.user.to_string();
                        let spam_dev_whitelisted = filter_config
                            .creator_config
                            .is_spam_dev_whitelisted(&dev_pubkey);

                        let mut is_spam_dev = false;
                        if let Some(spam_cap) = filter_config.creator_config.spam_skip_coins {
                            match creators
                                .count_creator_coins_capped(general_create.user, spam_cap)
                                .await
                            {
                                Ok(n) if n > spam_cap && !spam_dev_whitelisted => {
                                    is_spam_dev = true;
                                    eprintln!(
                                        "[FILTER] {} spam_dev (>{} coins): skipping creator-stats, \
                                         continuing with penalty",
                                        general_create.mint, spam_cap
                                    );
                                    bot_metrics_create.note_spam_dev_skip();
                                }
                                Ok(n) if n > spam_cap && spam_dev_whitelisted => {
                                    eprintln!(
                                        "[FILTER] {} spam_dev_whitelist: dev {} has >{} coins, \
                                         full creator-stats (no score penalty)",
                                        general_create.mint, dev_pubkey, spam_cap
                                    );
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    // Non-fatal: fall through to the full path on
                                    // count errors rather than dropping a token.
                                    eprintln!(
                                        "[FILTER] spam-dev count failed for {}: {e}",
                                        general_create.user
                                    );
                                }
                            }
                        }

                        // --- Stage 1: cheap pre-gate on dev history ---------
                        // Operator-tuned creator_config acts as the hard
                        // pre-filter for normal devs. For spam devs we skip the
                        // expensive query *and* the hard filter, leaning on the
                        // scoring penalty + A+-only gate instead. Score Engine
                        // runs after this so we don't burn a scoring window on
                        // hopeless devs.
                        let dev_stats: Option<loggaper::persistence::creators::CreatorStatistics> =
                            if is_spam_dev {
                                None
                            } else {
                                let dev_stats_opt = match creators
                                    .get_creator_stats_in_sol(general_create.user)
                                    .await
                                {
                                    Ok(stats) => stats,
                                    Err(e) => {
                                        eprintln!(
                                            "[FILTER] DB error for {}: {e}",
                                            general_create.user
                                        );
                                        if let Some(ref log) = learning_log_create {
                                            let log = log.clone();
                                            let mint_s = general_create.mint.to_string();
                                            let dev_s = general_create.user.to_string();
                                            let ts = unix_now();
                                            let msg = format!("{e}");
                                            tokio::spawn(async move {
                                                let _ = log
                                                    .log_skipped(
                                                        &mint_s,
                                                        Some(dev_s.as_str()),
                                                        "filter_db",
                                                        &msg,
                                                        serde_json::json!({}),
                                                        ts,
                                                    )
                                                    .await;
                                            });
                                        }
                                        return;
                                    }
                                };

                                match dev_stats_opt {
                                    Some(s) => {
                                        if !filter_config.creator_config.filter(&s) {
                                            let reasons =
                                                filter_config.creator_config.reject_reasons(&s);
                                            let wl_cfg = &filter_config.scoring.tier_b.fresh_watchlist;
                                            let prior = match creators
                                                .count_prior_coins(
                                                    general_create.user,
                                                    mint_address,
                                                )
                                                .await
                                            {
                                                Ok(n) => n,
                                                Err(e) => {
                                                    eprintln!(
                                                        "[FILTER] prior_coins DB error for {}: {e}",
                                                        general_create.mint
                                                    );
                                                    bot_metrics_create.note_filter_rejected();
                                                    return;
                                                }
                                            };
                                            if wl_cfg.enabled
                                                && filter_config.scoring.tier_b.enabled
                                                && !is_spam_dev
                                                && prior == 0
                                                && loggaper::autobuy::filters::creator::is_early_stats_only_reject(
                                                    &reasons,
                                                )
                                            {
                                                let initial_mcap = bucket_for_score
                                                    .pool()
                                                    .market_cap()
                                                    .amount()
                                                    .to_float();
                                                let reason_strings: Vec<String> = reasons
                                                    .iter()
                                                    .map(|r| (*r).to_string())
                                                    .collect();
                                                let pipeline_deps =
                                                    loggaper::pipeline::score_buy::ScoringPipelineDeps {
                                                        manager_tx: tx.clone(),
                                                        filter_config: filter_config.clone(),
                                                        launchpad_for_score: launchpad_for_score
                                                            .clone(),
                                                        dev_ranker: dev_ranker_for_create.clone(),
                                                        smart_money: smart_money_for_create.clone(),
                                                        learning_log: learning_log_create.clone(),
                                                        learning_overrides: learning_overrides_spawn
                                                            .clone(),
                                                        bot_metrics: bot_metrics_create.clone(),
                                                        buy_latency: buy_latency_create.clone(),
                                                        buy_cap: buy_cap.clone(),
                                                        registry: Some(registry.clone()),
                                                    };
                                                match fresh_watchlist_create.try_add(
                                                    wl_cfg,
                                                    mint_address,
                                                    general_create.user,
                                                    initial_mcap,
                                                    reason_strings,
                                                    bucket_for_score.clone(),
                                                    pipeline_t0,
                                                    pipeline_deps,
                                                    learning_log_create.clone(),
                                                    bot_metrics_create.clone(),
                                                ) {
                                                    Ok(()) => return,
                                                    Err(
                                                        loggaper::pipeline::fresh_watchlist::TryAddError::Duplicate,
                                                    ) => return,
                                                    Err(
                                                        loggaper::pipeline::fresh_watchlist::TryAddError::CapFull,
                                                    ) => {
                                                        eprintln!(
                                                            "[FILTER] {} fresh watchlist cap full, \
                                                             hard reject",
                                                            general_create.mint
                                                        );
                                                    }
                                                    Err(
                                                        loggaper::pipeline::fresh_watchlist::TryAddError::Disabled,
                                                    ) => {}
                                                }
                                            }
                                            eprintln!(
                                                "[FILTER] {} rejected by creator_config",
                                                general_create.mint
                                            );
                                            bot_metrics_create.note_filter_rejected();
                                            if let Some(ref log) = learning_log_create {
                                                let log = log.clone();
                                                let mint_s = general_create.mint.to_string();
                                                let dev_s = general_create.user.to_string();
                                                let ts = unix_now();
                                                let payload = serde_json::json!({
                                                    "reject_reasons": reasons,
                                                    "prior_coins": prior,
                                                });
                                                tokio::spawn(async move {
                                                    let _ = log
                                                        .log_skipped(
                                                            &mint_s,
                                                            Some(dev_s.as_str()),
                                                            "filter_creator",
                                                            "creator_config_rejected",
                                                            payload,
                                                            ts,
                                                        )
                                                        .await;
                                                });
                                            }
                                            return;
                                        }
                                        registry.save(mint_address, s.clone()).await;
                                        Some(s)
                                    }
                                    None => {
                                        if filter_config.scoring.tier_b.enabled && !is_spam_dev {
                                            eprintln!(
                                                "[FILTER] {} fresh_dev_b_lane: fresh_b_subtype={} \
                                                 continuing for tier B evaluation",
                                                general_create.mint,
                                                loggaper::scoring::fresh_b::FreshBSubtype::Unknown
                                                    .as_str(),
                                            );
                                            None
                                        } else {
                                            eprintln!(
                                                "[FILTER] {} skipped: no creator history",
                                                general_create.mint
                                            );
                                            bot_metrics_create.note_no_history();
                                            if let Some(ref log) = learning_log_create {
                                                let log = log.clone();
                                                let mint_s = general_create.mint.to_string();
                                                let dev_s = general_create.user.to_string();
                                                let ts = unix_now();
                                                tokio::spawn(async move {
                                                    let _ = log
                                                        .log_skipped(
                                                            &mint_s,
                                                            Some(dev_s.as_str()),
                                                            "filter_no_history",
                                                            "no_creator_history",
                                                            serde_json::json!({}),
                                                            ts,
                                                        )
                                                        .await;
                                                });
                                            }
                                            return;
                                        }
                                    }
                                }
                            };
                        bot_metrics_create.note_passed_filter();

                        let pipeline_deps = loggaper::pipeline::score_buy::ScoringPipelineDeps {
                            manager_tx: tx.clone(),
                            filter_config: filter_config.clone(),
                            launchpad_for_score: launchpad_for_score.clone(),
                            dev_ranker: dev_ranker_for_create.clone(),
                            smart_money: smart_money_for_create.clone(),
                            learning_log: learning_log_create.clone(),
                            learning_overrides: learning_overrides_spawn.clone(),
                            bot_metrics: bot_metrics_create.clone(),
                            buy_latency: buy_latency_create.clone(),
                            buy_cap: buy_cap.clone(),
                            registry: Some(registry.clone()),
                        };
                        loggaper::pipeline::score_buy::run_scoring_and_buy(
                            &pipeline_deps,
                            loggaper::pipeline::score_buy::ScoringPipelineInput {
                                mint: mint_address,
                                dev: general_create.user,
                                dev_stats,
                                is_spam_dev,
                                bucket_for_score,
                                pipeline_t0,
                                from_fresh_watchlist: false,
                            },
                        )
                        .await;
                    }
                });

                tokio::spawn({
                    let tokens = tokens.clone();
                    let waiter = waiter_handle.clone();
                    async move {
                        let start = Instant::now();
                        if let Err(err) = tokens
                            .save_token(general_create.mint, general_create.user, slot)
                            .await
                        {
                            dbg!(err);
                        }
                        waiter.notify_created(general_create.mint).await;
                        let _duration = start.elapsed();
                        let _now = SystemTime::now();
                    }
                });
            }
            Action::Trade(trade_action) => {
                let trade_action = Arc::new(trade_action);
                let bucket = Arc::new(bucket);

                // Authoritative reconciliation hook for the broker. For the
                // SolanaBroker this updates tracked token holdings and the
                // cached SOL balance whenever our wallet appears in a trade.
                // The MockBroker's default impl is a no-op.
                wallet_registry.on_trade(trade_action.as_ref(), bucket.pool());

                tokio::spawn({
                    let trade_action = trade_action.clone();
                    let bucket = bucket.clone();
                    let tx = manager_tx.clone();
                    let tracker = tracker.clone();
                    let registry = registry.clone();

                    {
                        let _ = tx
                            .send(PositionMessage::UpdateTokenBucket((*bucket).clone()))
                            .await;
                    }

                    async move {
                        let current_mc = bucket.pool().market_cap().amount().to_float();
                        let _best_mc = tracker.get_best_market_cap().await;
                        let trader_pnl = bucket
                            .swarm()
                            .get_pnl(loggaper::trading::trader::TraderType::Regular)
                            .await;

                        if trader_pnl > 0.0
                            && let Some(dev_stats) = registry.get(trade_action.mint()).await {
                                let _cloned = dev_stats.clone();
                                let updated = tracker.try_update_ath(current_mc, dev_stats).await;
                                if updated {
                                    // println!("{} {:?}", trade_action.mint(), &cloned);
                                }
                            }

                        // NOTE: a per-trade `get_trader_stats` lookup used to run
                        // here; its result was unused but the query (full
                        // aggregate over the trader's trades) cost 2-5s on the
                        // 25M-row trades table and stalled the trade stream. It
                        // has been removed. Re-add a cached/bounded variant if a
                        // consumer ever needs per-trade trader stats.
                    }
                });

                {
                    let trade_action = trade_action.clone();
                    let write_queue = write_queue.clone();
                    let waiter = waiter_handle.clone();
                    let bucket = bucket.clone();

                    tokio::spawn(async move {
                        waiter.wait_for(trade_action.mint()).await;

                        let trader = match bucket.swarm().get_trader(trade_action.trader()).await {
                            Some(trader) => trader,
                            None => return,
                        };

                        let mint_s = trade_action.mint().to_string();
                        let mcap_sol = bucket.pool().market_cap().amount().to_float();
                        write_queue.try_enqueue_tape(mint_s.clone(), mcap_sol, "trade");

                        let entry = TraderEntry {
                            trader_address: trade_action.trader().to_string(),
                            coin_address: mint_s,
                            realized_pnl: trader.pnl_percent(),
                            slot,
                            is_buy: trade_action.is_buy(),
                            market_cap: bucket.pool().market_cap(),
                            currency: trade_action.size(),
                            role: trader.trader_type(),
                        };

                        write_queue.try_enqueue_trade(entry);
                    });
                }
            }
        }
    }
}

pub async fn run_ws_server(
    addr: &str,
    broadcast_tx: broadcast::Sender<WsFeedMessage>,
    manager_tx: mpsc::Sender<PositionMessage>,
) {
    let listener = TcpListener::bind(addr)
        .await
        .expect("Failed to bind WS server");
    println!("WebSocket Feed active on: {}", addr);

    while let Ok((stream, _)) = listener.accept().await {
        let mut rx = broadcast_tx.subscribe();
        let manager_tx = manager_tx.clone();

        tokio::spawn(async move {
            use tokio_tungstenite::tungstenite::Message;

            let ws_stream = match accept_async(stream).await {
                Ok(ws) => ws,
                Err(_) => return,
            };

            let (mut sink, mut incoming) = ws_stream.split();

            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        match msg {
                            Ok(feed_msg) => {
                                if let Ok(json) = serde_json::to_string(&feed_msg)
                                    && sink.send(Message::Text(json.into())).await.is_err() {
                                        break;
                                    }
                            }
                            Err(_) => break,
                        }
                    }
                    msg = incoming.next() => {
                        match msg {
                            Some(Ok(Message::Text(text))) => {
                                if let Ok(cmd) = serde_json::from_str::<WsCommand>(&text) {
                                    match cmd {
                                        WsCommand::SetPaused { paused } => {
                                            let _ = manager_tx.send(PositionMessage::SetPaused(paused)).await;
                                        }
                                    }
                                }
                            }
                            Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                            _ => {}
                        }
                    }
                }
            }
        });
    }
}

/// Replace the `mode:` line inside the `execution:` block of `filter_config.yaml`
/// with the new value, preserving all other lines / formatting / comments.
/// Falls back to appending a fresh `execution` block if the section is absent.
fn rewrite_yaml_mode(content: &str, new_mode: &str) -> String {
    let mut out = String::with_capacity(content.len() + 64);
    let mut inside_exec = false;
    let mut mode_written = false;
    let mut saw_exec_header = false;

    for line in content.lines() {
        let trimmed = line.trim_start();

        if trimmed.starts_with("execution:") {
            inside_exec = true;
            saw_exec_header = true;
            out.push_str(line);
            out.push('\n');
            continue;
        }

        if inside_exec {
            // A new top-level key (no leading whitespace) ends the block.
            let leading_ws = line.len() - trimmed.len();
            let is_top_level = !line.is_empty() && leading_ws == 0;

            if is_top_level {
                if !mode_written {
                    out.push_str(&format!("  mode: {}\n", new_mode));
                    mode_written = true;
                }
                inside_exec = false;
                out.push_str(line);
                out.push('\n');
                continue;
            }

            // Replace existing `mode: <x>` line.
            if !mode_written && trimmed.starts_with("mode:") {
                let indent: String = line.chars().take_while(|c| c.is_whitespace()).collect();
                out.push_str(&indent);
                out.push_str("mode: ");
                out.push_str(new_mode);
                out.push('\n');
                mode_written = true;
                continue;
            }
        }

        out.push_str(line);
        out.push('\n');
    }

    if !mode_written {
        if !saw_exec_header {
            out.push_str("\nexecution:\n");
        }
        out.push_str(&format!("  mode: {}\n", new_mode));
    }

    out
}

fn rewrite_yaml_wallets(
    path: &str,
    wallets: &[loggaper::autobuy::wallet_registry::WalletEntryConfig],
) -> Result<(), String> {
    let content =
        std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
    let mut doc: serde_yaml::Value =
        serde_yaml::from_str(&content).map_err(|e| format!("parse yaml: {e}"))?;
    let wv = serde_yaml::to_value(wallets).map_err(|e| format!("wallets value: {e}"))?;
    if let Some(map) = doc.as_mapping_mut() {
        map.insert(serde_yaml::Value::from("wallets"), wv);
    } else {
        return Err("root yaml is not a mapping".into());
    }
    let out = serde_yaml::to_string(&doc).map_err(|e| format!("serialize: {e}"))?;
    std::fs::write(path, out).map_err(|e| format!("write {path}: {e}"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::rewrite_yaml_mode;

    #[test]
    fn rewrites_existing_mode_value() {
        let yaml = "ws_port: 1\nexecution:\n  mode: demo\n  live:\n    slippage_bps: 500\n";
        let out = rewrite_yaml_mode(yaml, "live");
        assert!(out.contains("mode: live"));
        assert!(!out.contains("mode: demo"));
        assert!(out.contains("slippage_bps: 500"));
    }

    #[test]
    fn inserts_mode_when_missing_in_block() {
        let yaml = "ws_port: 1\nexecution:\n  live:\n    slippage_bps: 500\nhttp_port: 2\n";
        let out = rewrite_yaml_mode(yaml, "live");
        let exec_idx = out.find("execution:").unwrap();
        let http_idx = out.find("http_port:").unwrap();
        let mode_idx = out.find("mode: live").unwrap();
        assert!(mode_idx > exec_idx && mode_idx < http_idx);
    }

    #[test]
    fn appends_block_when_missing_entirely() {
        let yaml = "ws_port: 1\nhttp_port: 2\n";
        let out = rewrite_yaml_mode(yaml, "live");
        assert!(out.contains("execution:"));
        assert!(out.contains("mode: live"));
    }
}
