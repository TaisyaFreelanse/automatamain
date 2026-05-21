use dotenvy::dotenv;
use futures::{SinkExt, StreamExt};
use loggaper::{
    autobuy::{
        broker::Broker,
        execution::{build_broker, ExecutionMode},
        manager::{
            OpenPositionWire, OpenReason, PositionManagerActor, PositionMessage, WsCommand,
            WsFeedMessage,
        },
        performance_tracker::{CreatorRegistryHandle, PerformanceTrackerHandle},
    },
    feed::metrics::{BotMetrics, BotSnapshot, FeedMetrics, FeedSnapshot, new_dedup},
    generalize::general_commands::Action,
    learning::{
        load_patch, merge_thresholds, spawn_learning_engine, LearningLogPg, LearningTradeSnapshot,
    },
    persistence::{
        bot_trades::BotTradeRow,
        creators::CreatorRepository,
        postgres::creators::CreatorsRepositoryPostgres,
        tokens::TokenRepository,
        traders::{TraderEntry, TraderRepository},
    },
    pipelines::pump::PumpPipeline,
    scoring::{
        config::MinBuyTier,
        dev_ranker::{self, DevRankerHandle, DevRankerSnapshot},
        features,
        score_engine::{ScoreEngine, Tier},
        smart_money::{self, SmartMoneyHandle, SmartMoneySnapshot},
        strategy_controller::StrategySnapshot,
    },
    setup::{
        load_config, setup_crypto, setup_logging, setup_postgres_pool, setup_repositories,
        setup_solana_rpc, waiter::DatabaseCreateWaiter,
    },
};
use solana_keypair::{Keypair, Signer};
use std::sync::Arc;
use std::{
    sync::atomic::Ordering,
    time::{Instant, SystemTime},
};
use tokio::{
    net::TcpListener,
    sync::{broadcast, mpsc, RwLock},
};
use tokio_tungstenite::accept_async;

#[tokio::main]
async fn main() {
    dotenv().ok();
    setup_crypto();
    setup_logging();
    let config = Arc::new(load_config().unwrap());
    let pool = setup_postgres_pool(30).await;
    let (creators, tokens, trades, bot_trades) = setup_repositories(pool.clone()).await;
    let (creators, tokens, trades, bot_trades) = (
        Arc::new(creators),
        Arc::new(tokens),
        Arc::new(trades),
        Arc::new(bot_trades),
    );

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
    }

    let (waiter_actor, waiter_handle) = DatabaseCreateWaiter::new();
    tokio::spawn(async move {
        waiter_actor.run().await;
    });

    let (ws_url, commitment_config) = setup_solana_rpc();
    let (general_tx, mut general_rx) = mpsc::channel(2048);
    let (broadcast_tx, _) = broadcast::channel::<WsFeedMessage>(4096);

    // Wallet pubkey is only displayed by `/pubkey` — derived lazily so demo
    // mode does not require a `PRIVATE_KEY` env to be set.
    let pubkey_string = std::env::var("PRIVATE_KEY")
        .ok()
        .map(|sk| Keypair::from_base58_string(&sk).pubkey().to_string())
        .unwrap_or_else(|| "demo-no-wallet".to_string());

    // Broker is chosen based on `execution.mode` in `filter_config.yaml`:
    //   demo -> MockBroker (start_balance_sol)
    //   live -> SolanaBroker (real wallet + mainnet RPC, slippage/priority/retries)
    // The rest of the bot uses the `Broker` trait, so PositionManagerActor
    // and the scoring/strategy pipeline are unchanged.
    let broker: Arc<dyn Broker> = match build_broker(&config.execution, config.start_balance_sol).await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("[FATAL] Failed to build broker: {e}");
            std::process::exit(1);
        }
    };
    println!("[BOOT] Broker active: {}", broker.mode_label());
    if config.scoring.legacy_scoring {
        eprintln!(
            "[BOOT] scoring=legacy_pre_v2 (YAML thresholds for snapshot+score; learning merge ignored)"
        );
    }

    // Persistent dev ranking + smart-money registries. Both are actors that
    // own their own state and flush JSON to disk every N seconds.
    let dev_ranker_handle = dev_ranker::spawn(config.persistence.clone());
    let smart_money_handle = smart_money::spawn(config.persistence.clone());

    let (mut manager_actor, manager_tx, mut event_rx, paused_state, balance_state) =
        PositionManagerActor::new(
            broker.clone(),
            config.start_balance_sol,
            config.buy_config.clone(),
            bot_trades,
            config.strategy.clone(),
            Some(dev_ranker_handle.clone()),
            Some(smart_money_handle.clone()),
            learning_log.clone(),
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
    // messages/sec per feed, and an [ALERT] line if the latest interval is
    // more than 3x the EMA. Keeps a single log line per feed every 30s.
    {
        let metrics_for_logger = feed_metrics_vec.clone();
        let bot_for_logger = bot_metrics.clone();
        tokio::spawn(async move {
            let interval_secs: u64 = 30;
            let mut last_messages: std::collections::HashMap<String, u64> =
                std::collections::HashMap::new();
            let mut ema_msgs: std::collections::HashMap<String, f64> =
                std::collections::HashMap::new();
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

                    println!(
                        "[metrics:{}] msgs/s={:.1} ema={:.1} ev/s={:.1} \
                         bytes/s={:.0} drop_failed={} drop_npd={} drop_self_dup={} \
                         cross_dup={} parse_err={} useful={:.3} subs={} reconn={}",
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
                    );

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
                     passed_filter={} score_skip={} score_a={} score_a_plus={} \
                     strategy_blocked={} positions_initiated={}",
                    b.creates_total,
                    b.creates_no_history,
                    b.creates_filter_rejected,
                    b.creates_passed_filter,
                    b.score_skipped,
                    b.score_a,
                    b.score_a_plus,
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
        let broker = broker.clone();
        let balance_state = balance_state.clone();

        async move {
            let mut tick_count: u64 = 0;
            loop {
                tick_count = tick_count.wrapping_add(1);

                // Pull a fresh value from RPC on the configured cadence; on
                // the other ticks reuse the broker's cached value. For the
                // mock broker `refresh_onchain_balance` is a no-op.
                if tick_count.is_multiple_of(balance_refresh_secs)
                    && let Err(e) = broker.refresh_onchain_balance().await {
                    eprintln!("[BROKER] balance refresh failed: {e}");
                }

                if let Ok(bal) = broker.balance_sol().await {
                    balance_state.store(f64::to_bits(bal), Ordering::Relaxed);
                }
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
        mode: broker.mode_label(),
        feed_metrics: feed_metrics_vec.clone(),
        bot_metrics: bot_metrics.clone(),
        manager_tx: manager_tx.clone(),
        dev_ranker: dev_ranker_handle.clone(),
        smart_money: smart_money_handle.clone(),
        tx_log: tx_log.clone(),
        config_path: "filter_config.yaml".to_string(),
        live_cfg: config.execution.live.clone(),
        buy_cap_floor,
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

        async fn get_bot_trades(State(state): State<ApiState>) -> impl IntoResponse {
            match sqlx::query_as::<_, BotTradeRow>(
                "SELECT id, mint, entry_mcap_sol, invested_sol, realized_pnl_pct, close_reason, closed_at, exit_mcap_sol, entry_meta \
                 FROM bot_trades ORDER BY closed_at DESC"
            )
            .fetch_all(&state.pool)
            .await {
                Ok(rows) => Json(rows).into_response(),
                Err(e) => {
                    eprintln!("[HTTP] bot_trades error: {e}");
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response()
                }
            }
        }

        async fn get_chart(
            State(state): State<ApiState>,
            Path(mint): Path<String>,
        ) -> impl IntoResponse {
            #[derive(sqlx::FromRow)]
            struct PricePoint {
                market_cap: f64,
            }
            #[derive(sqlx::FromRow)]
            struct BotTradeMarkerRow {
                entry_mcap_sol: f64,
                exit_mcap_sol: f64,
                realized_pnl_pct: f64,
                close_reason: String,
            }
            #[derive(serde::Serialize)]
            struct ChartMarker {
                entry_mcap: f64,
                exit_mcap: f64,
                pnl: f64,
                reason: String,
            }
            #[derive(serde::Serialize)]
            struct ChartResponse {
                prices: Vec<f64>,
                markers: Vec<ChartMarker>,
            }

            let price_rows = match sqlx::query_as::<_, PricePoint>(
                "SELECT market_cap::float8 AS market_cap \
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
            };

            let marker_rows = match sqlx::query_as::<_, BotTradeMarkerRow>(
                "SELECT entry_mcap_sol, exit_mcap_sol, realized_pnl_pct, close_reason \
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

            let prices: Vec<f64> = price_rows.into_iter().map(|p| p.market_cap).collect();
            let markers: Vec<ChartMarker> = marker_rows
                .into_iter()
                .map(|m| ChartMarker {
                    entry_mcap: m.entry_mcap_sol,
                    exit_mcap: m.exit_mcap_sol,
                    pnl: m.realized_pnl_pct,
                    reason: m.close_reason,
                })
                .collect();

            Json(ChartResponse { prices, markers }).into_response()
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
                mode: &'static str,
                wallet: String,
            }
            Json(Status {
                paused: state.paused.load(std::sync::atomic::Ordering::Relaxed),
                balance_sol: f64::from_bits(
                    state.balance.load(std::sync::atomic::Ordering::Relaxed),
                ),
                mode: state.mode,
                wallet: state.pubkey.clone(),
            })
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
                    .send(PositionMessage::AbandonPosition { mint })
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
            .route("/tx-log", get(get_tx_log))
            .route("/mode", get(get_mode).put(put_mode))
            .route("/positions", get(get_open_positions))
            .route("/positions/abandon", post(post_positions_abandon))
            .with_state(api_state);

        let listener = tokio::net::TcpListener::bind(&http_addr)
            .await
            .expect("Failed to bind HTTP server");
        println!("HTTP API active on: {}", http_addr);
        axum::serve(listener, app).await.unwrap();
    });

    while let Some((slot, event, bucket)) = general_rx.recv().await {
        match event {
            Action::Create(general_create) => {
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

                tokio::spawn({
                    let creators = creators.clone();
                    async move {
                        let unix_now = || {
                            SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_secs() as i64)
                                .unwrap_or(0)
                        };
                        // --- Stage 1: cheap pre-gate on dev history ---------
                        // Operator-tuned creator_config still acts as the
                        // hard pre-filter. Score Engine runs *after* this so
                        // we don't burn a scoring window on hopeless devs.
                        let dev_stats_opt =
                            match creators.get_creator_stats_in_sol(general_create.user).await {
                                Ok(stats) => stats,
                                Err(e) => {
                                    eprintln!("[FILTER] DB error for {}: {e}", general_create.user);
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

                        let dev_stats = match dev_stats_opt {
                            Some(s) => s,
                            None => {
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
                        };

                        if !filter_config.creator_config.filter(&dev_stats) {
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
                                tokio::spawn(async move {
                                    let _ = log
                                        .log_skipped(
                                            &mint_s,
                                            Some(dev_s.as_str()),
                                            "filter_creator",
                                            "creator_config_rejected",
                                            serde_json::json!({}),
                                            ts,
                                        )
                                        .await;
                                });
                            }
                            return;
                        }
                        registry.save(mint_address, dev_stats.clone()).await;
                        bot_metrics_create.note_passed_filter();

                        // --- Stage 2: scoring window + early tape ---------
                        let window_ms = filter_config.scoring.scoring_window_ms;
                        let tape_slices = filter_config
                            .scoring
                            .buyer_velocity_slices
                            .max(1);
                        let tape_points = features::observe_early_tape_points_live(
                            &launchpad_for_score,
                            mint_address,
                            window_ms,
                            tape_slices,
                        )
                        .await;

                        let scoring_bucket = features::fetch_live_bucket(
                            &launchpad_for_score,
                            mint_address,
                        )
                        .await
                        .unwrap_or(bucket_for_score.clone());

                        let pool_mcap = |b: &loggaper::launchpads::token_bucket::TokenBucket| {
                            b.pool().market_cap().amount().to_float()
                        };
                        let initial_mcap_sol = tape_points
                            .first()
                            .map(|p| p.mcap_sol)
                            .unwrap_or_else(|| pool_mcap(&scoring_bucket));
                        let current_mcap_sol = tape_points
                            .last()
                            .map(|p| p.mcap_sol)
                            .unwrap_or_else(|| pool_mcap(&scoring_bucket));

                        let merged_thr = merge_thresholds(
                            &filter_config.scoring.thresholds,
                            &learning_overrides_spawn.read().await.patch,
                        );
                        let thr_snapshot = if filter_config.scoring.legacy_scoring {
                            &filter_config.scoring.thresholds
                        } else {
                            &merged_thr
                        };

                        // --- Stage 3: snapshot features ---------------------
                        let (early_buyers, _buy_sizes_sol, buy_volume_sol, still_long, sold, bundle) =
                            features::snapshot_early_buyers(&scoring_bucket, thr_snapshot).await;

                        let (dev_category, dev_record) =
                            dev_ranker_for_create.category(general_create.user).await;
                        let buyers_for_position = early_buyers.all();
                        let smart_count = smart_money_for_create
                            .count_smart(buyers_for_position.clone())
                            .await;

                        let smart_addrs = smart_money_for_create
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

                        let token_features = features::assemble(
                            general_create.mint,
                            general_create.user,
                            Some(&dev_stats),
                            dev_category,
                            dev_record,
                            initial_mcap_sol,
                            current_mcap_sol,
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

                        let engine = ScoreEngine::new(&filter_config.scoring);
                        let thr_score = if filter_config.scoring.legacy_scoring {
                            &filter_config.scoring.thresholds
                        } else {
                            &merged_thr
                        };
                        let breakdown = engine.score(&token_features, thr_score);

                        eprintln!(
                            "[SCORE] {} tier={:?} score={} buyers={}+{} vol={:.2} \
                             mcap_init={:.1} mcap_now={:.1} bundle_sim={:.2} \
                             bundle_id={:.2} dev_cat={:?} smart={} bv_persist={:.2} \
                             sell_press={:.2} absorb={:.2} dumps={} sm_exits={} items={:?}",
                            general_create.mint,
                            breakdown.tier,
                            breakdown.total,
                            regular_buyer_count,
                            sniper_count,
                            buy_volume_sol,
                            initial_mcap_sol,
                            current_mcap_sol,
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
                            bot_metrics_create.note_score_skip();
                            if let Some(ref log) = learning_log_create {
                                let log = log.clone();
                                let mint_s = general_create.mint.to_string();
                                let dev_s = general_create.user.to_string();
                                let snap = LearningTradeSnapshot::from_scoring(&token_features, &breakdown);
                                let payload = serde_json::to_value(&snap).unwrap_or_else(|_| serde_json::json!({}));
                                let ts = unix_now();
                                tokio::spawn(async move {
                                    let _ = log
                                        .log_skipped(
                                            &mint_s,
                                            Some(dev_s.as_str()),
                                            "score_skip",
                                            "tier_skip",
                                            payload,
                                            ts,
                                        )
                                        .await;
                                });
                            }
                            return;
                        }

                        // Live-only gates: avoid A-tier noise entries with flat
                        // mcap (no `momentum_good`) and optionally require A+.
                        if filter_config.execution.mode == ExecutionMode::Live {
                            let has_momentum_good = breakdown
                                .items
                                .iter()
                                .any(|(name, _)| *name == "momentum_good");

                            if filter_config.scoring.require_momentum_good && !has_momentum_good {
                                eprintln!(
                                    "[BUY] {} skipped (live): require_momentum_good=true but no \
                                     momentum_good in items={:?}",
                                    general_create.mint,
                                    breakdown.items
                                );
                                if let Some(ref log) = learning_log_create {
                                    let log = log.clone();
                                    let mint_s = general_create.mint.to_string();
                                    let dev_s = general_create.user.to_string();
                                    let snap =
                                        LearningTradeSnapshot::from_scoring(&token_features, &breakdown);
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

                            if filter_config.scoring.minimum_tier_for_buy == MinBuyTier::APlus
                                && breakdown.tier != Tier::APlus
                            {
                                eprintln!(
                                    "[BUY] {} skipped (live): minimum_tier_for_buy=APlus but tier={:?}",
                                    general_create.mint,
                                    breakdown.tier
                                );
                                if let Some(ref log) = learning_log_create {
                                    let log = log.clone();
                                    let mint_s = general_create.mint.to_string();
                                    let dev_s = general_create.user.to_string();
                                    let snap =
                                        LearningTradeSnapshot::from_scoring(&token_features, &breakdown);
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
                        }

                        match breakdown.tier {
                            Tier::A => bot_metrics_create.note_score_a(),
                            Tier::APlus => bot_metrics_create.note_score_a_plus(),
                            Tier::Skip => unreachable!("tier Skip filtered above"),
                        }

                        // --- Stage 4: dispatch to manager (which still
                        // applies the StrategyController gate) -------------
                        let operator_cap =
                            f64::from_bits(buy_cap.load(std::sync::atomic::Ordering::Relaxed));
                        let amount_sol = breakdown
                            .recommended_size_sol
                            .min(operator_cap)
                            .max(0.0);
                        if amount_sol <= f64::EPSILON {
                            eprintln!(
                                "[BUY] {} skipped: tier size {:.4} capped to {:.4} (operator cap)",
                                general_create.mint, breakdown.recommended_size_sol, operator_cap
                            );
                            if let Some(ref log) = learning_log_create {
                                let log = log.clone();
                                let mint_s = general_create.mint.to_string();
                                let dev_s = general_create.user.to_string();
                                let snap = LearningTradeSnapshot::from_scoring(&token_features, &breakdown);
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
                            general_create.mint,
                            breakdown.tier,
                            breakdown.recommended_size_sol,
                            operator_cap,
                            amount_sol,
                        );
                        let open_reason = OpenReason::DevStats(dev_stats);
                        let learning_snapshot =
                            LearningTradeSnapshot::from_scoring(&token_features, &breakdown);

                        if tx
                            .send(PositionMessage::InitiateBuy {
                                pool: scoring_bucket.pool().clone_box(),
                                amount_sol,
                                open_reason,
                                dev_address: Some(general_create.user),
                                early_buyers: buyers_for_position,
                                learning_snapshot: Some(learning_snapshot),
                            })
                            .await
                            .is_ok()
                        {
                            bot_metrics_create.note_position_initiated();
                        }
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
                broker.on_trade(trade_action.as_ref(), bucket.pool());

                tokio::spawn({
                    let trades = trades.clone();
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

                        let start = Instant::now();
                        let _trader_stats =
                            match trades.get_trader_stats(trade_action.trader()).await {
                                Ok(stats) => stats,
                                Err(_) => return,
                            };
                        let _duration = start.elapsed();

                        let trader_type =
                            match bucket.swarm().get_trader(trade_action.trader()).await {
                                Some(trader) => trader.trader_type(),
                                None => return,
                            };

                        let _sol = trade_action.size().amount();
                        if trader_type == loggaper::trading::trader::TraderType::Regular {}
                    }
                });

                tokio::spawn({
                    let trade_action = trade_action.clone();
                    let trades = trades.clone();
                    let waiter = waiter_handle.clone();
                    let bucket = bucket.clone();
                    let _tx = manager_tx.clone();

                    async move {
                        waiter.wait_for(trade_action.mint()).await;

                        let trader = match bucket.swarm().get_trader(trade_action.trader()).await {
                            Some(trader) => trader,
                            None => return,
                        };

                        let _now = SystemTime::now();
                        let entry = TraderEntry {
                            trader_address: trade_action.trader().to_string(),
                            coin_address: trade_action.mint().to_string(),
                            realized_pnl: trader.pnl_percent(),
                            slot,
                            is_buy: trade_action.is_buy(),
                            market_cap: bucket.pool().market_cap(),
                            currency: trade_action.size(),
                            role: trader.trader_type(),
                        };

                        if let Err(_err) = trades.save_trade(entry).await {
                            println!("error while saving {}", bucket.pool().mint());
                        }
                    }
                });
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
