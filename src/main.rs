use dotenvy::dotenv;
use futures::{SinkExt, StreamExt};
use loggaper::{
    autobuy::{
        broker::Broker,
        broker_mock::MockBroker,
        manager::{OpenReason, PositionManagerActor, PositionMessage, WsCommand, WsFeedMessage},
        performance_tracker::{CreatorRegistryHandle, PerformanceTrackerHandle},
    },
    feed::metrics::{FeedMetrics, FeedSnapshot, new_dedup},
    generalize::general_commands::Action,
    persistence::{
        bot_trades::BotTradeRow,
        creators::CreatorRepository,
        postgres::creators::CreatorsRepositoryPostgres,
        tokens::TokenRepository,
        traders::{TraderEntry, TraderRepository},
    },
    pipelines::pump::PumpPipeline,
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
    sync::{broadcast, mpsc},
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

    #[derive(Clone)]
    struct ApiState {
        pool: sqlx::Pool<sqlx::Postgres>,
        creators: std::sync::Arc<CreatorsRepositoryPostgres>,
        paused: std::sync::Arc<std::sync::atomic::AtomicBool>,
        balance: std::sync::Arc<std::sync::atomic::AtomicU64>,
        buy_size: std::sync::Arc<std::sync::atomic::AtomicU64>,
        pubkey: String,
        feed_metrics: Arc<Vec<Arc<FeedMetrics>>>,
    }

    let (waiter_actor, waiter_handle) = DatabaseCreateWaiter::new();
    tokio::spawn(async move {
        waiter_actor.run().await;
    });

    let (ws_url, commitment_config) = setup_solana_rpc();
    let (general_tx, mut general_rx) = mpsc::channel(2048);
    let (broadcast_tx, _) = broadcast::channel::<WsFeedMessage>(4096);

    let private_key = std::env::var("PRIVATE_KEY").unwrap();
    let keypair = Arc::new(Keypair::from_base58_string(&private_key));
    let pubkey_string = keypair.pubkey().to_string();

    // Mock broker starts at the configured start balance (SOL). Keep this
    // in sync with `start_balance_sol` in filter_config.yaml so the manager
    // and the broker report the same initial balance.
    let broker = Arc::new(MockBroker::new(config.start_balance_sol));

    let (mut manager_actor, manager_tx, mut event_rx, paused_state, balance_state) =
        PositionManagerActor::new(
            broker.clone(),
            config.start_balance_sol,
            config.buy_config.clone(),
            bot_trades,
        );

    // Default buy size of 0.6 SOL, stored as f64 bits in an AtomicU64
    let buy_size_state = Arc::new(std::sync::atomic::AtomicU64::new(f64::to_bits(0.6_f64)));

    let broadcast_tx_bridge = broadcast_tx.clone();
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
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
    tokio::spawn(async move { pump.run() });

    // Periodic feed-metrics logger with simple anomaly alerting: EMA of
    // messages/sec per feed, and an [ALERT] line if the latest interval is
    // more than 3x the EMA. Keeps a single log line per feed every 30s.
    {
        let metrics_for_logger = feed_metrics_vec.clone();
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
                        "[metrics:{}] msgs/s={:.1} ema={:.1} events/s_avg={:.1} \
                         bytes/s_avg={:.0} reconnects={} stream_err={} parse_err={} \
                         err_logs={} cross_dup={} subs={}",
                        snap.name,
                        rate,
                        *ema,
                        snap.events_per_sec_avg,
                        snap.bytes_per_sec_avg,
                        snap.reconnects,
                        snap.stream_errors,
                        snap.parse_errors,
                        snap.err_logs,
                        snap.duplicates_cross_feed,
                        snap.subscribed,
                    );

                    if prior > 1.0 && rate > prior * 3.0 {
                        println!(
                            "[ALERT:{}] message rate spike: {:.1}/s (ema {:.1}/s)",
                            snap.name, rate, prior
                        );
                    }
                }
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

    tokio::spawn({
        let broker = broker.clone();
        let balance_state = balance_state.clone();

        async move {
            loop {
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
        feed_metrics: feed_metrics_vec.clone(),
    };

    let http_addr = format!("0.0.0.0:{}", config.http_port);
    tokio::spawn(async move {
        use axum::{
            Json, Router,
            extract::{Path, State},
            response::IntoResponse,
            routing::get,
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
                "SELECT id, mint, entry_mcap_sol, invested_sol, realized_pnl_pct, close_reason, closed_at, exit_mcap_sol \
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
            }
            Json(Status {
                paused: state.paused.load(std::sync::atomic::Ordering::Relaxed),
                balance_sol: f64::from_bits(
                    state.balance.load(std::sync::atomic::Ordering::Relaxed),
                ),
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
            let snaps: Vec<FeedSnapshot> =
                state.feed_metrics.iter().map(|m| m.snapshot()).collect();
            Json(snaps)
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
            state
                .buy_size
                .store(f64::to_bits(sol), std::sync::atomic::Ordering::Relaxed);
            eprintln!("[HTTP] buy size updated to {sol} SOL");
            axum::http::StatusCode::NO_CONTENT.into_response()
        }

        let app = Router::new()
            .route("/bot-trades", get(get_bot_trades))
            .route("/status", get(get_status))
            .route("/pubkey", get(get_pubkey))
            .route("/dev-stats/{mint}", get(get_dev_stats))
            .route("/chart/{mint}", get(get_chart))
            .route("/buy-size", get(get_buy_size).put(set_buy_size))
            .route("/metrics", get(get_metrics))
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
                let creators = creators.clone();
                let tx = manager_tx.clone();
                let filter_config = config.clone();
                let registry = registry.clone();
                let mint_address = general_create.mint;
                let buy_size = buy_size_state.clone();

                tokio::spawn({
                    let creators = creators.clone();
                    async move {
                        let dev_stats_opt =
                            match creators.get_creator_stats_in_sol(general_create.user).await {
                                Ok(stats) => stats,
                                Err(e) => {
                                    eprintln!("[FILTER] DB error for {}: {e}", general_create.user);
                                    return;
                                }
                            };

                        match &dev_stats_opt {
                            Some(s) => eprintln!(
                                "[FILTER] {} stats: coins={} pnl={:.1}% holders={}",
                                general_create.mint,
                                s.total_coins,
                                s.trader_pnl_average,
                                s.total_holders_average,
                            ),
                            None => {
                                eprintln!("[FILTER] {} no creator history", general_create.mint)
                            }
                        }

                        let open_reason = match dev_stats_opt {
                            Some(stats) => {
                                registry.save(mint_address, stats.clone()).await;
                                if !filter_config.creator_config.filter(&stats) {
                                    eprintln!(
                                        "[FILTER] {} rejected by filter",
                                        general_create.mint
                                    );
                                    return;
                                }
                                OpenReason::DevStats(stats)
                            }
                            None => {
                                // No creator history → do NOT open. We
                                // intentionally disable the TraderStats
                                // fallback so positions are only opened on
                                // developers with proven track record.
                                eprintln!(
                                    "[FILTER] {} skipped: no creator history",
                                    general_create.mint
                                );
                                return;
                            }
                        };

                        let amount_sol =
                            f64::from_bits(buy_size.load(std::sync::atomic::Ordering::Relaxed));

                        let _ = tx
                            .send(PositionMessage::InitiateBuy {
                                pool: bucket.pool().clone_box(),
                                amount_sol,
                                open_reason,
                            })
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

                tokio::spawn({
                    let trades = trades.clone();
                    let trade_action = trade_action.clone();
                    let bucket = bucket.clone();
                    let tx = manager_tx.clone();
                    let tracker = tracker.clone();
                    let registry = registry.clone();

                    {
                        let _ = tx
                            .send(PositionMessage::UpdatePool(bucket.pool().clone_box()))
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
