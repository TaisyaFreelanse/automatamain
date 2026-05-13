use crate::{
    autobuy::{
        broker::Broker,
        positions::Position,
    },
    generalize::general_pool::Pool,
    helper::Amount,
    persistence::{
        bot_trades::{BotTradeEntry, BotTradeRepository},
        creators::CreatorStatistics,
    },
    scoring::{
        config::StrategyConfig,
        dev_ranker::{DevRankerHandle, TokenOutcome},
        smart_money::SmartMoneyHandle,
        strategy_controller::{BuyDecision, StrategyController, StrategySnapshot},
    },
};
use solana_address::Address;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc as StdArc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, oneshot},
    time::sleep,
};

// --- КОНФИГУРАЦИЯ ---

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SmartBuyConfig {
    /// SOL amount to spend per buy
    pub amount_sol: f64,
    /// Take Profit 1 trigger threshold (%)
    pub tp1_pct: f64,
    /// Take Profit 1 sell size (% of current holdings)
    pub tp1_sell_pct: f64,
    /// Take Profit 2 trigger threshold (%)
    pub tp2_pct: f64,
    /// Take Profit 2 sell size (% of current holdings)
    pub tp2_sell_pct: f64,
    /// Initial stop loss — stored as profit floor, always use negative for a loss
    /// (e.g. -25.0 means "exit if profit drops below -25%").
    /// After smart-stop / trailing activation this floor is raised to a positive value
    /// (e.g. 5.0 means "exit if profit drops below +5%").
    pub exit_profit_floor: f64,
    /// Time kill: sell after this many seconds if profit is too low
    pub time_kill_secs: u64,
    /// Time kill: minimum profit to survive the time kill check (%)
    pub time_kill_min_profit_pct: f64,
    /// Trailing stop: how far (%) below the peak mcap before we exit
    pub trailing_stop_drawdown_pct: f64,
}

impl Default for SmartBuyConfig {
    fn default() -> Self {
        Self {
            amount_sol: 0.1,
            tp1_pct: 30.0,
            tp1_sell_pct: 30.0,
            tp2_pct: 50.0,
            tp2_sell_pct: 15.0,
            exit_profit_floor: -40.0, // lose at most 25%
            time_kill_secs: 25,
            time_kill_min_profit_pct: 10.0,
            trailing_stop_drawdown_pct: 30.0, // exit if mcap drops 30% from peak
        }
    }
}

// --- ПРИЧИНА ОТКРЫТИЯ ---

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum OpenReason {
    DevStats(CreatorStatistics),
    TraderStats,
}

// --- СООБЩЕНИЯ ---

pub enum PositionMessage {
    /// Pool state update arriving from websocket / network
    UpdatePool(Box<dyn Pool>),
    /// Pause or resume opening new positions (existing ones keep being managed)
    SetPaused(bool),
    /// Request to open a position (executed after 800 ms delay)
    InitiateBuy {
        pool: Box<dyn Pool>,
        amount_sol: f64,
        open_reason: OpenReason,
        /// Token developer (used for dev_ranker on close).
        dev_address: Option<Address>,
        /// Snapshot of early buyers (used for smart_money on close).
        early_buyers: Vec<Address>,
    },
    /// Internal: actual buy after delay
    ExecuteBuy {
        mint: solana_address::Address,
        amount_sol: f64,
        open_reason: OpenReason,
        dev_address: Option<Address>,
        early_buyers: Vec<Address>,
    },
    /// Snapshot the current strategy state (caps, loss-streak, regime).
    /// Used by the HTTP /metrics endpoint.
    GetStrategySnapshot {
        responder: oneshot::Sender<StrategySnapshot>,
    },
    /// Internal: actual sell after delay
    ExecuteSell {
        mint: solana_address::Address,
        percent: f64,
        reason: String,
    },
    GetPnl {
        mint: solana_address::Address,
        responder: tokio::sync::oneshot::Sender<Option<f64>>,
    },
    /// Tick: evaluate all open positions and redraw dashboard
    Tick,
}

// --- ПОЗИЦИЯ (расширена) ---
//
// FIX Bug 5: added `pending_partial_sell` flag so that TP1 and TP2 cannot
// both be queued simultaneously before either ExecuteSell arrives.

// NOTE: these fields must be added to your existing `Position` struct:
//
//   pub exit_profit_floor: f64,     // replaces stop_loss_pct (unified sign convention)
//   pub trailing_active: bool,
//   pub highest_mcap: f64,
//   pub tp1_triggered: bool,
//   pub tp2_triggered: bool,
//   pub is_closing: bool,
//   pub pending_partial_sell: bool, // NEW — guards against double partial-sell queue
//
// Everything below assumes those fields exist.

// --- СТРУКТУРА АКТОР-МЕНЕДЖЕРА ---

pub struct PositionManagerActor {
    config: SmartBuyConfig,
    broker: Arc<dyn Broker>,
    positions: HashMap<solana_address::Address, Position>,
    /// Tracks mints with an ExecuteBuy already in-flight (800ms delay window).
    pending_buys: HashSet<solana_address::Address>,
    /// Mints that have been fully closed — never re-enter.
    closed_mints: HashSet<solana_address::Address>,
    /// Stores the most recent pool state (used for slippage simulation)
    pool_cache: HashMap<solana_address::Address, Box<dyn Pool>>,
    tx: mpsc::Sender<PositionMessage>,
    rx: mpsc::Receiver<PositionMessage>,
    last_print_time: u64,
    event_tx: mpsc::Sender<WsFeedMessage>,
    bot_trades: Arc<dyn BotTradeRepository + Send + Sync>,
    /// When true, InitiateBuy signals are dropped; open positions still managed.
    paused: bool,
    paused_state: StdArc<AtomicBool>,
    balance_state: StdArc<std::sync::atomic::AtomicU64>,
    strategy: StrategyController,
    dev_ranker: Option<DevRankerHandle>,
    smart_money: Option<SmartMoneyHandle>,
}

impl PositionManagerActor {
    pub fn new(
        broker: Arc<dyn Broker>,
        initial_balance_sol: f64,
        config: SmartBuyConfig,
        bot_trades: Arc<dyn BotTradeRepository + Send + Sync>,
        strategy_cfg: StrategyConfig,
        dev_ranker: Option<DevRankerHandle>,
        smart_money: Option<SmartMoneyHandle>,
    ) -> (
        Self,
        mpsc::Sender<PositionMessage>,
        mpsc::Receiver<WsFeedMessage>,
        StdArc<AtomicBool>,
        StdArc<std::sync::atomic::AtomicU64>,
    ) {
        let (tx, rx) = mpsc::channel(10_000);
        let (event_tx, event_rx) = mpsc::channel(4096);
        let paused_state = StdArc::new(AtomicBool::new(false));
        let balance_state = StdArc::new(std::sync::atomic::AtomicU64::new(initial_balance_sol.to_bits()));
        let actor = Self {
            config,
            broker,
            positions: HashMap::new(),
            pending_buys: HashSet::new(),
            closed_mints: HashSet::new(),
            pool_cache: HashMap::new(),
            tx: tx.clone(),
            rx,
            last_print_time: 0,
            event_tx,
            bot_trades,
            paused: false,
            paused_state: paused_state.clone(),
            balance_state: balance_state.clone(),
            strategy: StrategyController::new(strategy_cfg),
            dev_ranker,
            smart_money,
        };
        (actor, tx, event_rx, paused_state, balance_state)
    }

    /// Main actor loop — single-threaded, no locking needed.
    pub async fn run(&mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                // ----------------------------------------------------------------
                PositionMessage::UpdatePool(pool) => {
                    let mint = pool.mint();
                    self.pool_cache.insert(mint, pool.clone_box());
                    if let Some(pos) = self.positions.get_mut(&mint) {
                        pos.pool = pool;
                    }
                }

                PositionMessage::SetPaused(paused) => {
                    self.paused = paused;
                    self.paused_state.store(paused, Ordering::Relaxed);
                    let _ = self.event_tx.try_send(WsFeedMessage::PausedState { paused });
                    eprintln!(
                        "[MANAGER] New positions {}",
                        if paused { "PAUSED" } else { "RESUMED" }
                    );
                }

                // ----------------------------------------------------------------
                PositionMessage::InitiateBuy {
                    pool,
                    amount_sol,
                    open_reason,
                    dev_address,
                    early_buyers,
                } => {
                    if self.paused {
                        continue;
                    }

                    let mint = pool.mint();

                    // Reject if a position is already open OR an ExecuteBuy is already
                    // in-flight for this mint. Without the pending_buys check, multiple
                    // InitiateBuy signals arriving within the 800ms window would all
                    // pass the positions guard and spawn redundant ExecuteBuy tasks.
                    if self.positions.contains_key(&mint)
                        || self.pending_buys.contains(&mint)
                        || self.closed_mints.contains(&mint)
                    {
                        continue;
                    }

                    // --- Strategy controller gate -----------------------------------
                    let open_now = (self.positions.len() + self.pending_buys.len()) as u32;
                    match self.strategy.can_open(open_now) {
                        BuyDecision::Allow => {}
                        block => {
                            eprintln!("[STRATEGY] {mint} blocked: {:?}", block);
                            continue;
                        }
                    }

                    self.pending_buys.insert(mint);
                    self.pool_cache.insert(mint, pool);
                    let tx_clone = self.tx.clone();
                    tokio::spawn(async move {
                        sleep(Duration::from_millis(800)).await;
                        let _ = tx_clone
                            .send(PositionMessage::ExecuteBuy {
                                mint,
                                amount_sol,
                                open_reason,
                                dev_address,
                                early_buyers,
                            })
                            .await;
                    });
                }

                // ----------------------------------------------------------------
                PositionMessage::GetStrategySnapshot { responder } => {
                    let _ = responder.send(self.strategy.snapshot());
                }

                // ----------------------------------------------------------------
                PositionMessage::ExecuteBuy {
                    mint,
                    amount_sol,
                    open_reason,
                    dev_address,
                    early_buyers,
                } => {
                    self.pending_buys.remove(&mint);

                    if self.paused {
                        continue;
                    }

                    if self.positions.contains_key(&mint) {
                        eprintln!("[BUY] Skipped {mint} — position already open (ExecuteBuy)");
                        continue;
                    }

                    let Some(latest_pool) = self.pool_cache.get(&mint).map(|p| p.clone_box()) else {
                        eprintln!("[BUY] Skipped {mint} — no pool in cache");
                        continue;
                    };

                    let receipt = match self.broker.buy(mint, amount_sol, latest_pool.as_ref()).await {
                        Ok(r) => r,
                        Err(e) => {
                            eprintln!("[BUY] Failed {mint}: {e}");
                            continue;
                        }
                    };

                    let current_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();

                    let tokens = Amount::from_float_native(receipt.tokens_received);
                    let mut position = Position::new(latest_pool, tokens, current_time);
                    position.exit_profit_floor = self.config.exit_profit_floor;
                    position.spent_sol = amount_sol;
                    position.dev_address = dev_address;
                    position.early_buyers = early_buyers;
                    let enter_mcap = position.enter_mcap.to_float();
                    eprintln!("[BUY] Opened {mint} | mcap={enter_mcap:.1} SOL | spent={amount_sol:.4} SOL");
                    self.positions.insert(mint, position);
                    self.strategy.note_position_opened();

                    if let Ok(bal) = self.broker.balance_sol().await {
                        self.balance_state.store(bal.to_bits(), Ordering::Relaxed);
                        let _ = self.event_tx.try_send(WsFeedMessage::BalanceUpdate { balance: bal });
                    }
                    let _ = self.event_tx.try_send(WsFeedMessage::PositionOpen {
                        address: mint.to_string(),
                        open_reason,
                        enter_mcap,
                    });
                }

                // ----------------------------------------------------------------
                PositionMessage::ExecuteSell {
                    mint,
                    percent,
                    reason,
                } => {
                    if let Some(mut pos) = self.positions.remove(&mint) {
                        let actual_pnl = pos.pnl();
                        let entry_mcap_sol = pos.enter_mcap.to_float();
                        let invested_sol = pos.initial_holdings.to_float();
                        let exit_mcap_sol = pos.pool.market_cap().amount().to_float();

                        let sell_qty = pos.holdings.to_float() * (percent / 100.0);

                        let return_value = match self.broker.sell(mint, sell_qty, pos.pool.as_ref()).await {
                            Ok(r) => r.sol_received,
                            Err(e) => {
                                eprintln!("[SELL] Broker error for {mint}: {e}");
                                self.positions.insert(mint, pos);
                                continue;
                            }
                        };

                        println!(
                            "[SELL] {mint} | reason={reason} | pnl={actual_pnl:+.2}% \
                             | sold={percent:.0}% of holdings | returned={return_value:.4} SOL"
                        );

                        pos.total_returned += return_value;
                        let total_returned = pos.total_returned;

                        // Snapshot fields needed in the close branch BEFORE
                        // potentially moving `pos` back into the map for
                        // partial sells.
                        let close_dev_address = pos.dev_address;
                        let close_spent_sol = pos.spent_sol;
                        let close_early_buyers = if percent >= 100.0 {
                            std::mem::take(&mut pos.early_buyers)
                        } else {
                            Vec::new()
                        };

                        if percent < 100.0 {
                            pos.holdings = Amount::from_float_native(pos.holdings.to_float() - sell_qty);
                            pos.pending_partial_sell = false;
                            self.positions.insert(mint, pos);
                        }

                        if let Ok(bal) = self.broker.balance_sol().await {
                            self.balance_state.store(bal.to_bits(), Ordering::Relaxed);
                            let _ = self.event_tx.try_send(WsFeedMessage::BalanceUpdate { balance: bal });
                        }

                        if percent >= 100.0 {
                            self.closed_mints.insert(mint);
                            let _ = self.pool_cache.remove(&mint);

                            let overall_pnl_pct = (total_returned / invested_sol - 1.0) * 100.0;

                            // Real SOL PnL of the whole position. The
                            // historical `invested_sol = initial_holdings.to_float()`
                            // field above is in tokens, not SOL — so we
                            // recompute against the actual SOL spent on
                            // entry, captured on Position at ExecuteBuy.
                            let pnl_sol = if close_spent_sol > 0.0 {
                                total_returned - close_spent_sol
                            } else {
                                0.0
                            };
                            let pnl_pct_sol = if close_spent_sol > 0.0 {
                                (total_returned / close_spent_sol - 1.0) * 100.0
                            } else {
                                overall_pnl_pct
                            };

                            // Strategy controller bookkeeping (loss streak,
                            // daily caps, regime pause).
                            self.strategy.note_position_closed(pnl_sol);

                            // Dev ranker / smart money updates. Both async,
                            // spawned so the actor loop doesn't block.
                            if let Some(dev) = close_dev_address
                                && let Some(handle) = self.dev_ranker.clone() {
                                    let outcome = TokenOutcome::classify(pnl_pct_sol);
                                    tokio::spawn(async move {
                                        handle.note_outcome(dev, outcome).await;
                                    });
                                }
                            if !close_early_buyers.is_empty()
                                && let Some(handle) = self.smart_money.clone() {
                                    let buyers = close_early_buyers;
                                    let pnl = pnl_pct_sol;
                                    tokio::spawn(async move {
                                        handle.note_trade_outcome(buyers, pnl).await;
                                    });
                                }

                            let _ = self.event_tx.try_send(WsFeedMessage::PositionClose {
                                address: mint.to_string(),
                                reason: reason.clone(),
                            });

                            let entry = BotTradeEntry {
                                mint: mint.to_string(),
                                entry_mcap_sol,
                                invested_sol,
                                realized_pnl_pct: overall_pnl_pct,
                                close_reason: reason,
                                closed_at: SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs() as i64,
                                exit_mcap_sol,
                            };
                            let repo = self.bot_trades.clone();
                            tokio::spawn(async move {
                                if let Err(e) = repo.save_bot_trade(entry).await {
                                    eprintln!("[BOT TRADE] save failed: {e:?}");
                                }
                            });
                        }
                    }
                }

                // ----------------------------------------------------------------
                PositionMessage::Tick => {
                    self.process_positions().await;
                }
                PositionMessage::GetPnl { mint, responder } => {
                    // Check if the position exists, grab its PnL, and send it back
                    let pnl = self.positions.get(&mint).map(|pos| pos.pnl());

                    // The send fails if the receiver was dropped, which we can safely ignore
                    let _ = responder.send(pnl);
                }
            }
        }
    }

    async fn process_positions(&mut self) {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut actions: Vec<(solana_address::Address, f64, String)> = Vec::new();

        for (mint, pos) in self.positions.iter_mut() {
            // Skip positions that are already queued for a full close.
            if pos.is_closing {
                continue;
            }

            let current_mcap = pos.pool.market_cap().amount().to_float();
            if current_mcap > pos.highest_mcap {
                pos.highest_mcap = current_mcap;
            }

            let profit = pos.pnl();

            let _ = self.event_tx.try_send(WsFeedMessage::PositionUpdate {
                address: mint.to_string(),
                pnl: profit,
                holdings: pos.holdings.to_float(),
                market_cap: current_mcap,
            });

            // --- 1. Time Kill ---
            if current_time >= pos.entry_time + self.config.time_kill_secs
                && profit < self.config.time_kill_min_profit_pct
            {
                pos.is_closing = true;
                actions.push((*mint, 100.0, "TIME KILL".to_string()));
                continue;
            }

            // --- 2. Stop Loss / Smart Stop ---
            // FIX Bug 4: unified sign convention — floor can be negative (initial SL)
            // or positive (raised after smart-stop). The check is always the same:
            // "exit if current profit is below the floor".
            if profit <= pos.exit_profit_floor {
                pos.is_closing = true;
                actions.push((
                    *mint,
                    100.0,
                    format!("SL (floor {:.1}%)", pos.exit_profit_floor),
                ));
                continue;
            }

            // --- 3. Hard market-cap ceiling ---
            if current_mcap >= 350.0 {
                pos.is_closing = true;
                actions.push((*mint, 100.0, "MCAP CEILING".to_string()));
                continue;
            }

            // --- 4. Trailing Stop ---
            if pos.trailing_active {
                // FIX Bug 3: use config-driven drawdown percentage, not hardcoded 0.70.
                let keep_fraction = 1.0 - self.config.trailing_stop_drawdown_pct / 100.0;
                let trailing_stop_mcap = pos.highest_mcap * keep_fraction;
                if current_mcap <= trailing_stop_mcap {
                    pos.is_closing = true;
                    actions.push((*mint, 100.0, "TRAILING EXIT".to_string()));
                    continue;
                }
            }

            // --- Profit-protection level upgrades ---
            if profit >= 80.0 {
                if !pos.trailing_active {
                    pos.trailing_active = true;
                    // Raise floor: do not give back more than 35 % from peak.
                    // FIX Bug 4: positive floor value is intentional here — it means
                    // "exit if profit drops below +35%", consistent with the unified
                    // convention used in the stop-loss check above.
                    pos.exit_profit_floor = 35.0;
                }
            } else if profit >= 50.0
                && pos.exit_profit_floor < 5.0 {
                    pos.exit_profit_floor = 5.0; // Smart Stop: lock in at least +5%
                }

            // FIX Bug 5: only queue one partial sell at a time.
            // If a partial sell is already in-flight, skip TP checks this tick.
            if pos.pending_partial_sell {
                continue;
            }

            // FIX Bug 1: check TP1 before TP2 so they fire in the correct order.
            // Both cannot be queued simultaneously thanks to the flag above.
            if profit >= self.config.tp1_pct && !pos.tp1_triggered {
                pos.tp1_triggered = true;
                pos.pending_partial_sell = true;
                actions.push((*mint, self.config.tp1_sell_pct, "TP1".to_string()));
                continue; // don't evaluate TP2 until TP1 ExecuteSell clears the flag
            }

            if profit >= self.config.tp2_pct && !pos.tp2_triggered {
                pos.tp2_triggered = true;
                pos.pending_partial_sell = true;
                actions.push((*mint, self.config.tp2_sell_pct, "TP2".to_string()));
            }
        }

        // Execute collected actions after releasing the mutable borrow.
        for (mint, percent, reason) in actions {
            self.schedule_sell(mint, percent, reason);
        }

        if current_time > self.last_print_time {
            // self.print_dashboard(current_time);
            self.last_print_time = current_time;
        }
    }

    /// Spawns an 800 ms delay before sending the sell order (simulates tx latency).
    fn schedule_sell(&self, mint: solana_address::Address, percent: f64, reason: String) {
        let tx_clone = self.tx.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(800)).await;
            let _ = tx_clone
                .send(PositionMessage::ExecuteSell {
                    mint,
                    percent,
                    reason,
                })
                .await;
        });
    }

    #[allow(dead_code)]
    fn print_dashboard(&self, current_time: u64) {
        print!("{}[2J{}[1;1H", 27 as char, 27 as char);

        println!("=========================================================================");
        println!("🚀 LOGGAPER ACTOR-BASED POSITION MANAGER");
        println!("=========================================================================");
        println!("Time (Unix) : {}", current_time);
        let bal_bits = self.balance_state.load(Ordering::Relaxed);
        println!("Balance     : {:.4} SOL", f64::from_bits(bal_bits));
        println!("Open Trades : {}", self.positions.len());
        println!("-------------------------------------------------------------------------");

        if self.positions.is_empty() {
            println!("No active positions.");
            println!("=========================================================================\n");
            return;
        }

        println!(
            "{:<10} | {:<9} | {:<9} | {:<10} | {:<10} | {:<20}",
            "Mint", "Entry MC", "Curr MC", "PNL %", "Holdings", "Status"
        );
        println!("-------------------------------------------------------------------------");

        for (mint, pos) in &self.positions {
            let mint_str = mint.to_string();
            let short_mint = if mint_str.len() > 8 {
                format!("{}..{}", &mint_str[..4], &mint_str[mint_str.len() - 4..])
            } else {
                mint_str
            };

            let pnl = pos.pnl();
            let pnl_str = if pnl >= 0.0 {
                format!("\x1B[32m+{:.2}%\x1B[0m", pnl)
            } else {
                format!("\x1B[31m{:.2}%\x1B[0m", pnl)
            };

            let curr_mcap = pos.pool.market_cap().amount().to_float();
            let entry_mcap = pos.enter_mcap.to_float();

            let holdings = pos.holdings.to_float();

            let mut status = format!("floor: {:.0}%", pos.exit_profit_floor);

            if pos.trailing_active {
                status.push_str(" [TRAIL]");
            }
            if pos.tp1_triggered {
                status.push_str(" [TP1]");
            }
            if pos.tp2_triggered {
                status.push_str(" [TP2]");
            }
            if pos.pending_partial_sell {
                status.push_str(" [PARTIAL]");
            }
            if pos.is_closing {
                status.push_str(" [CLOSING]");
            }

            println!(
                "{:<10} | {:<9.1} | {:<9.1} | {:<10} | {} | {}",
                short_mint, entry_mcap, curr_mcap, pnl_str, holdings, status
            );
        }

        println!("=========================================================================\n");
    }
}

#[derive(Clone)]
pub struct PositionManagerHandler {
    tx: mpsc::Sender<PositionMessage>,
}

impl PositionManagerHandler {
    pub fn new(tx: mpsc::Sender<PositionMessage>) -> Self {
        Self { tx }
    }

    /// Update the pool state from market data feeds
    pub async fn update_pool(&self, pool: Box<dyn Pool>) {
        let _ = self.tx.send(PositionMessage::UpdatePool(pool)).await;
    }

    /// Trigger a new position entry
    pub async fn initiate_buy(
        &self,
        pool: Box<dyn Pool>,
        amount_sol: f64,
        open_reason: OpenReason,
        dev_address: Option<Address>,
        early_buyers: Vec<Address>,
    ) {
        let _ = self
            .tx
            .send(PositionMessage::InitiateBuy {
                pool,
                amount_sol,
                open_reason,
                dev_address,
                early_buyers,
            })
            .await;
    }

    /// Pause or resume opening new positions. Open positions continue to be managed.
    pub async fn set_paused(&self, paused: bool) {
        let _ = self.tx.send(PositionMessage::SetPaused(paused)).await;
    }

    /// Manual trigger to force a tick update if external events occur
    pub async fn tick(&self) {
        let _ = self.tx.send(PositionMessage::Tick).await;
    }

    /// Fetch PnL for a specific mint (Uses a oneshot channel)
    pub async fn get_pnl(&self, mint: solana_address::Address) -> Option<f64> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self
            .tx
            .send(PositionMessage::GetPnl {
                mint,
                responder: resp_tx,
            })
            .await;

        resp_rx.await.unwrap_or(None)
    }
}

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum WsCommand {
    SetPaused { paused: bool },
}

#[derive(Serialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsFeedMessage {
    PositionOpen {
        address: String,
        open_reason: OpenReason,
        enter_mcap: f64,
    },
    PositionUpdate {
        address: String,
        pnl: f64,
        holdings: f64,
        market_cap: f64,
    },
    PositionClose {
        address: String,
        reason: String,
    },
    BalanceUpdate {
        balance: f64,
    },
    PausedState {
        paused: bool,
    },
}
