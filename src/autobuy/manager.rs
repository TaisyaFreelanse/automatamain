use crate::{
    autobuy::{
        broker::{Broker, BuyReceipt},
        positions::Position,
    },
    generalize::general_pool::Pool,
    helper::Amount,
    learning::{LearningLogPg, LearningTradeSnapshot},
    persistence::{
        bot_trades::{BotTradeEntry, BotTradeRepository},
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
use serde_json::json;
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

// --- WebSocket / dashboard wire: V3 entry tape --------------------------------

#[derive(Serialize, Clone, Debug, Default)]
pub struct V3TapeWire {
    pub bv_persist: f64,
    pub sell_press: f64,
    pub absorb: f64,
    pub dumps: u32,
    pub sm_exits: u32,
}

impl V3TapeWire {
    pub fn from_learning(s: &LearningTradeSnapshot) -> Self {
        Self {
            bv_persist: s.buyer_velocity_persistence,
            sell_press: s.sell_pressure_score,
            absorb: s.absorb_quality_score,
            dumps: s.repeat_dump_slices,
            sm_exits: s.smart_wallet_early_exits,
        }
    }
}

pub fn entry_meta_json(snap: Option<&LearningTradeSnapshot>) -> String {
    snap.map(|s| {
        let w = V3TapeWire::from_learning(s);
        serde_json::to_string(&w).unwrap_or_default()
    })
    .unwrap_or_default()
}

// --- КОНФИГУРАЦИЯ ---

fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

fn default_tp3_pct() -> f64 {
    100.0
}
fn default_tp3_sell_pct() -> f64 {
    10.0
}
fn default_tp4_pct() -> f64 {
    150.0
}
fn default_tp4_sell_pct() -> f64 {
    10.0
}
fn default_tp5_pct() -> f64 {
    200.0
}
fn default_tp5_sell_pct() -> f64 {
    10.0
}

fn default_time_kill_adaptive() -> bool {
    true
}
fn default_time_kill_weak_secs() -> u64 {
    22
}
fn default_time_kill_strong_secs() -> u64 {
    60
}
fn default_time_kill_neutral_secs() -> u64 {
    32
}
fn default_time_kill_strong_min_buyers() -> u64 {
    38
}
fn default_time_kill_strong_min_b2s() -> f64 {
    4.5
}
fn default_time_kill_weak_max_buyers() -> u64 {
    28
}
fn default_time_kill_weak_max_b2s() -> f64 {
    3.0
}
fn default_time_kill_vel_strong() -> f64 {
    0.06
}
fn default_time_kill_vel_flat() -> f64 {
    0.025
}
fn default_time_kill_early_green_pct() -> f64 {
    8.0
}
fn default_time_kill_peak_dd_weak() -> f64 {
    0.055
}

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
    /// Take Profit 3 — partial at high mcap PnL (default +100%)
    #[serde(default = "default_tp3_pct")]
    pub tp3_pct: f64,
    #[serde(default = "default_tp3_sell_pct")]
    pub tp3_sell_pct: f64,
    /// Take Profit 4 (default +150%)
    #[serde(default = "default_tp4_pct")]
    pub tp4_pct: f64,
    #[serde(default = "default_tp4_sell_pct")]
    pub tp4_sell_pct: f64,
    /// Take Profit 5 (default +200%)
    #[serde(default = "default_tp5_pct")]
    pub tp5_pct: f64,
    #[serde(default = "default_tp5_sell_pct")]
    pub tp5_sell_pct: f64,
    /// Initial stop loss — stored as profit floor, always use negative for a loss
    /// (e.g. -25.0 means "exit if profit drops below -25%").
    /// After smart-stop / trailing activation this floor is raised to a positive value
    /// (e.g. 5.0 means "exit if profit drops below +5%").
    pub exit_profit_floor: f64,
    /// Time kill: sell after this many seconds if profit is too low
    pub time_kill_secs: u64,
    /// Time kill: minimum profit to survive the time kill check (%)
    pub time_kill_min_profit_pct: f64,
    /// When true, time-kill window is chosen from weak/neutral/strong using entry snapshot + mcap tape.
    #[serde(default = "default_time_kill_adaptive")]
    pub time_kill_adaptive: bool,
    /// Weak launch: aggressive cut-off (seconds).
    #[serde(default = "default_time_kill_weak_secs")]
    pub time_kill_weak_secs: u64,
    /// Strong launch: extended consolidation window (seconds).
    #[serde(default = "default_time_kill_strong_secs")]
    pub time_kill_strong_secs: u64,
    /// Neither clearly weak nor strong.
    #[serde(default = "default_time_kill_neutral_secs")]
    pub time_kill_neutral_secs: u64,
    /// Entry buyers at or above this count vote "strong".
    #[serde(default = "default_time_kill_strong_min_buyers")]
    pub time_kill_strong_min_buyers: u64,
    /// Entry buy/sell at or above this votes "strong".
    #[serde(default = "default_time_kill_strong_min_b2s")]
    pub time_kill_strong_min_b2s: f64,
    /// Entry buyers below this vote "weak" (with other weak cues).
    #[serde(default = "default_time_kill_weak_max_buyers")]
    pub time_kill_weak_max_buyers: u64,
    /// Entry buy/sell below this (when > 0) votes "weak".
    #[serde(default = "default_time_kill_weak_max_b2s")]
    pub time_kill_weak_max_b2s: f64,
    /// Live mcap velocity (% of entry mcap / sec) at or above → continuation vote.
    #[serde(default = "default_time_kill_vel_strong")]
    pub time_kill_vel_strong: f64,
    /// |velocity| at or below this (after a few seconds held) → stagnation vote.
    #[serde(default = "default_time_kill_vel_flat")]
    pub time_kill_vel_flat: f64,
    /// In-position mcap PnL at or above this early → strong (already escaping chop).
    #[serde(default = "default_time_kill_early_green_pct")]
    pub time_kill_early_green_pct: f64,
    /// Drawdown from session peak (0..1) while still below min profit → weak / distribution.
    #[serde(default = "default_time_kill_peak_dd_weak")]
    pub time_kill_peak_dd_for_weak: f64,
    /// Trailing stop: how far (%) below the peak mcap before we exit
    pub trailing_stop_drawdown_pct: f64,
    /// When true, immediately sell after buy if on-chain post-fill mcap exceeds
    /// score-time mcap by more than [`Self::fill_mcap_abort_max_ratio`].
    #[serde(default = "default_fill_mcap_abort_enabled")]
    pub fill_mcap_abort_enabled: bool,
    /// Max allowed `fill_mcap / score_mcap` (e.g. 1.5 = abort if fill is 50%+ above score).
    #[serde(default = "default_fill_mcap_abort_max_ratio")]
    pub fill_mcap_abort_max_ratio: f64,
}

fn default_fill_mcap_abort_enabled() -> bool {
    true
}

fn default_fill_mcap_abort_max_ratio() -> f64 {
    1.5
}

/// Score-time vs post-fill mcap comparison for spike abort.
struct FillMcapSpike {
    score_mcap_sol: f64,
    fill_mcap_sol: f64,
    ratio: f64,
    max_ratio: f64,
}

impl Default for SmartBuyConfig {
    fn default() -> Self {
        Self {
            amount_sol: 0.1,
            tp1_pct: 30.0,
            tp1_sell_pct: 30.0,
            tp2_pct: 50.0,
            tp2_sell_pct: 15.0,
            tp3_pct: default_tp3_pct(),
            tp3_sell_pct: default_tp3_sell_pct(),
            tp4_pct: default_tp4_pct(),
            tp4_sell_pct: default_tp4_sell_pct(),
            tp5_pct: default_tp5_pct(),
            tp5_sell_pct: default_tp5_sell_pct(),
            exit_profit_floor: -40.0, // lose at most 25%
            time_kill_secs: 25,
            time_kill_min_profit_pct: 10.0,
            time_kill_adaptive: default_time_kill_adaptive(),
            time_kill_weak_secs: default_time_kill_weak_secs(),
            time_kill_strong_secs: default_time_kill_strong_secs(),
            time_kill_neutral_secs: default_time_kill_neutral_secs(),
            time_kill_strong_min_buyers: default_time_kill_strong_min_buyers(),
            time_kill_strong_min_b2s: default_time_kill_strong_min_b2s(),
            time_kill_weak_max_buyers: default_time_kill_weak_max_buyers(),
            time_kill_weak_max_b2s: default_time_kill_weak_max_b2s(),
            time_kill_vel_strong: default_time_kill_vel_strong(),
            time_kill_vel_flat: default_time_kill_vel_flat(),
            time_kill_early_green_pct: default_time_kill_early_green_pct(),
            time_kill_peak_dd_for_weak: default_time_kill_peak_dd_weak(),
            trailing_stop_drawdown_pct: 26.0, // exit if mcap drops 26% from peak
            fill_mcap_abort_enabled: default_fill_mcap_abort_enabled(),
            fill_mcap_abort_max_ratio: default_fill_mcap_abort_max_ratio(),
        }
    }
}

pub use crate::autobuy::open_reason::OpenReason;

/// HTTP/WS snapshot of one open position (dashboard restore on reconnect).
#[derive(Serialize, Clone, Debug)]
pub struct OpenPositionWire {
    pub address: String,
    pub open_reason: OpenReason,
    pub enter_mcap: f64,
    pub pnl: f64,
    pub holdings: f64,
    pub market_cap: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub v3_tape: Option<V3TapeWire>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_kill_tier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_kill_after_secs: Option<u64>,
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
        /// Scoring-time snapshot for self-learning (optional).
        learning_snapshot: Option<LearningTradeSnapshot>,
    },
    /// Internal: actual buy after delay
    ExecuteBuy {
        mint: solana_address::Address,
        amount_sol: f64,
        open_reason: OpenReason,
        dev_address: Option<Address>,
        early_buyers: Vec<Address>,
        learning_snapshot: Option<LearningTradeSnapshot>,
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
    /// Snapshot of all open positions (dashboard HTTP restore).
    GetOpenPositions {
        responder: tokio::sync::oneshot::Sender<Vec<OpenPositionWire>>,
    },
    /// Operator: drop a position from manager + broker caches without on-chain
    /// sell (stuck ghost / manual unwind elsewhere). Emits `PositionClose` so
    /// dashboards remove the row from OPEN.
    AbandonPosition {
        mint: solana_address::Address,
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
    learning: Option<LearningLogPg>,
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
        learning: Option<LearningLogPg>,
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
            learning,
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
                    learning_snapshot,
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
                            if let Some(ref log) = self.learning {
                                let log = log.clone();
                                let mint_s = mint.to_string();
                                let dev_s = dev_address.map(|d| d.to_string());
                                let r = format!("{block:?}");
                                let payload = json!({ "tier_sol": amount_sol });
                                tokio::spawn(async move {
                                    let _ = log
                                        .log_skipped(
                                            &mint_s,
                                            dev_s.as_deref(),
                                            "strategy_gate",
                                            &r,
                                            payload,
                                            now_secs(),
                                        )
                                        .await;
                                });
                            }
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
                                learning_snapshot,
                            })
                            .await;
                    });
                }

                // ----------------------------------------------------------------
                PositionMessage::GetStrategySnapshot { responder } => {
                    let _ = responder.send(self.strategy.snapshot());
                }

                // ----------------------------------------------------------------
                PositionMessage::AbandonPosition { mint } => {
                    let had = self.positions.remove(&mint).is_some();
                    self.pool_cache.remove(&mint);
                    self.broker.forget_position(mint);
                    let _ = self.event_tx.try_send(WsFeedMessage::PositionClose {
                        address: mint.to_string(),
                        reason: "abandoned (operator removed from OPEN)".into(),
                    });
                    eprintln!(
                        "[MANAGER] AbandonPosition {mint}: had_open={had}, pool_cache cleared, \
                         broker tracking cleared"
                    );
                }

                // ----------------------------------------------------------------
                PositionMessage::ExecuteBuy {
                    mint,
                    amount_sol,
                    open_reason,
                    dev_address,
                    early_buyers,
                    learning_snapshot,
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

                    let v3_wire = learning_snapshot.as_ref().map(V3TapeWire::from_learning);

                    let receipt = match self.broker.buy(mint, amount_sol, latest_pool.as_ref()).await {
                        Ok(r) => {
                            let _ = self.event_tx.try_send(WsFeedMessage::TxEvent {
                                kind: TxEventKind::Buy,
                                mint: mint.to_string(),
                                signature: r.signature.clone(),
                                amount_sol: r.sol_spent,
                                amount_sol_estimated: None,
                                status: "sent".into(),
                                reason: None,
                                mode: self.broker.mode_label().to_string(),
                                ts: now_secs(),
                                v3_tape: v3_wire.clone(),
                                time_kill_detail: None,
                            });
                            r
                        }
                        Err(e) => {
                            eprintln!("[BUY] Failed {mint}: {e}");
                            let _ = self.event_tx.try_send(WsFeedMessage::TxEvent {
                                kind: TxEventKind::Buy,
                                mint: mint.to_string(),
                                signature: None,
                                amount_sol,
                                amount_sol_estimated: None,
                                status: "failed".into(),
                                reason: Some(e.to_string()),
                                mode: self.broker.mode_label().to_string(),
                                ts: now_secs(),
                                v3_tape: v3_wire.clone(),
                                time_kill_detail: None,
                            });
                            continue;
                        }
                    };

                    let current_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();

                    if let Some(spike) = Self::detect_fill_mcap_spike(
                        &self.config,
                        learning_snapshot.as_ref(),
                        &receipt,
                    ) {
                        if self
                            .abort_buy_after_fill_mcap_spike(
                                mint,
                                amount_sol,
                                &receipt,
                                latest_pool.as_ref(),
                                &spike,
                                learning_snapshot.as_ref(),
                                dev_address,
                                current_time,
                            )
                            .await
                        {
                            continue;
                        }
                        eprintln!(
                            "[BUY ABORT] {mint}: fill mcap spike detected but emergency \
                             sell failed — opening position anyway (manual review advised)"
                        );
                    }

                    let tokens = Amount::from_float_native(receipt.tokens_received);
                    let mut position = Position::new(
                        latest_pool,
                        tokens,
                        current_time,
                        receipt.entry_mcap_fill_sol,
                    );
                    position.exit_profit_floor = self.config.exit_profit_floor;
                    position.spent_sol = amount_sol;
                    position.dev_address = dev_address;
                    position.early_buyers = early_buyers;
                    let (tk_b, tk_s, tk_b2s) = learning_snapshot
                        .as_ref()
                        .map(|s| (s.buyer_count, s.smart_wallet_count, s.buy_to_sell_ratio))
                        .unwrap_or_else(|| {
                            let eb = position.early_buyers.len() as u64;
                            (eb, 0u32, 0.0f64)
                        });
                    position.tk_entry_buyers = tk_b;
                    position.tk_entry_smart = tk_s;
                    position.tk_entry_b2s = tk_b2s;
                    position.learning_snapshot = learning_snapshot;
                    position.open_reason = Some(open_reason.clone());
                    let enter_mcap = position.enter_mcap.to_float();
                    let mcap_src = if receipt.entry_mcap_fill_sol.is_some() {
                        "on-chain fill"
                    } else {
                        "WS pool cache"
                    };
                    eprintln!(
                        "[BUY] Opened {mint} | mcap={enter_mcap:.1} SOL ({mcap_src}) | spent={amount_sol:.4} SOL"
                    );
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
                        v3_tape: v3_wire,
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
                        // 100% sell = position close. Tell the broker to
                        // also tear down the on-chain ATA (atomic
                        // CloseAccount in the same tx) so rent-exempt SOL
                        // (~0.00203928 SOL/account) is refunded immediately
                        // instead of being permanently locked.
                        let close_ata = percent >= 100.0;

                        let time_kill_detail = if reason == "TIME KILL" {
                            if self.config.time_kill_adaptive && !pos.last_time_kill_tier.is_empty() {
                                Some(format!(
                                    "adaptive · {} · {}s",
                                    pos.last_time_kill_tier, pos.last_time_kill_after_secs
                                ))
                            } else if !self.config.time_kill_adaptive {
                                Some(format!("fixed · {}s", self.config.time_kill_secs))
                            } else {
                                Some("adaptive · ?".to_string())
                            }
                        } else {
                            None
                        };

                        let (return_value, sell_estimated_for_log) = match self
                            .broker
                            .sell(mint, sell_qty, pos.pool.as_ref(), close_ata)
                            .await
                        {
                            Ok(r) => {
                                let _ = self.event_tx.try_send(WsFeedMessage::TxEvent {
                                    kind: TxEventKind::Sell,
                                    mint: mint.to_string(),
                                    signature: r.signature.clone(),
                                    amount_sol: r.sol_received_actual,
                                    amount_sol_estimated: Some(r.sol_received_estimated),
                                    status: "confirmed".into(),
                                    reason: Some(reason.clone()),
                                    mode: self.broker.mode_label().to_string(),
                                    ts: now_secs(),
                                    v3_tape: None,
                                    time_kill_detail: time_kill_detail.clone(),
                                });
                                (r.sol_received_actual, r.sol_received_estimated)
                            }
                            Err(e) => {
                                eprintln!("[SELL] Broker error for {mint}: {e}");
                                let _ = self.event_tx.try_send(WsFeedMessage::TxEvent {
                                    kind: TxEventKind::Sell,
                                    mint: mint.to_string(),
                                    signature: None,
                                    amount_sol: 0.0,
                                    amount_sol_estimated: None,
                                    status: "failed".into(),
                                    reason: Some(format!("{}: {}", reason, e)),
                                    mode: self.broker.mode_label().to_string(),
                                    ts: now_secs(),
                                    v3_tape: None,
                                    time_kill_detail,
                                });
                                self.positions.insert(mint, pos);
                                continue;
                            }
                        };

                        println!(
                            "[SELL] {mint} | reason={reason} | pnl={actual_pnl:+.2}% \
                             | sold={percent:.0}% of holdings | returned={return_value:.4} SOL \
                             (est {sell_estimated_for_log:.4} SOL)"
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
                        let close_learning_snapshot = pos.learning_snapshot.clone();
                        let close_entry_time = pos.entry_time;

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

                            if let (Some(log), Some(snap)) =
                                (self.learning.as_ref(), close_learning_snapshot.as_ref())
                            {
                                let log = log.clone();
                                let snap = snap.clone();
                                let ex_mc = exit_mcap_sol;
                                let pnl_p = pnl_pct_sol;
                                let rsn = reason.clone();
                                let closed_at_ts = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs() as i64;
                                let held_secs = (closed_at_ts - close_entry_time as i64).max(0);
                                tokio::spawn(async move {
                                    if let Err(e) = log
                                        .log_closed_trade(&snap, ex_mc, pnl_p, held_secs, &rsn, closed_at_ts)
                                        .await
                                    {
                                        eprintln!("[LEARNING] log_closed_trade: {e}");
                                    }
                                });
                            }

                            let _ = self.event_tx.try_send(WsFeedMessage::PositionClose {
                                address: mint.to_string(),
                                reason: reason.clone(),
                            });

                            // Historical bug: the row used to report
                            // `invested_sol = initial_holdings.to_float()`,
                            // which is in TOKEN units, and `realized_pnl_pct`
                            // computed against that — both junk numbers (HUGE
                            // `Invested $` and a sticky `-100%` whenever
                            // `total_returned` came out at 0). We already have
                            // the right SOL-side values just above, so write
                            // those into Postgres.
                            //
                            // If `spent_sol` is somehow zero (only possible
                            // for legacy MockBroker rows), fall back to a
                            // mcap-based pct so the row at least reflects the
                            // actual price move instead of -100%.
                            let invested_sol_row = if close_spent_sol > 0.0 {
                                close_spent_sol
                            } else {
                                0.0
                            };
                            let realized_pnl_pct_row = if close_spent_sol > 0.0 {
                                pnl_pct_sol
                            } else if entry_mcap_sol > 0.0 {
                                (exit_mcap_sol / entry_mcap_sol - 1.0) * 100.0
                            } else {
                                0.0
                            };

                            let entry = BotTradeEntry {
                                mint: mint.to_string(),
                                entry_mcap_sol,
                                invested_sol: invested_sol_row,
                                realized_pnl_pct: realized_pnl_pct_row,
                                close_reason: reason,
                                closed_at: SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs() as i64,
                                exit_mcap_sol,
                                entry_meta: entry_meta_json(close_learning_snapshot.as_ref()),
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

                PositionMessage::GetOpenPositions { responder } => {
                    let snapshot: Vec<OpenPositionWire> = self
                        .positions
                        .iter()
                        .filter_map(|(mint, pos)| {
                            let open_reason = pos.open_reason.clone()?;
                            let enter_mcap = pos.enter_mcap.to_float();
                            let market_cap = pos.pool.market_cap().amount().to_float();
                            Some(OpenPositionWire {
                                address: mint.to_string(),
                                open_reason,
                                enter_mcap,
                                pnl: pos.pnl(),
                                holdings: pos.holdings.to_float(),
                                market_cap,
                                v3_tape: pos
                                    .learning_snapshot
                                    .as_ref()
                                    .map(V3TapeWire::from_learning),
                                time_kill_tier: if pos.last_time_kill_tier.is_empty() {
                                    None
                                } else {
                                    Some(pos.last_time_kill_tier.clone())
                                },
                                time_kill_after_secs: if pos.last_time_kill_after_secs == 0 {
                                    None
                                } else {
                                    Some(pos.last_time_kill_after_secs)
                                },
                            })
                        })
                        .collect();
                    let _ = responder.send(snapshot);
                }
            }
        }
    }

    /// Post-fill bonding-curve mcap vs score-time mcap (`LearningTradeSnapshot::entry_mcap_sol`).
    fn detect_fill_mcap_spike(
        cfg: &SmartBuyConfig,
        snapshot: Option<&LearningTradeSnapshot>,
        receipt: &BuyReceipt,
    ) -> Option<FillMcapSpike> {
        if !cfg.fill_mcap_abort_enabled {
            return None;
        }
        let snap = snapshot?;
        let score_mcap = snap.entry_mcap_sol;
        if score_mcap <= 0.0 {
            return None;
        }
        // Require RPC post-fill mcap so we compare apples-to-apples with the spike
        // during our own buy (not a stale WS tick).
        let fill_mcap = receipt.entry_mcap_fill_sol.filter(|m| *m > 0.0)?;
        let ratio = fill_mcap / score_mcap;
        let max_ratio = cfg.fill_mcap_abort_max_ratio.max(1.0);
        if ratio > max_ratio {
            Some(FillMcapSpike {
                score_mcap_sol: score_mcap,
                fill_mcap_sol: fill_mcap,
                ratio,
                max_ratio,
            })
        } else {
            None
        }
    }

    /// Immediate 100% sell after a fill-time mcap spike. Returns `true` if the
    /// position was **not** opened (abort succeeded or we gave up after sell).
    async fn abort_buy_after_fill_mcap_spike(
        &mut self,
        mint: Address,
        amount_sol: f64,
        receipt: &BuyReceipt,
        pool: &dyn Pool,
        spike: &FillMcapSpike,
        learning_snapshot: Option<&LearningTradeSnapshot>,
        dev_address: Option<Address>,
        now: u64,
    ) -> bool {
        let reason = format!(
            "FILL MCAP SPIKE ABORT ({:.2}x > {:.2}x: {:.1}→{:.1} SOL)",
            spike.ratio,
            spike.max_ratio,
            spike.score_mcap_sol,
            spike.fill_mcap_sol,
        );

        eprintln!("[BUY ABORT] {mint}: {reason}");

        if let Some(log) = self.learning.as_ref() {
            let log = log.clone();
            let mint_s = mint.to_string();
            let dev_s = dev_address.map(|d| d.to_string());
            let payload = json!({
                "score_mcap_sol": spike.score_mcap_sol,
                "fill_mcap_sol": spike.fill_mcap_sol,
                "ratio": spike.ratio,
                "max_ratio": spike.max_ratio,
                "spent_sol": amount_sol,
            });
            let stage = "post_buy";
            let reason_short = "fill_mcap_spike";
            tokio::spawn(async move {
                if let Err(e) = log
                    .log_skipped(&mint_s, dev_s.as_deref(), stage, reason_short, payload, now as i64)
                    .await
                {
                    eprintln!("[LEARNING] log_skipped fill_mcap_spike: {e}");
                }
            });
        }

        let sell_qty = receipt.tokens_received;
        match self
            .broker
            .sell(mint, sell_qty, pool, true)
            .await
        {
            Ok(r) => {
                let _ = self.event_tx.try_send(WsFeedMessage::TxEvent {
                    kind: TxEventKind::Sell,
                    mint: mint.to_string(),
                    signature: r.signature.clone(),
                    amount_sol: r.sol_received_actual,
                    amount_sol_estimated: Some(r.sol_received_estimated),
                    status: "confirmed".into(),
                    reason: Some(reason.clone()),
                    mode: self.broker.mode_label().to_string(),
                    ts: now_secs(),
                    v3_tape: None,
                    time_kill_detail: None,
                });

                let pnl_sol = r.sol_received_actual - amount_sol;
                let pnl_pct = if amount_sol > 0.0 {
                    (r.sol_received_actual / amount_sol - 1.0) * 100.0
                } else {
                    0.0
                };
                self.strategy.note_position_opened();
                self.strategy.note_position_closed(pnl_sol);

                if let Ok(bal) = self.broker.balance_sol().await {
                    self.balance_state.store(bal.to_bits(), Ordering::Relaxed);
                    let _ = self.event_tx.try_send(WsFeedMessage::BalanceUpdate { balance: bal });
                }

                let exit_mcap_sol = pool.market_cap().amount().to_float();
                let entry = BotTradeEntry {
                    mint: mint.to_string(),
                    entry_mcap_sol: spike.fill_mcap_sol,
                    invested_sol: amount_sol,
                    realized_pnl_pct: pnl_pct,
                    close_reason: reason,
                    closed_at: now as i64,
                    exit_mcap_sol,
                    entry_meta: entry_meta_json(learning_snapshot),
                };
                let repo = self.bot_trades.clone();
                tokio::spawn(async move {
                    if let Err(e) = repo.save_bot_trade(entry).await {
                        eprintln!("[BOT TRADE] fill spike abort save failed: {e:?}");
                    }
                });

                true
            }
            Err(e) => {
                eprintln!("[BUY ABORT] {mint}: emergency sell failed: {e}");
                false
            }
        }
    }

    /// V3 adaptive time-kill: weak vs strong window from entry snapshot + short mcap tape.
    /// Returns `(kill_after_secs, tier_label)` where `tier_label` is `strong` / `weak` / `neutral`, or `fixed` when adaptive is off.
    fn time_kill_window_profile(
        cfg: &SmartBuyConfig,
        pos: &Position,
        profit: f64,
        current_mcap: f64,
        held_secs: u64,
    ) -> (u64, &'static str) {
        if !cfg.time_kill_adaptive {
            return (cfg.time_kill_secs, "fixed");
        }

        let enter_mcap = pos.enter_mcap.to_float();
        let vel = pos.time_kill_mcap_velocity_pct_per_sec(enter_mcap);

        let mut strong = 0u32;
        let mut weak = 0u32;

        if pos.tk_entry_smart >= 1 {
            strong += 1;
        }
        if pos.tk_entry_buyers >= cfg.time_kill_strong_min_buyers {
            strong += 1;
        }
        if pos.tk_entry_b2s >= cfg.time_kill_strong_min_b2s {
            strong += 1;
        }

        if pos.tk_entry_smart == 0 {
            weak += 1;
        }
        if pos.tk_entry_buyers > 0 && pos.tk_entry_buyers < cfg.time_kill_weak_max_buyers {
            weak += 1;
        }
        if pos.tk_entry_b2s > 0.0 && pos.tk_entry_b2s < cfg.time_kill_weak_max_b2s {
            weak += 1;
        }

        if held_secs >= 6 && vel >= cfg.time_kill_vel_strong {
            strong += 1;
        }
        if held_secs >= 8 && vel.abs() <= cfg.time_kill_vel_flat {
            weak += 1;
        }

        if profit >= cfg.time_kill_early_green_pct {
            strong += 1;
        }

        let peak_dd = if pos.highest_mcap > 0.0 {
            (pos.highest_mcap - current_mcap) / pos.highest_mcap
        } else {
            0.0
        };
        if peak_dd >= cfg.time_kill_peak_dd_for_weak && profit < cfg.time_kill_min_profit_pct {
            weak += 1;
        }

        if strong >= 4 && weak <= 1 {
            (cfg.time_kill_strong_secs.clamp(45, 70), "strong")
        } else if weak >= 3 && strong <= 1 {
            (cfg.time_kill_weak_secs.clamp(18, 28), "weak")
        } else {
            (cfg.time_kill_neutral_secs.clamp(20, 45), "neutral")
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

            pos.time_kill_note_mcap_sample(current_time, current_mcap);

            let profit = pos.pnl();

            let held_secs = current_time.saturating_sub(pos.entry_time);
            let (kill_after_secs, tk_tier) =
                Self::time_kill_window_profile(&self.config, pos, profit, current_mcap, held_secs);
            pos.last_time_kill_tier = tk_tier.to_string();
            pos.last_time_kill_after_secs = kill_after_secs;

            let _ = self.event_tx.try_send(WsFeedMessage::PositionUpdate {
                address: mint.to_string(),
                pnl: profit,
                holdings: pos.holdings.to_float(),
                market_cap: current_mcap,
                time_kill_tier: Some(pos.last_time_kill_tier.clone()),
                time_kill_after_secs: Some(pos.last_time_kill_after_secs),
            });

            // --- 1. Time Kill (adaptive window: weak 20–25s, strong 45–70s) ---
            if current_time >= pos.entry_time + kill_after_secs
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
                continue;
            }

            if profit >= self.config.tp3_pct && !pos.tp3_triggered {
                pos.tp3_triggered = true;
                pos.pending_partial_sell = true;
                actions.push((*mint, self.config.tp3_sell_pct, "TP3 +100%".to_string()));
                continue;
            }

            if profit >= self.config.tp4_pct && !pos.tp4_triggered {
                pos.tp4_triggered = true;
                pos.pending_partial_sell = true;
                actions.push((*mint, self.config.tp4_sell_pct, "TP4 +150%".to_string()));
                continue;
            }

            if profit >= self.config.tp5_pct && !pos.tp5_triggered {
                pos.tp5_triggered = true;
                pos.pending_partial_sell = true;
                actions.push((*mint, self.config.tp5_sell_pct, "TP5 +200%".to_string()));
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
            if pos.tp3_triggered {
                status.push_str(" [TP3]");
            }
            if pos.tp4_triggered {
                status.push_str(" [TP4]");
            }
            if pos.tp5_triggered {
                status.push_str(" [TP5]");
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
        learning_snapshot: Option<LearningTradeSnapshot>,
    ) {
        let _ = self
            .tx
            .send(PositionMessage::InitiateBuy {
                pool,
                amount_sol,
                open_reason,
                dev_address,
                early_buyers,
                learning_snapshot,
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

    /// All open positions (for dashboard HTTP restore after WS reconnect).
    pub async fn get_open_positions(&self) -> Vec<OpenPositionWire> {
        let (resp_tx, resp_rx) = oneshot::channel();
        let _ = self
            .tx
            .send(PositionMessage::GetOpenPositions {
                responder: resp_tx,
            })
            .await;
        resp_rx.await.unwrap_or_default()
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
        #[serde(skip_serializing_if = "Option::is_none")]
        v3_tape: Option<V3TapeWire>,
    },
    PositionUpdate {
        address: String,
        pnl: f64,
        holdings: f64,
        market_cap: f64,
        #[serde(skip_serializing_if = "Option::is_none")]
        time_kill_tier: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        time_kill_after_secs: Option<u64>,
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
    /// Transaction event surfaced to UIs. `signature` is None for the demo
    /// broker (simulated) and Some(base58) for live on-chain tx. `status` is
    /// one of: "sent", "confirmed", "failed", "rejected".
    TxEvent {
        kind: TxEventKind,
        mint: String,
        signature: Option<String>,
        /// For sells: on-chain wallet lamport delta / 1e9 (fees, rent refund included).
        amount_sol: f64,
        /// For sells: bonding-curve / Jupiter estimate before execution (`None` for buys).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        amount_sol_estimated: Option<f64>,
        status: String,
        reason: Option<String>,
        mode: String,
        ts: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        v3_tape: Option<V3TapeWire>,
        #[serde(skip_serializing_if = "Option::is_none")]
        time_kill_detail: Option<String>,
    },
}

#[derive(Serialize, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum TxEventKind {
    Buy,
    Sell,
}
