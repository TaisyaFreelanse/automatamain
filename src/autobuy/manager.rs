use crate::{
    autobuy::{
        broker::{Broker, BrokerError, BuyReceipt},
        exit_engine::{
            adaptive_moonbag_sell_pct, adaptive_trailing, apply_phase_to_tp,
            calculate_live_score, entry_live_score_floor, format_sl_close_reason,
            in_exit_grace_period, in_sl_grace_period, sl_crash_triggered, sl_raw_tick_drop_pct,
            sl_trigger_pnl_pct,
            live_metrics_from_snapshot, live_metrics_lite, maybe_upgrade_runner,
            momentum_decay_detected, profit_lock_staircase_floor, recovery_score,
            resolve_entry_profile, transition_position_phase, ExitEngineV4Config, ExitProfile,
            PositionPhase, TkEntryThresholds,
        },
        positions::Position,
        curve_quarantine::{self, CurveQuarantineCache},
        dev_blacklist,
        filters::config::{CurveQuarantineConfig, DevBlacklistConfig},
    },
    generalize::general_pool::Pool,
    helper::Amount,
    launchpads::token_bucket::TokenBucket,
    learning::{LearningLogPg, LearningTradeSnapshot},
    persistence::{
        bot_trade_post_exit::BotTradePostExitRepository,
        bot_trades::{BotTradeEntry, BotTradeRepository},
        dev_blacklist::{DevBlacklistEntry, DevBlacklistRepository},
    },
    scoring::{
        config::StrategyConfig,
        dev_ranker::{DevRankerHandle, TokenOutcome},
        live_position::snapshot_live_position,
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

/// RPC never showed the mint account (or still missing after wait) — skip quietly.
fn is_buy_mint_unavailable(err: &BrokerError) -> bool {
    if err.is_mint_not_on_chain() {
        return true;
    }
    let msg = err.to_string().to_lowercase();
    msg.contains("mint account not found")
        || (msg.contains("buy mint") && msg.contains("accountnotfound"))
        || msg.contains("not visible on rpc")
}

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
    /// Abort when `fill_mcap / score_mcap` is below this (adverse fill vs score tape).
    #[serde(default = "default_fill_mcap_abort_min_ratio")]
    pub fill_mcap_abort_min_ratio: f64,
    /// When true, seed the position entry/SL baseline with `min(post-fill mcap,
    /// score-time mcap)` instead of the raw post-fill bonding-curve mcap. The
    /// post-fill mcap is inflated by our own buy's price impact (and any buys
    /// that piled in during our fill), so using it as the SL reference makes the
    /// stop fire on our own slippage rather than real market downside. The
    /// score-time mcap is a clean pre-impact reference. `false` keeps the raw
    /// post-fill mcap.
    #[serde(default = "default_honest_entry_baseline")]
    pub honest_entry_baseline: bool,
    /// SL: consecutive ticks (100ms) with filtered PnL <= floor before full exit.
    #[serde(default = "default_sl_confirm_ticks")]
    pub sl_confirm_ticks: u32,
    /// SL: no stop-loss until held this many seconds after entry (decay grace is separate).
    #[serde(default = "default_sl_grace_secs")]
    pub sl_grace_secs: u64,
    /// Emergency SL: instant exit when pessimistic PnL <= this (bypasses grace + confirm).
    #[serde(default = "default_sl_crash_pnl_pct")]
    pub sl_crash_pnl_pct: f64,
    /// Emergency SL: instant exit when raw mcap drops this % vs previous tick (bypasses grace).
    #[serde(default = "default_sl_crash_tick_drop_pct")]
    pub sl_crash_tick_drop_pct: f64,
    /// Ring buffer length for exit mcap median filter (~500ms at 100ms ticks when 5).
    #[serde(default = "default_exit_mcap_median_ticks")]
    pub exit_mcap_median_ticks: usize,
    /// Outlier band vs tape median (low/high multipliers, chart-style).
    #[serde(default = "default_exit_mcap_band_low_ratio")]
    pub exit_mcap_band_low_ratio: f64,
    #[serde(default = "default_exit_mcap_band_high_ratio")]
    pub exit_mcap_band_high_ratio: f64,
    /// Bonding-curve mcap (SOL) at which we lock most of the position (partial sell + moonbag).
    #[serde(default = "default_mcap_ceiling_sol")]
    pub mcap_ceiling_sol: f64,
    /// % of current holdings to sell on first mcap ceiling hit (remainder = moonbag).
    #[serde(default = "default_mcap_ceiling_partial_sell_pct")]
    pub mcap_ceiling_partial_sell_pct: f64,
    /// Adaptive Exit Engine V4 (profiles, live score, hold/runner, adaptive trailing).
    #[serde(default)]
    pub exit_v4: ExitEngineV4Config,
}

fn default_sl_confirm_ticks() -> u32 {
    3
}

fn default_sl_grace_secs() -> u64 {
    5
}

fn default_sl_crash_pnl_pct() -> f64 {
    -28.0
}

fn default_sl_crash_tick_drop_pct() -> f64 {
    18.0
}

fn default_exit_mcap_median_ticks() -> usize {
    5
}

fn default_exit_mcap_band_low_ratio() -> f64 {
    0.02
}

fn default_exit_mcap_band_high_ratio() -> f64 {
    50.0
}

fn default_mcap_ceiling_sol() -> f64 {
    350.0
}

fn default_mcap_ceiling_partial_sell_pct() -> f64 {
    65.0
}

fn default_fill_mcap_abort_enabled() -> bool {
    true
}

fn default_fill_mcap_abort_max_ratio() -> f64 {
    1.5
}

fn default_fill_mcap_abort_min_ratio() -> f64 {
    0.78
}

fn default_honest_entry_baseline() -> bool {
    true
}

/// Score-time vs post-fill mcap comparison for spike abort.
struct FillMcapSpike {
    score_mcap_sol: f64,
    fill_mcap_sol: f64,
    ratio: f64,
    max_ratio: f64,
    min_ratio: f64,
    adverse: bool,
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
            fill_mcap_abort_min_ratio: default_fill_mcap_abort_min_ratio(),
            sl_confirm_ticks: default_sl_confirm_ticks(),
            sl_grace_secs: default_sl_grace_secs(),
            sl_crash_pnl_pct: default_sl_crash_pnl_pct(),
            sl_crash_tick_drop_pct: default_sl_crash_tick_drop_pct(),
            exit_mcap_median_ticks: default_exit_mcap_median_ticks(),
            exit_mcap_band_low_ratio: default_exit_mcap_band_low_ratio(),
            exit_mcap_band_high_ratio: default_exit_mcap_band_high_ratio(),
            mcap_ceiling_sol: default_mcap_ceiling_sol(),
            mcap_ceiling_partial_sell_pct: default_mcap_ceiling_partial_sell_pct(),
            exit_v4: ExitEngineV4Config::default(),
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
    /// Full token bucket (pool + swarm) for live exit metrics.
    UpdateTokenBucket(TokenBucket),
    /// Async live tape refresh result (from `UpdateTokenBucket` / `Tick`).
    ApplyLiveSnapshot {
        mint: solana_address::Address,
        live: crate::scoring::live_position::LivePositionSnapshot,
    },
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
    /// Background Jupiter/bonding mcap poll for graduated moonbag (exit + dashboard).
    ApplyExitMcapRefresh {
        mint: solana_address::Address,
        mcap: f64,
        use_jupiter: bool,
    },
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
    /// Latest bonding-curve bucket per mint (live score / tape).
    bucket_cache: HashMap<solana_address::Address, TokenBucket>,
    tx: mpsc::Sender<PositionMessage>,
    rx: mpsc::Receiver<PositionMessage>,
    last_print_time: u64,
    event_tx: mpsc::Sender<WsFeedMessage>,
    bot_trades: Arc<dyn BotTradeRepository + Send + Sync>,
    dev_blacklist: Arc<dyn DevBlacklistRepository + Send + Sync>,
    dev_blacklist_cfg: DevBlacklistConfig,
    curve_quarantine_cfg: CurveQuarantineConfig,
    curve_quarantine: CurveQuarantineCache,
    post_exit_repo: Arc<dyn BotTradePostExitRepository + Send + Sync>,
    post_exit_rpc: Option<Arc<solana_client::nonblocking::rpc_client::RpcClient>>,
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
        dev_blacklist: Arc<dyn DevBlacklistRepository + Send + Sync>,
        dev_blacklist_cfg: DevBlacklistConfig,
        curve_quarantine_cfg: CurveQuarantineConfig,
        post_exit_repo: Arc<dyn BotTradePostExitRepository + Send + Sync>,
        post_exit_rpc: Option<Arc<solana_client::nonblocking::rpc_client::RpcClient>>,
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
            bucket_cache: HashMap::new(),
            tx: tx.clone(),
            rx,
            last_print_time: 0,
            event_tx,
            bot_trades,
            dev_blacklist,
            dev_blacklist_cfg,
            curve_quarantine_cfg,
            curve_quarantine: CurveQuarantineCache::default(),
            post_exit_repo,
            post_exit_rpc,
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

                PositionMessage::UpdateTokenBucket(bucket) => {
                    let mint = bucket.pool().mint();
                    self.pool_cache
                        .insert(mint, bucket.pool().clone_box());
                    self.bucket_cache.insert(mint, bucket.clone());
                    if let Some(pos) = self.positions.get_mut(&mint) {
                        pos.pool = bucket.pool().clone_box();
                    }
                    if self.positions.contains_key(&mint) {
                        self.spawn_live_snapshot_refresh(mint);
                    }
                }

                PositionMessage::ApplyExitMcapRefresh {
                    mint,
                    mcap,
                    use_jupiter,
                } => {
                    if let Some(pos) = self.positions.get_mut(&mint) {
                        let prev_jupiter = pos.use_jupiter_exit_mcap;
                        if use_jupiter {
                            pos.use_jupiter_exit_mcap = true;
                            pos.exit_mcap_jupiter = Some(mcap);
                        } else if !pos.use_jupiter_exit_mcap && mcap > 0.0 {
                            // Bonding still authoritative; no pool overwrite here.
                        }
                        if use_jupiter {
                            let pool_mcap = pos.pool.market_cap().amount().to_float();
                            if !prev_jupiter {
                                eprintln!(
                                    "[EXIT MCAP] {mint}: switched to Jupiter mcap={mcap:.2} SOL \
                                     (pool WS={pool_mcap:.2})"
                                );
                            } else if pos
                                .exit_mcap_jupiter
                                .is_none_or(|prev| (prev - mcap).abs() / prev.max(1.0) > 0.02)
                            {
                                eprintln!(
                                    "[EXIT MCAP] {mint}: jupiter refresh mcap={mcap:.2} SOL \
                                     (pool WS={pool_mcap:.2})"
                                );
                            }
                        }
                    }
                }
                PositionMessage::ApplyLiveSnapshot { mint, live } => {
                    if let Some(pos) = self.positions.get_mut(&mint) {
                        let prev_tape = pos.live_tape_curr.clone();
                        pos.live_tape_prev = prev_tape;
                        pos.live_tape_curr = Some(live.tape.clone());
                        let held = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs()
                            .saturating_sub(pos.entry_time);
                        let enter_mcap = pos.enter_mcap.to_float();
                        let vel = pos.time_kill_mcap_velocity_pct_per_sec(enter_mcap);
                        let profit = pos.pnl();
                        let entry_vel = pos
                            .learning_snapshot
                            .as_ref()
                            .map(|s| s.velocity_pct)
                            .unwrap_or(0.0);
                        let v4 = &self.config.exit_v4;
                        let metrics = live_metrics_from_snapshot(
                            &live,
                            pos.live_prev_buyers_per_sec,
                            vel,
                            pos.live_prev_velocity,
                            entry_vel,
                            profit,
                            v4,
                        );
                        pos.live_prev_buyers_per_sec = pos.live_buyers_per_sec;
                        pos.live_buyers_per_sec = live.buyers_per_sec;
                        pos.live_prev_velocity = vel;
                        pos.live_score = calculate_live_score(&metrics)
                            .max(pos.live_score_entry_floor);
                        pos.last_live_score_at = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_secs();
                        if !in_exit_grace_period(held, v4) {
                            pos.exit_phase = transition_position_phase(
                                pos.exit_phase,
                                pos.live_score,
                                metrics.momentum_decay,
                                profit,
                                v4,
                            );
                        }
                        let (profile, hold) = maybe_upgrade_runner(
                            v4,
                            pos.exit_profile,
                            &metrics,
                            pos.live_score,
                            pos.hold_mode,
                        );
                        pos.exit_profile = profile;
                        pos.hold_mode = hold;
                        let _ = held;
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

                    let wall_secs = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs();
                    self.curve_quarantine.prune(wall_secs);
                    if self.curve_quarantine_cfg.enabled
                        && self.curve_quarantine.is_active(&mint, wall_secs)
                    {
                        eprintln!(
                            "[FILTER] {mint} skipped: {}",
                            curve_quarantine::format_skip_detail(&mint.to_string())
                        );
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
                                let skip_ts = wall_secs as i64;
                                tokio::spawn(async move {
                                    let _ = log
                                        .log_skipped(
                                            &mint_s,
                                            dev_s.as_deref(),
                                            "strategy_gate",
                                            &r,
                                            payload,
                                            skip_ts,
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
                                pnl_mcap_pct: None,
                                pnl_sol_pct: None,
                            });
                            r
                        }
                        Err(e) if is_buy_mint_unavailable(&e) => {
                            eprintln!(
                                "[BUY] Skipped {mint} — mint not on RPC (stale/dead feed, not a trade): {e}"
                            );
                            self.closed_mints.insert(mint);
                            if let Some(ref log) = self.learning {
                                let log = log.clone();
                                let mint_s = mint.to_string();
                                let dev_s = dev_address.map(|d| d.to_string());
                                let detail = e.to_string();
                                tokio::spawn(async move {
                                    let _ = log
                                        .log_skipped(
                                            &mint_s,
                                            dev_s.as_deref(),
                                            "buy_rpc",
                                            "mint_not_on_chain",
                                            json!({ "detail": detail }),
                                            now_secs(),
                                        )
                                        .await;
                                });
                            }
                            continue;
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
                                pnl_mcap_pct: None,
                                pnl_sol_pct: None,
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
                    // Honest entry baseline: the raw post-fill bonding-curve mcap
                    // is inflated by our own buy's price impact, so using it as the
                    // SL reference makes the stop fire on our own slippage. Prefer
                    // `min(post-fill, score-time)` — the score-time mcap is a clean
                    // pre-impact reference and tracks our true cost basis.
                    let entry_baseline_mcap =
                        receipt.entry_mcap_fill_sol.filter(|m| *m > 0.0).map(|fill| {
                            if self.config.honest_entry_baseline {
                                match learning_snapshot
                                    .as_ref()
                                    .map(|s| s.entry_mcap_sol)
                                    .filter(|m| *m > 0.0)
                                {
                                    Some(score) => fill.min(score),
                                    None => fill,
                                }
                            } else {
                                fill
                            }
                        });
                    let mut position = Position::new(
                        latest_pool,
                        tokens,
                        current_time,
                        entry_baseline_mcap,
                    );
                    if let Some(entry_mcap) = entry_baseline_mcap.filter(|m| *m > 0.0) {
                        position.exit_mcap_ticks.clear();
                        position.push_exit_mcap_tick(
                            entry_mcap,
                            self.config.exit_mcap_median_ticks,
                        );
                        position.sl_prev_raw_mcap = Some(entry_mcap);
                    }
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
                    let snap_ref = learning_snapshot.as_ref();
                    if self.config.exit_v4.enabled {
                        let tk = TkEntryThresholds {
                            strong_min_buyers: self.config.time_kill_strong_min_buyers,
                            strong_min_b2s: self.config.time_kill_strong_min_b2s,
                            weak_max_buyers: self.config.time_kill_weak_max_buyers,
                            weak_max_b2s: self.config.time_kill_weak_max_b2s,
                        };
                        let (profile, hold) = resolve_entry_profile(
                            tk,
                            &self.config.exit_v4,
                            &position,
                            snap_ref,
                        );
                        position.exit_profile = profile;
                        position.hold_mode = hold;
                        position.exit_phase = PositionPhase::Exploration;
                        position.live_score_entry_floor =
                            entry_live_score_floor(learning_snapshot.as_ref());
                        position.live_score = position.live_score_entry_floor;
                        position.last_live_score_at = current_time;
                        eprintln!(
                            "[EXIT V4] {mint} profile={} phase=exploration hold={hold} \
                             live_floor={}",
                            profile.as_str(),
                            position.live_score_entry_floor
                        );
                    }
                    if self.bucket_cache.contains_key(&mint) {
                        self.spawn_live_snapshot_refresh(mint);
                    }
                    position.learning_snapshot = learning_snapshot;
                    position.open_reason = Some(open_reason.clone());
                    let enter_mcap = position.enter_mcap.to_float();
                    position.push_exit_mcap_tick(enter_mcap, self.config.exit_mcap_median_ticks);
                    let impact_adjusted = matches!(
                        (receipt.entry_mcap_fill_sol.filter(|m| *m > 0.0), entry_baseline_mcap),
                        (Some(fill), Some(base)) if base < fill
                    );
                    let mcap_src = if receipt.entry_mcap_fill_sol.is_some() {
                        if impact_adjusted {
                            "on-chain fill, impact-adj"
                        } else {
                            "on-chain fill"
                        }
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
                        let band_lo = self.config.exit_mcap_band_low_ratio;
                        let band_hi = self.config.exit_mcap_band_high_ratio;
                        let filt_pnl_at_sell = pos.pnl_filtered(band_lo, band_hi);
                        let raw_pnl_at_sell = pos.pnl_at_mcap(pos.exit_raw_mcap());
                        let mcap_pnl_pct = pos.pnl();
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

                        let (return_value, sell_estimated_for_log, sell_signature) = match self
                            .broker
                            .sell(mint, sell_qty, pos.pool.as_ref(), close_ata)
                            .await
                        {
                            Ok(r) => (
                                r.sol_received_actual,
                                r.sol_received_estimated,
                                r.signature.clone(),
                            ),
                            Err(e) => {
                                eprintln!("[SELL] Broker error for {mint}: {e}");
                                if e.requires_manual_sell() {
                                    eprintln!(
                                        "[SELL] {mint}: Jupiter routing exhausted — \
                                         clearing is_closing; MANUAL SELL required (holdings {:.4})",
                                        pos.holdings.to_float()
                                    );
                                    pos.is_closing = false;
                                    pos.pending_partial_sell = false;
                                    let _ = self.event_tx.try_send(
                                        WsFeedMessage::ManualSellRequired {
                                            mint: mint.to_string(),
                                            exit_reason: reason.clone(),
                                            detail: e.to_string(),
                                            holdings: pos.holdings.to_float(),
                                            ts: now_secs(),
                                        },
                                    );
                                } else if percent >= 100.0 {
                                    pos.is_closing = false;
                                    pos.pending_partial_sell = false;
                                } else {
                                    pos.pending_partial_sell = false;
                                }
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
                                    pnl_mcap_pct: Some(mcap_pnl_pct),
                                    pnl_sol_pct: None,
                                });
                                self.positions.insert(mint, pos);
                                continue;
                            }
                        };

                        pos.total_returned += return_value;
                        let pnl_sol_pct_tx = if pos.spent_sol > 0.0 && percent >= 100.0 {
                            Some((pos.total_returned / pos.spent_sol - 1.0) * 100.0)
                        } else {
                            None
                        };
                        let _ = self.event_tx.try_send(WsFeedMessage::TxEvent {
                            kind: TxEventKind::Sell,
                            mint: mint.to_string(),
                            signature: sell_signature,
                            amount_sol: return_value,
                            amount_sol_estimated: Some(sell_estimated_for_log),
                            status: "confirmed".into(),
                            reason: Some(reason.clone()),
                            mode: self.broker.mode_label().to_string(),
                            ts: now_secs(),
                            v3_tape: None,
                            time_kill_detail: time_kill_detail.clone(),
                            pnl_mcap_pct: Some(mcap_pnl_pct),
                            pnl_sol_pct: pnl_sol_pct_tx,
                        });
                        let total_returned = pos.total_returned;
                        let spent_sol = pos.spent_sol;
                        if percent >= 100.0 && spent_sol > 0.0 {
                            let sol_pnl_pct = (total_returned / spent_sol - 1.0) * 100.0;
                            println!(
                                "[SELL] {mint} | reason={reason} | pnl_sol={sol_pnl_pct:+.2}% \
                                 pnl_mcap={mcap_pnl_pct:+.2}% | spent={spent_sol:.4} SOL \
                                 | returned={total_returned:.4} SOL (est {sell_estimated_for_log:.4}) \
                                 | sold={percent:.0}%"
                            );
                        } else {
                            println!(
                                "[SELL] {mint} | reason={reason} | pnl_mcap={mcap_pnl_pct:+.2}% \
                                 | sold={percent:.0}% of holdings | leg_returned={return_value:.4} SOL \
                                 (est {sell_estimated_for_log:.4} SOL)"
                            );
                        }

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

                            if curve_quarantine::should_quarantine_mint(
                                &reason,
                                pnl_pct_sol,
                                filt_pnl_at_sell,
                                raw_pnl_at_sell,
                                &self.curve_quarantine_cfg,
                            ) {
                                let closed_at_secs = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_secs();
                                let expires = closed_at_secs
                                    + self.curve_quarantine_cfg.cooldown_secs;
                                self.curve_quarantine.insert(mint, expires);
                                eprintln!(
                                    "[CURVE QUARANTINE] {mint} until +{}s (pnl_sol={pnl_pct_sol:.1}% \
                                     filt_pnl={filt_pnl_at_sell:.1}% raw_pnl={raw_pnl_at_sell:.1}%)",
                                    self.curve_quarantine_cfg.cooldown_secs
                                );
                            }

                            if let Some(dev) = close_dev_address {
                                if dev_blacklist::should_blacklist_dev(
                                    &reason,
                                    pnl_pct_sol,
                                    &self.dev_blacklist_cfg,
                                ) {
                                    let tag = dev_blacklist::cliff_reason_tag(&reason);
                                    let summary_reason =
                                        format!("{tag} {:.0}%", pnl_pct_sol.round());
                                    let closed_at_ts = SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs() as i64;
                                    let entry = DevBlacklistEntry {
                                        dev_wallet: dev.to_string(),
                                        reason: summary_reason,
                                        mint: mint.to_string(),
                                        pnl_sol,
                                        close_reason: reason.clone(),
                                        created_at: closed_at_ts,
                                        expires_at: closed_at_ts
                                            + self.dev_blacklist_cfg.cooldown_secs,
                                    };
                                    let repo = self.dev_blacklist.clone();
                                    tokio::spawn(async move {
                                        if let Err(e) = repo.insert(entry).await {
                                            eprintln!("[DEV BLACKLIST] insert failed: {e}");
                                        }
                                    });
                                }
                            }

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

                            let closed_at_ts = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_secs() as i64;
                            let entry = BotTradeEntry {
                                mint: mint.to_string(),
                                entry_mcap_sol,
                                invested_sol: invested_sol_row,
                                realized_pnl_pct: realized_pnl_pct_row,
                                close_reason: reason,
                                entry_at: close_entry_time as i64,
                                closed_at: closed_at_ts,
                                exit_mcap_sol,
                                entry_meta: entry_meta_json(close_learning_snapshot.as_ref()),
                            };
                            let repo = self.bot_trades.clone();
                            let post_exit_repo = self.post_exit_repo.clone();
                            let post_exit_rpc = self.post_exit_rpc.clone();
                            let mint_str = mint.to_string();
                            tokio::spawn(async move {
                                match repo.save_bot_trade(entry).await {
                                    Ok(trade_id) => {
                                        if let Some(rpc) = post_exit_rpc {
                                            crate::autobuy::post_exit_tracker::spawn_post_exit_tracking(
                                                rpc,
                                                post_exit_repo,
                                                trade_id,
                                                mint_str,
                                                exit_mcap_sol,
                                            );
                                        }
                                    }
                                    Err(e) => eprintln!("[BOT TRADE] save failed: {e:?}"),
                                }
                            });
                        }
                    }
                }

                // ----------------------------------------------------------------
                PositionMessage::Tick => {
                    if self.config.exit_v4.enabled {
                        let mints: Vec<_> = self.positions.keys().copied().collect();
                        for mint in mints {
                            if self.bucket_cache.contains_key(&mint) {
                                self.spawn_live_snapshot_refresh(mint);
                            }
                        }
                    }
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
                            let market_cap = pos.display_market_cap();
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
        let min_ratio = cfg.fill_mcap_abort_min_ratio.clamp(0.1, 1.0);
        if ratio > max_ratio {
            Some(FillMcapSpike {
                score_mcap_sol: score_mcap,
                fill_mcap_sol: fill_mcap,
                ratio,
                max_ratio,
                min_ratio,
                adverse: false,
            })
        } else if ratio < min_ratio {
            Some(FillMcapSpike {
                score_mcap_sol: score_mcap,
                fill_mcap_sol: fill_mcap,
                ratio,
                max_ratio,
                min_ratio,
                adverse: true,
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
        let reason = if spike.adverse {
            format!(
                "FILL MCAP ADVERSE ABORT ({:.2}x < {:.2}x: score {:.1}→fill {:.1} SOL)",
                spike.ratio,
                spike.min_ratio,
                spike.score_mcap_sol,
                spike.fill_mcap_sol,
            )
        } else {
            format!(
                "FILL MCAP SPIKE ABORT ({:.2}x > {:.2}x: {:.1}→{:.1} SOL)",
                spike.ratio,
                spike.max_ratio,
                spike.score_mcap_sol,
                spike.fill_mcap_sol,
            )
        };

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
                let pnl_pct = if amount_sol > 0.0 {
                    (r.sol_received_actual / amount_sol - 1.0) * 100.0
                } else {
                    0.0
                };
                let fill_mcap = spike.fill_mcap_sol;
                let score_mcap = spike.score_mcap_sol;
                let pnl_mcap_abort = if score_mcap > 0.0 {
                    (fill_mcap / score_mcap - 1.0) * 100.0
                } else {
                    0.0
                };
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
                    pnl_mcap_pct: Some(pnl_mcap_abort),
                    pnl_sol_pct: Some(pnl_pct),
                });

                let pnl_sol = r.sol_received_actual - amount_sol;
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
                    entry_at: now as i64,
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

    fn spawn_live_snapshot_refresh(&self, mint: solana_address::Address) {
        let Some(bucket) = self.bucket_cache.get(&mint).cloned() else {
            return;
        };
        let Some(pos) = self.positions.get(&mint) else {
            return;
        };
        let entry_time = pos.entry_time;
        let early_buyers = pos.early_buyers.clone();
        let prev = pos.live_tape_curr.clone();
        let tol = self.config.exit_v4.bundle_similar_tolerance;
        let tx = self.tx.clone();
        let smart_money = self.smart_money.clone();
        tokio::spawn(async move {
            let held = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                .saturating_sub(entry_time);
            let live = snapshot_live_position(
                &bucket,
                prev.as_ref(),
                held,
                smart_money.as_ref(),
                &early_buyers,
                tol,
            )
            .await;
            let _ = tx
                .send(PositionMessage::ApplyLiveSnapshot { mint, live })
                .await;
        });
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

        let v4_enabled = self.config.exit_v4.enabled;
        let v4_cfg = self.config.exit_v4.clone();
        let global_cfg = self.config.clone();

        let mut actions: Vec<(solana_address::Address, f64, String, bool)> = Vec::new();
        let exit_mcap_rpc = self.post_exit_rpc.clone();
        let exit_mcap_tx = self.tx.clone();

        for (mint, pos) in self.positions.iter_mut() {
            // Skip positions that are already queued for a full close.
            if pos.is_closing {
                continue;
            }

            let pool_raw = pos.pool.market_cap().amount().to_float();
            let should_poll_exit_mcap = pos.use_jupiter_exit_mcap
                || pos.mcap_ceiling_triggered
                || pool_raw >= 150.0;
            if should_poll_exit_mcap
                && current_time.saturating_sub(pos.last_exit_mcap_poll_at)
                    >= crate::autobuy::post_exit_tracker::OPEN_EXIT_MCAP_POLL_SECS
                && let Some(rpc) = exit_mcap_rpc.clone()
            {
                let force_jupiter = pos.use_jupiter_exit_mcap || pos.mcap_ceiling_triggered;
                pos.last_exit_mcap_poll_at = current_time;
                let tx = exit_mcap_tx.clone();
                let mint_poll = *mint;
                tokio::spawn(async move {
                    let mint_str = mint_poll.to_string();
                    let (mcap, use_jupiter) =
                        crate::autobuy::post_exit_tracker::resolve_open_exit_mcap(
                            &rpc,
                            &mint_poll,
                            &mint_str,
                            pool_raw,
                            force_jupiter,
                        )
                        .await;
                    let _ = tx
                        .send(PositionMessage::ApplyExitMcapRefresh {
                            mint: mint_poll,
                            mcap,
                            use_jupiter,
                        })
                        .await;
                });
            }

            let raw_mcap = pos.exit_raw_mcap();
            pos.push_exit_mcap_tick(raw_mcap, global_cfg.exit_mcap_median_ticks);
            let band_lo = global_cfg.exit_mcap_band_low_ratio;
            let band_hi = global_cfg.exit_mcap_band_high_ratio;
            let current_mcap = pos.filtered_market_cap(band_lo, band_hi);
            if current_mcap > pos.highest_mcap {
                pos.highest_mcap = current_mcap;
            }

            pos.time_kill_note_mcap_sample(current_time, current_mcap);

            let profit = pos.pnl_filtered(band_lo, band_hi);
            let profit_raw = pos.pnl_at_mcap(raw_mcap);
            let sl_profit = sl_trigger_pnl_pct(profit, profit_raw);
            let tick_drop = pos
                .sl_prev_raw_mcap
                .and_then(|prev| sl_raw_tick_drop_pct(prev, raw_mcap));
            pos.sl_prev_raw_mcap = Some(raw_mcap);
            if profit > pos.peak_profit_pct {
                pos.peak_profit_pct = profit;
            }
            let enter_mcap = pos.enter_mcap.to_float();
            let vel = pos.time_kill_mcap_velocity_pct_per_sec(enter_mcap);

            let held_secs = current_time.saturating_sub(pos.entry_time);
            let in_grace = v4_enabled && in_exit_grace_period(held_secs, &v4_cfg);

            if v4_enabled
                && !in_grace
                && current_time.saturating_sub(pos.last_live_score_at)
                    >= v4_cfg.live_score_refresh_secs
                && pos.live_tape_curr.is_none()
            {
                let snap = pos.learning_snapshot.as_ref();
                let metrics = live_metrics_lite(pos, vel, profit, snap, &v4_cfg);
                pos.live_score =
                    calculate_live_score(&metrics).max(pos.live_score_entry_floor);
                pos.live_prev_velocity = vel;
                pos.last_live_score_at = current_time;
                pos.exit_phase = transition_position_phase(
                    pos.exit_phase,
                    pos.live_score,
                    metrics.momentum_decay,
                    profit,
                    &v4_cfg,
                );
                let (profile, hold) =
                    maybe_upgrade_runner(&v4_cfg, pos.exit_profile, &metrics, pos.live_score, pos.hold_mode);
                pos.exit_profile = profile;
                pos.hold_mode = hold;
            }

            if v4_enabled && v4_cfg.profit_staircase_enabled {
                if let Some(floor) = profit_lock_staircase_floor(pos.peak_profit_pct) {
                    if pos.exit_profit_floor < floor {
                        pos.exit_profit_floor = floor;
                    }
                }
            }

            let (kill_after_secs, tk_tier) =
                Self::time_kill_window_profile(&global_cfg, pos, profit, current_mcap, held_secs);
            pos.last_time_kill_tier = tk_tier.to_string();
            pos.last_time_kill_after_secs = kill_after_secs;

            let tp_cfg = if v4_enabled {
                let base = v4_cfg.profile_params(pos.exit_profile).clone();
                apply_phase_to_tp(base, pos.exit_phase)
            } else {
                crate::autobuy::exit_engine::ExitTpProfile {
                    tp1_pct: global_cfg.tp1_pct,
                    tp1_sell_pct: global_cfg.tp1_sell_pct,
                    tp2_pct: global_cfg.tp2_pct,
                    tp2_sell_pct: global_cfg.tp2_sell_pct,
                    tp3_pct: global_cfg.tp3_pct,
                    tp3_sell_pct: global_cfg.tp3_sell_pct,
                    tp4_pct: global_cfg.tp4_pct,
                    tp4_sell_pct: global_cfg.tp4_sell_pct,
                    tp5_pct: global_cfg.tp5_pct,
                    tp5_sell_pct: global_cfg.tp5_sell_pct,
                    trailing_stop_drawdown_pct: global_cfg.trailing_stop_drawdown_pct,
                    trailing_activate_profit_pct: 80.0,
                    trailing_floor_profit_pct: 35.0,
                    smart_stop_activate_profit_pct: 50.0,
                    smart_stop_floor_profit_pct: 5.0,
                }
            };

            let trailing_drawdown_pct = if v4_enabled {
                adaptive_trailing(pos.exit_profile, pos.live_score, profit)
            } else {
                tp_cfg.trailing_stop_drawdown_pct
            };

            let _ = self.event_tx.try_send(WsFeedMessage::PositionUpdate {
                address: mint.to_string(),
                pnl: profit,
                holdings: pos.holdings.to_float(),
                market_cap: current_mcap,
                time_kill_tier: Some(pos.last_time_kill_tier.clone()),
                time_kill_after_secs: Some(pos.last_time_kill_after_secs),
            });

            // --- 0. Phase 2: momentum decay full exit (multi-tick confirmed) ---
            // doc 6.3/6.4: a single flush must not exit. Require `decay_confirm_ticks`
            // consecutive qualifying ticks, and suppress the exit while a flush is
            // recovering healthily (doc 5.1 / 6.5 recovery score).
            if v4_enabled
                && v4_cfg.momentum_decay_exit_enabled
                && !in_grace
                && pos.live_tape_curr.is_some()
            {
                let sell_p = pos
                    .learning_snapshot
                    .as_ref()
                    .map(|s| s.sell_pressure_score)
                    .unwrap_or(0.0);
                let decay_now = momentum_decay_detected(
                    pos.live_buyers_per_sec,
                    pos.live_prev_buyers_per_sec,
                    sell_p,
                    vel,
                ) || (pos.exit_phase == PositionPhase::Distribution
                    && pos.live_score <= v4_cfg.phase_distribution_max_score);

                // Flush tracking: drawdown from session high opens a flush window
                // and records its bottom for higher-low detection on recovery.
                let drawdown_from_high = if pos.highest_mcap > 0.0 {
                    (pos.highest_mcap - current_mcap) / pos.highest_mcap * 100.0
                } else {
                    0.0
                };
                if drawdown_from_high >= v4_cfg.flush_drop_pct {
                    if !pos.flush_active {
                        pos.flush_active = true;
                        pos.flush_low_mcap = current_mcap;
                    } else if current_mcap < pos.flush_low_mcap {
                        pos.flush_low_mcap = current_mcap;
                    }
                } else if current_mcap >= pos.highest_mcap * 0.99 {
                    // reclaimed the high: flush resolved
                    pos.flush_active = false;
                }

                let snap_ref = pos.learning_snapshot.as_ref();
                let rec_metrics = live_metrics_lite(pos, vel, profit, snap_ref, &v4_cfg);
                pos.recovery_score = recovery_score(
                    &rec_metrics,
                    current_mcap,
                    pos.flush_low_mcap,
                    pos.highest_mcap,
                );
                let healthy_recovery =
                    pos.flush_active && pos.recovery_score >= v4_cfg.recovery_min_score;

                if decay_now && !healthy_recovery {
                    pos.decay_streak = pos.decay_streak.saturating_add(1);
                } else {
                    pos.decay_streak = 0;
                }

                if pos.decay_streak >= v4_cfg.decay_confirm_ticks.max(1) {
                    pos.is_closing = true;
                    let reason = if pos.exit_phase == PositionPhase::Distribution {
                        format!(
                            "MOMENTUM DECAY [{}] ({} ticks, rec={})",
                            pos.exit_phase.as_str(),
                            pos.decay_streak,
                            pos.recovery_score
                        )
                    } else {
                        format!(
                            "MOMENTUM DECAY ({} ticks, rec={})",
                            pos.decay_streak, pos.recovery_score
                        )
                    };
                    actions.push((*mint, 100.0, reason, false));
                    continue;
                }
            }

            // --- 1. Time Kill (adaptive window: weak 20–25s, strong 45–70s) ---
            let mut skip_time_kill = false;
            if v4_enabled
                && (pos.exit_profile == ExitProfile::Strong
                    || pos.exit_profile == ExitProfile::Runner)
                && (pos.tp1_triggered || pos.tp2_triggered)
                && profit >= v4_cfg.strong_time_kill_min_profit_after_tp
            {
                skip_time_kill = true;
            }
            if !skip_time_kill
                && current_time >= pos.entry_time + kill_after_secs
                && profit < global_cfg.time_kill_min_profit_pct
            {
                pos.is_closing = true;
                actions.push((*mint, 100.0, "TIME KILL".to_string(), false));
                continue;
            }

            // --- 2. Stop Loss / Smart Stop ---
            // Pessimistic PnL (min filtered, raw) + N-tick confirm + grace; crash bypasses both.
            let in_sl_grace = in_sl_grace_period(held_secs, global_cfg.sl_grace_secs);
            let sl_confirm = global_cfg.sl_confirm_ticks.max(1);
            let crash = sl_crash_triggered(
                sl_profit,
                profit_raw,
                tick_drop,
                global_cfg.sl_crash_pnl_pct,
                global_cfg.sl_crash_tick_drop_pct,
                in_sl_grace,
            );
            if crash {
                pos.is_closing = true;
                let reason = format_sl_close_reason(
                    true,
                    sl_profit,
                    pos.exit_profit_floor,
                    profit,
                    profit_raw,
                    current_mcap,
                    raw_mcap,
                    None,
                    tick_drop,
                );
                eprintln!("[EXIT] {mint}: {reason}");
                actions.push((*mint, 100.0, reason, true));
                continue;
            }
            if in_sl_grace {
                pos.sl_below_floor_streak = 0;
            } else if sl_profit <= pos.exit_profit_floor {
                pos.sl_below_floor_streak = pos
                    .sl_below_floor_streak
                    .saturating_add(1)
                    .min(255);
            } else {
                pos.sl_below_floor_streak = 0;
            }
            if !in_sl_grace && pos.sl_below_floor_streak >= sl_confirm as u8 {
                pos.is_closing = true;
                let reason = format_sl_close_reason(
                    false,
                    sl_profit,
                    pos.exit_profit_floor,
                    profit,
                    profit_raw,
                    current_mcap,
                    raw_mcap,
                    Some(sl_confirm),
                    tick_drop,
                );
                eprintln!("[EXIT] {mint}: {reason}");
                actions.push((*mint, 100.0, reason, false));
                continue;
            }

            // --- 3. MCAP ceiling: partial lock + moonbag (not full exit) ---
            let ceiling_sol = global_cfg.mcap_ceiling_sol;
            if ceiling_sol > 0.0
                && current_mcap >= ceiling_sol
                && !pos.mcap_ceiling_triggered
                && !pos.pending_partial_sell
            {
                pos.mcap_ceiling_triggered = true;
                pos.use_jupiter_exit_mcap = true;
                let sell_pct = global_cfg
                    .mcap_ceiling_partial_sell_pct
                    .clamp(1.0, 99.0);
                if !pos.trailing_active {
                    pos.trailing_active = true;
                    pos.exit_profit_floor = pos
                        .exit_profit_floor
                        .max(tp_cfg.trailing_floor_profit_pct);
                }
                pos.pending_partial_sell = true;
                actions.push((
                    *mint,
                    sell_pct,
                    format!("MCAP CEILING ({sell_pct:.0}% lock, moonbag)"),
                    false,
                ));
                eprintln!(
                    "[EXIT] {mint}: MCAP CEILING partial sell {sell_pct:.0}% at filt mcap \
                     {current_mcap:.1} SOL (>= {ceiling_sol:.0}); moonbag + trailing"
                );
                continue;
            }

            // --- 4. Trailing Stop (V4: adaptive drawdown %) ---
            if pos.trailing_active {
                let keep_fraction = 1.0 - trailing_drawdown_pct / 100.0;
                let trailing_stop_mcap = pos.highest_mcap * keep_fraction;
                if current_mcap <= trailing_stop_mcap {
                    pos.is_closing = true;
                    let reason = if v4_enabled {
                        format!("TRAILING EXIT ({:.0}% trail)", trailing_drawdown_pct)
                    } else {
                        "TRAILING EXIT".to_string()
                    };
                    actions.push((*mint, 100.0, reason, false));
                    continue;
                }
            }

            // --- Profit-protection level upgrades (profile-aware) ---
            if profit >= tp_cfg.trailing_activate_profit_pct {
                if !pos.trailing_active {
                    pos.trailing_active = true;
                    pos.exit_profit_floor = tp_cfg.trailing_floor_profit_pct;
                }
            } else if profit >= tp_cfg.smart_stop_activate_profit_pct
                && pos.exit_profit_floor < tp_cfg.smart_stop_floor_profit_pct
            {
                pos.exit_profit_floor = tp_cfg.smart_stop_floor_profit_pct;
            }

            // FIX Bug 5: only queue one partial sell at a time.
            // If a partial sell is already in-flight, skip TP checks this tick.
            if pos.pending_partial_sell {
                continue;
            }

            // FIX Bug 1: check TP1 before TP2 so they fire in the correct order.
            // Both cannot be queued simultaneously thanks to the flag above.
            let tp_label = |n: u8, pct: f64| {
                if v4_enabled {
                    format!(
                        "TP{n} ({:.0}%) [{}|{}]",
                        pct,
                        pos.exit_profile.as_str(),
                        pos.exit_phase.as_str()
                    )
                } else {
                    format!("TP{n}")
                }
            };

            let moonbag_sell = |base: f64| {
                if v4_enabled && v4_cfg.adaptive_moonbag_enabled {
                    adaptive_moonbag_sell_pct(base, pos.live_score, pos.exit_phase)
                } else {
                    base
                }
            };

            if profit >= tp_cfg.tp1_pct && !pos.tp1_triggered {
                pos.tp1_triggered = true;
                pos.pending_partial_sell = true;
                actions.push((
                    *mint,
                    tp_cfg.tp1_sell_pct,
                    tp_label(1, tp_cfg.tp1_pct),
                    false,
                ));
                continue;
            }

            if profit >= tp_cfg.tp2_pct && !pos.tp2_triggered {
                pos.tp2_triggered = true;
                pos.pending_partial_sell = true;
                actions.push((
                    *mint,
                    tp_cfg.tp2_sell_pct,
                    tp_label(2, tp_cfg.tp2_pct),
                    false,
                ));
                continue;
            }

            if profit >= tp_cfg.tp3_pct && !pos.tp3_triggered {
                pos.tp3_triggered = true;
                pos.pending_partial_sell = true;
                actions.push((
                    *mint,
                    moonbag_sell(tp_cfg.tp3_sell_pct),
                    tp_label(3, tp_cfg.tp3_pct),
                    false,
                ));
                continue;
            }

            if profit >= tp_cfg.tp4_pct && !pos.tp4_triggered {
                pos.tp4_triggered = true;
                pos.pending_partial_sell = true;
                actions.push((
                    *mint,
                    moonbag_sell(tp_cfg.tp4_sell_pct),
                    tp_label(4, tp_cfg.tp4_pct),
                    false,
                ));
                continue;
            }

            if profit >= tp_cfg.tp5_pct && !pos.tp5_triggered {
                pos.tp5_triggered = true;
                pos.pending_partial_sell = true;
                actions.push((
                    *mint,
                    moonbag_sell(tp_cfg.tp5_sell_pct),
                    tp_label(5, tp_cfg.tp5_pct),
                    false,
                ));
            }
        }

        // Execute collected actions after releasing the mutable borrow.
        for (mint, percent, reason, urgent) in actions {
            self.schedule_sell(mint, percent, reason, urgent);
        }

        if current_time > self.last_print_time {
            // self.print_dashboard(current_time);
            self.last_print_time = current_time;
        }
    }

    /// Queue sell. Emergency SL (`urgent`) skips the 800 ms delay.
    fn schedule_sell(
        &self,
        mint: solana_address::Address,
        percent: f64,
        reason: String,
        urgent: bool,
    ) {
        let tx_clone = self.tx.clone();
        tokio::spawn(async move {
            if !urgent {
                sleep(Duration::from_millis(800)).await;
            }
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
        /// Sell: mcap PnL % at decision time (`None` for buys).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pnl_mcap_pct: Option<f64>,
        /// Sell: position SOL PnL % after this leg (`None` for buys / partial without spent).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pnl_sol_pct: Option<f64>,
    },
    /// Bot could not auto-sell (Jupiter quote/route exhausted). Position stays OPEN.
    ManualSellRequired {
        mint: String,
        exit_reason: String,
        detail: String,
        holdings: f64,
        ts: i64,
    },
}

#[derive(Serialize, Clone, Copy, Debug)]
#[serde(rename_all = "snake_case")]
pub enum TxEventKind {
    Buy,
    Sell,
}
