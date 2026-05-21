// No extra console window when launching the .exe on Windows (debug + release).
#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use eframe::egui;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};

// ── Wire types ────────────────────────────────────────────────────────────────

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "snake_case")]
pub struct Amount {
    pub value: u64,
    pub decimals: u8,
}

impl Amount {
    pub fn to_float(&self) -> f64 {
        self.value as f64 / 10f64.powi(self.decimals as i32)
    }
}

#[derive(Deserialize, Clone, Debug)]
pub enum Currency {
    Native(Amount),
    Dollar(Amount),
}

impl Currency {
    pub fn to_float(&self) -> f64 {
        match self {
            Currency::Native(a) | Currency::Dollar(a) => a.to_float(),
        }
    }

    pub fn format_usd(&self, sol_price: Option<f64>, decimals: usize) -> String {
        match self {
            Currency::Dollar(a) => format!("${:.*}", decimals, a.to_float()),
            Currency::Native(a) => {
                let val = a.to_float();
                if let Some(p) = sol_price {
                    format!("${:.*}", decimals, val * p)
                } else {
                    format!("{:.*} SOL", decimals, val)
                }
            }
        }
    }
}

#[derive(Deserialize, Clone, Debug)]
pub struct DevStats {
    pub median_market_cap: Currency,
    pub trader_pnl_average: f64,
    pub total_holders_average: u64,
    pub average_volume: f64,
    pub median_total_trades: u64,
    pub average_unique_buy_to_sell_ratio: f64,
    pub average_buy_trader_size: Currency,
    pub total_coins: u64,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum OpenReason {
    DevStats(DevStats),
    TraderStats,
}

/// V3 tape snapshot (entry buy / persisted close meta). Mirrors `autobuy::manager::V3TapeWire`.
#[derive(Deserialize, Clone, Debug, Default)]
pub struct V3TapeWire {
    #[serde(default)]
    pub bv_persist: f64,
    #[serde(default)]
    pub sell_press: f64,
    #[serde(default)]
    pub absorb: f64,
    #[serde(default)]
    pub dumps: u32,
    #[serde(default)]
    pub sm_exits: u32,
}

/// Mirrors backend `OpenPositionWire` (`GET /positions`).
#[derive(Deserialize, Clone, Debug)]
pub struct OpenPositionWire {
    pub address: String,
    pub open_reason: OpenReason,
    pub enter_mcap: f64,
    pub pnl: f64,
    pub holdings: f64,
    pub market_cap: f64,
    #[serde(default)]
    pub v3_tape: Option<V3TapeWire>,
    #[serde(default)]
    pub time_kill_tier: Option<String>,
    #[serde(default)]
    pub time_kill_after_secs: Option<u64>,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsMsg {
    PositionOpen {
        address: String,
        open_reason: OpenReason,
        enter_mcap: f64,
        #[serde(default)]
        v3_tape: Option<V3TapeWire>,
    },
    PositionUpdate {
        address: String,
        pnl: f64,
        holdings: f64,
        market_cap: f64,
        #[serde(default)]
        time_kill_tier: Option<String>,
        #[serde(default)]
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
    TxEvent {
        kind: TxEventKind,
        mint: String,
        signature: Option<String>,
        amount_sol: f64,
        #[serde(default)]
        amount_sol_estimated: Option<f64>,
        status: String,
        reason: Option<String>,
        mode: String,
        ts: i64,
        #[serde(default)]
        v3_tape: Option<V3TapeWire>,
        #[serde(default)]
        time_kill_detail: Option<String>,
    },
}

#[derive(Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TxEventKind {
    Buy,
    Sell,
}

#[derive(Clone, Debug)]
pub struct TxLogRow {
    pub kind: TxEventKind,
    pub mint: String,
    pub signature: Option<String>,
    pub amount_sol: f64,
    pub amount_sol_estimated: Option<f64>,
    pub status: String,
    pub reason: Option<String>,
    pub mode: String,
    pub ts: i64,
    pub v3_tape: Option<V3TapeWire>,
    pub time_kill_detail: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct BotTradeRow {
    pub id: i64,
    pub mint: String,
    pub entry_mcap_sol: f64,
    pub invested_sol: f64,
    pub realized_pnl_pct: f64,
    pub close_reason: String,
    pub closed_at: i64,
    pub exit_mcap_sol: f64,
    #[serde(default)]
    pub entry_meta: String,
}

#[derive(Deserialize, Clone)]
struct ChartPoint {
    t: i64,
    mcap: f64,
}

#[derive(Deserialize, Clone)]
struct ChartMarker {
    entry_at: i64,
    closed_at: i64,
    entry_mcap: f64,
    exit_mcap: f64,
    pnl: f64,
    reason: String,
}

#[derive(Deserialize, Clone, Default)]
struct ChartData {
    #[serde(default)]
    t0: i64,
    #[serde(default)]
    points: Vec<ChartPoint>,
    #[serde(default)]
    markers: Vec<ChartMarker>,
}

/// Pump.fun tape mcaps are SOL; reject slot-sized garbage and NaN/inf.
const CHART_MCAP_ABS_MAX: f64 = 200_000.0;
const CHART_PRE_SECS: i64 = 10;
const CHART_POST_SECS: i64 = 120;

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

fn sanitize_chart(mut chart: ChartData) -> ChartData {
    chart.points.retain(|p| chart_mcap_valid(p.mcap));
    chart.points.sort_by_key(|p| p.t);
    let mut deduped: Vec<ChartPoint> = Vec::new();
    for p in chart.points {
        if let Some(last) = deduped.last_mut() {
            if last.t == p.t {
                last.mcap = p.mcap;
                continue;
            }
        }
        deduped.push(p);
    }
    chart.points = deduped;

    let median = chart_tape_median(&chart.points.iter().map(|p| p.mcap).collect::<Vec<_>>());
    chart.points.retain(|p| chart_mcap_matches_tape(p.mcap, median));

    let series: Vec<(i64, f64)> = chart.points.iter().map(|p| (p.t, p.mcap)).collect();
    for m in &mut chart.markers {
        if !chart_mcap_matches_tape(m.entry_mcap, median) {
            let v = mcap_at_time(&series, m.entry_at);
            if chart_mcap_valid(v) {
                m.entry_mcap = v;
            }
        }
        if !chart_mcap_matches_tape(m.exit_mcap, median) {
            let v = mcap_at_time(&series, m.closed_at);
            if chart_mcap_valid(v) {
                m.exit_mcap = v;
            }
        }
    }
    chart.markers.retain(|m| {
        m.closed_at >= m.entry_at
            && m.entry_at >= 0
            && m.closed_at >= 0
            && chart_mcap_matches_tape(m.entry_mcap, median)
            && chart_mcap_matches_tape(m.exit_mcap, median)
    });
    chart_focus_trade_window(&mut chart);
    chart.t0 = 0;
    chart
}

fn chart_trade_window_bounds(markers: &[ChartMarker], points: &[ChartPoint]) -> (i64, i64) {
    if !markers.is_empty() {
        let entry_min = markers.iter().map(|m| m.entry_at).min().unwrap_or(0);
        let exit_max = markers.iter().map(|m| m.closed_at).max().unwrap_or(entry_min);
        return (
            entry_min.saturating_sub(CHART_PRE_SECS),
            exit_max + CHART_POST_SECS,
        );
    }
    if let (Some(lo), Some(hi)) = (points.first().map(|p| p.t), points.last().map(|p| p.t)) {
        let span = hi.saturating_sub(lo);
        if span > 180 {
            return (lo.saturating_sub(CHART_PRE_SECS), lo + 180);
        }
        return (lo.saturating_sub(CHART_PRE_SECS), hi + 60);
    }
    (0, CHART_POST_SECS)
}

/// Clip tape to entry−10s … exit+120s; re-base so BUY = 0s (pre-entry ≈ −10…0).
fn chart_focus_trade_window(chart: &mut ChartData) {
    let (win_lo, win_hi) = chart_trade_window_bounds(&chart.markers, &chart.points);
    chart.points.retain(|p| p.t >= win_lo && p.t <= win_hi);
    let entry_base = chart
        .markers
        .iter()
        .map(|m| m.entry_at)
        .min()
        .unwrap_or(win_lo);
    for p in &mut chart.points {
        p.t = p.t.saturating_sub(entry_base);
    }
    for m in &mut chart.markers {
        m.entry_at = m.entry_at.clamp(win_lo, win_hi).saturating_sub(entry_base);
        m.closed_at = m.closed_at.clamp(win_lo, win_hi).saturating_sub(entry_base);
    }
}

/// Y extent for autoscale: tape points + marker mcaps in plot units (× price_mult).
fn chart_plot_y_bounds(
    points: &[ChartPoint],
    markers: &[ChartMarker],
    price_mult: f64,
    x_entry: f64,
    x_exit: f64,
) -> Option<(f64, f64)> {
    let mut ys: Vec<f64> = points
        .iter()
        .filter(|p| chart_mcap_valid(p.mcap))
        .filter(|p| {
            let x = p.t as f64;
            x >= x_entry - 30.0 && x <= x_exit + 30.0
        })
        .map(|p| p.mcap * price_mult)
        .collect();
    for m in markers {
        if chart_mcap_valid(m.entry_mcap) {
            ys.push(m.entry_mcap * price_mult);
        }
        if chart_mcap_valid(m.exit_mcap) {
            ys.push(m.exit_mcap * price_mult);
        }
    }
    if ys.is_empty() {
        return None;
    }
    let mut lo = f64::INFINITY;
    let mut hi = f64::NEG_INFINITY;
    for y in ys {
        lo = lo.min(y);
        hi = hi.max(y);
    }
    if !lo.is_finite() || !hi.is_finite() || hi <= lo {
        return None;
    }
    Some((lo, hi))
}

/// Linear mcap at chart time `t` (seconds from tape start) from tape points.
fn mcap_at_time(points: &[(i64, f64)], t: i64) -> f64 {
    if points.is_empty() {
        return 0.0;
    }
    if t <= points[0].0 {
        return points[0].1;
    }
    if t >= points.last().unwrap().0 {
        return points.last().unwrap().1;
    }
    for w in points.windows(2) {
        let (t0, m0) = w[0];
        let (t1, m1) = w[1];
        if t >= t0 && t <= t1 {
            if t1 == t0 {
                return m0;
            }
            let f = (t - t0) as f64 / (t1 - t0) as f64;
            return m0 + (m1 - m0) * f;
        }
    }
    points.last().unwrap().1
}

fn chart_x_sec(t: i64, t0: i64) -> f64 {
    (t.saturating_sub(t0)) as f64
}

// ── Commands ──────────────────────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
enum WsCmd {
    SetPaused { paused: bool },
}

#[derive(Deserialize, Clone, Debug)]
pub struct LiveCfg {
    pub slippage_bps: u32,
    pub priority_fee_micro_lamports: u64,
    pub compute_unit_limit: u32,
    pub max_retries: u32,
    pub balance_refresh_secs: u64,
    #[serde(default)]
    pub skip_preflight: bool,
}

#[derive(Deserialize, Clone, Debug)]
pub struct ModeInfo {
    pub mode: String,
    pub wallet: String,
    pub balance_sol: f64,
    pub live: LiveCfg,
}

enum DashCmd {
    Ws(WsCmd),
    FetchDevStats(String),
    FetchChart(String),
    FetchBuySize,
    SetBuySize(f64),
    FetchMode,
    FetchTxLog,
    FetchOpenPositions,
    /// Switch broker mode. `confirm_live` must be true to switch to live;
    /// the dashboard enforces this via a two-step typed confirmation.
    SetMode { mode: String, confirm_live: bool },
}

// ── App events ────────────────────────────────────────────────────────────────

enum AppEvent {
    Connected,
    Disconnected,
    Msg(WsMsg),
    BotTrades(Vec<BotTradeRow>),
    Status {
        paused: bool,
        mode: Option<String>,
    },
    DevStats {
        mint: String,
        stats: Option<DevStats>,
    },
    ChartData {
        mint: String,
        data: Option<ChartData>,
    },
    SolPrice(f64),
    Pubkey(String),
    BuySize(f64),
    BuySizeSetOk,
    BuySizeSetErr(String),
    ModeInfo(ModeInfo),
    TxLog(Vec<TxLogRow>),
    ModeSetOk {
        mode: String,
        restart_required: bool,
    },
    ModeSetErr(String),
    OpenPositions(Vec<OpenPositionWire>),
}

// ── Positions ─────────────────────────────────────────────────────────────────

struct Position {
    address: String,
    open_reason: OpenReason,
    pnl: f64,
    holdings: f64,
    market_cap: f64,
    enter_mcap: f64,
    v3_tape: Option<V3TapeWire>,
    time_kill_tier: Option<String>,
    time_kill_after_secs: Option<u64>,
}

// ── Config panel ──────────────────────────────────────────────────────────────

struct ConfigPanel {
    open: bool,
    buy_size_remote: Option<f64>,
    buy_size_input: String,
    buy_size_status: Option<String>,
    buy_size_saving: bool,

    /// Last known mode info from the backend. None until /mode has answered.
    mode_info: Option<ModeInfo>,
    /// True after the user clicks "Switch to Live" but before they finish the
    /// typed-confirmation. While true, the dashboard renders the confirm UI.
    pending_live_switch: bool,
    /// User-typed confirmation text. Must equal "LIVE" to enable the final
    /// confirm button.
    confirm_input: String,
    /// In-flight indicator for the PUT /mode request.
    mode_saving: bool,
    /// Last status message for the mode block (✓ ok / ✗ err).
    mode_status: Option<String>,
    /// True when the user has changed mode at runtime; surfaces a restart
    /// banner until the user actually restarts the bot.
    restart_required: bool,
}

impl ConfigPanel {
    fn new() -> Self {
        Self {
            open: false,
            buy_size_remote: None,
            buy_size_input: String::new(),
            buy_size_status: None,
            buy_size_saving: false,
            mode_info: None,
            pending_live_switch: false,
            confirm_input: String::new(),
            mode_saving: false,
            mode_status: None,
            restart_required: false,
        }
    }

    fn on_loaded(&mut self, sol: f64) {
        self.buy_size_remote = Some(sol);
        if self.buy_size_input.is_empty() {
            self.buy_size_input = format!("{:.4}", sol);
        }
    }

    fn on_save_ok(&mut self, sol: f64) {
        self.buy_size_saving = false;
        self.buy_size_remote = Some(sol);
        self.buy_size_status = Some("✓ Saved".to_string());
    }

    fn on_save_err(&mut self, msg: String) {
        self.buy_size_saving = false;
        self.buy_size_status = Some(format!("✗ {}", msg));
    }
}

// ── Dashboard ─────────────────────────────────────────────────────────────────

struct Dashboard {
    rx: mpsc::Receiver<AppEvent>,
    cmd_tx: tokio::sync::mpsc::Sender<DashCmd>,
    connected: bool,
    paused: bool,
    balance: Option<f64>,
    open: HashMap<String, Position>,
    history: Vec<BotTradeRow>,
    selected_dev: Option<(String, Option<DevStats>)>,
    loading_dev: Option<String>,
    chart_window: Option<(String, ChartData)>,
    sol_price: Option<f64>,
    pubkey: Option<String>,
    /// Currently active broker mode reported by `/status` (`"demo"` / `"live"`).
    mode: Option<String>,
    /// Bounded log of recent tx events (buy/sell/failed) — live signatures
    /// and demo synthetic entries share the same row format.
    tx_log: std::collections::VecDeque<TxLogRow>,
    /// Last UI tick when we polled `/tx-log` and `/mode` over HTTP. Keeps the
    /// dashboard tolerant of WS gaps without spamming the backend.
    last_http_poll: Option<std::time::Instant>,
    config_panel: ConfigPanel,
    /// Brief "Copied" banner after copying a mint to the clipboard.
    mint_copy_flash_until: Option<Instant>,
}

impl Dashboard {
    fn new(
        cc: &eframe::CreationContext<'_>,
        rx: mpsc::Receiver<AppEvent>,
        cmd_tx: tokio::sync::mpsc::Sender<DashCmd>,
    ) -> Self {
        let mut style = (*cc.egui_ctx.style()).clone();
        style
            .text_styles
            .insert(egui::TextStyle::Body, egui::FontId::proportional(13.0));
        style
            .text_styles
            .insert(egui::TextStyle::Monospace, egui::FontId::monospace(12.0));
        cc.egui_ctx.set_style(style);
        Self {
            rx,
            cmd_tx,
            connected: false,
            paused: false,
            balance: None,
            open: HashMap::new(),
            history: Vec::new(),
            selected_dev: None,
            loading_dev: None,
            chart_window: None,
            sol_price: None,
            pubkey: None,
            mode: None,
            tx_log: std::collections::VecDeque::with_capacity(256),
            last_http_poll: None,
            config_panel: ConfigPanel::new(),
            mint_copy_flash_until: None,
        }
    }

    fn wire_to_position(w: OpenPositionWire) -> Position {
        Position {
            address: w.address,
            open_reason: w.open_reason,
            pnl: w.pnl,
            holdings: w.holdings,
            market_cap: w.market_cap,
            enter_mcap: w.enter_mcap,
            v3_tape: w.v3_tape,
            time_kill_tier: w.time_kill_tier,
            time_kill_after_secs: w.time_kill_after_secs,
        }
    }

    /// Authoritative restore from `GET /positions` (survives GUI restart / WS gaps).
    fn apply_open_positions(&mut self, rows: Vec<OpenPositionWire>) {
        let live: std::collections::HashSet<String> =
            rows.iter().map(|w| w.address.clone()).collect();
        self.open.retain(|k, _| live.contains(k));
        for w in rows {
            self.open
                .insert(w.address.clone(), Self::wire_to_position(w));
        }
    }

    fn drain_events(&mut self) {
        while let Ok(ev) = self.rx.try_recv() {
            match ev {
                AppEvent::Connected => self.connected = true,
                AppEvent::Disconnected => self.connected = false,
                AppEvent::BotTrades(rows) => self.history = rows,
                AppEvent::Status { paused, mode } => {
                    self.paused = paused;
                    if let Some(m) = mode {
                        self.mode = Some(m);
                    }
                }
                AppEvent::ModeInfo(info) => {
                    self.mode = Some(info.mode.clone());
                    self.config_panel.mode_info = Some(info);
                }
                AppEvent::TxLog(rows) => {
                    self.tx_log.clear();
                    for r in rows {
                        if self.tx_log.len() >= 256 {
                            self.tx_log.pop_front();
                        }
                        self.tx_log.push_back(r);
                    }
                }
                AppEvent::ModeSetOk {
                    mode,
                    restart_required,
                } => {
                    self.config_panel.mode_saving = false;
                    self.config_panel.pending_live_switch = false;
                    self.config_panel.confirm_input.clear();
                    self.config_panel.restart_required = restart_required;
                    self.config_panel.mode_status = Some(format!(
                        "✓ saved (mode={})  — {}",
                        mode,
                        if restart_required {
                            "restart bot to activate"
                        } else {
                            "no change"
                        }
                    ));
                }
                AppEvent::ModeSetErr(msg) => {
                    self.config_panel.mode_saving = false;
                    self.config_panel.mode_status = Some(format!("✗ {}", msg));
                }
                AppEvent::SolPrice(price) => self.sol_price = Some(price),
                AppEvent::Pubkey(key) => self.pubkey = Some(key),
                AppEvent::BuySize(sol) => self.config_panel.on_loaded(sol),
                AppEvent::BuySizeSetOk => {
                    let saved = self
                        .config_panel
                        .buy_size_input
                        .parse::<f64>()
                        .unwrap_or(0.0);
                    self.config_panel.on_save_ok(saved);
                }
                AppEvent::BuySizeSetErr(msg) => self.config_panel.on_save_err(msg),
                AppEvent::DevStats { mint, stats } => {
                    if self.loading_dev.as_deref() == Some(&mint) {
                        self.loading_dev = None;
                    }
                    self.selected_dev = Some((mint, stats));
                }
                AppEvent::ChartData { mint, data } => {
                    if let Some(d) = data {
                        self.chart_window = Some((mint, sanitize_chart(d)));
                    }
                }
                AppEvent::OpenPositions(rows) => self.apply_open_positions(rows),
                AppEvent::Msg(msg) => match msg {
                    WsMsg::PositionOpen {
                        address,
                        open_reason,
                        enter_mcap,
                        v3_tape,
                    } => {
                        self.open.insert(
                            address.clone(),
                            Position {
                                address,
                                open_reason,
                                pnl: 0.0,
                                holdings: 0.0,
                                market_cap: 0.0,
                                enter_mcap,
                                v3_tape,
                                time_kill_tier: None,
                                time_kill_after_secs: None,
                            },
                        );
                    }
                    WsMsg::PositionUpdate {
                        address,
                        pnl,
                        holdings,
                        market_cap,
                        time_kill_tier,
                        time_kill_after_secs,
                    } => {
                        if let Some(pos) = self.open.get_mut(&address) {
                            pos.pnl = pnl;
                            pos.holdings = holdings;
                            pos.market_cap = market_cap;
                            if time_kill_tier.is_some() {
                                pos.time_kill_tier = time_kill_tier;
                            }
                            if time_kill_after_secs.is_some() {
                                pos.time_kill_after_secs = time_kill_after_secs;
                            }
                        }
                    }
                    WsMsg::PositionClose { address, .. } => {
                        self.open.remove(&address);
                    }
                    WsMsg::BalanceUpdate { balance } => self.balance = Some(balance),
                    WsMsg::PausedState { paused } => self.paused = paused,
                    WsMsg::TxEvent {
                        kind,
                        mint,
                        signature,
                        amount_sol,
                        amount_sol_estimated,
                        status,
                        reason,
                        mode,
                        ts,
                        v3_tape,
                        time_kill_detail,
                    } => {
                        if self.tx_log.len() >= 256 {
                            self.tx_log.pop_front();
                        }
                        self.tx_log.push_back(TxLogRow {
                            kind,
                            mint,
                            signature,
                            amount_sol,
                            amount_sol_estimated,
                            status,
                            reason,
                            mode,
                            ts,
                            v3_tape,
                            time_kill_detail,
                        });
                    }
                },
            }
        }
    }

    fn usd_val(&self, val_sol: f64, decimals: usize) -> String {
        if let Some(p) = self.sol_price {
            format!("${:.*}", decimals, val_sol * p)
        } else {
            format!("{:.*} SOL", decimals, val_sol)
        }
    }

    fn render_mode_block(&mut self, ui: &mut egui::Ui) {
        egui::Frame::group(ui.style()).show(ui, |ui| {
            ui.set_min_width(320.0);

            // Current mode + summary.
            let current = self
                .config_panel
                .mode_info
                .as_ref()
                .map(|m| m.mode.clone())
                .or_else(|| self.mode.clone())
                .unwrap_or_else(|| "unknown".into());

            let (lbl, col) = match current.as_str() {
                "live" => ("⚠ LIVE", egui::Color32::from_rgb(255, 80, 80)),
                "demo" => ("● DEMO", egui::Color32::from_rgb(120, 220, 120)),
                _ => ("…", egui::Color32::GRAY),
            };

            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("Current:").strong());
                ui.colored_label(col, egui::RichText::new(lbl).strong());
            });

            ui.add_space(6.0);

            // Switch actions. Switching to LIVE always goes through the
            // typed-confirm flow; switching to DEMO is one click since it's
            // always safer.
            if self.config_panel.pending_live_switch {
                ui.label(
                    egui::RichText::new("⚠ Type LIVE to confirm switching to real trading:")
                        .color(egui::Color32::from_rgb(255, 150, 60))
                        .strong(),
                );
                ui.horizontal(|ui| {
                    ui.add(
                        egui::TextEdit::singleline(&mut self.config_panel.confirm_input)
                            .desired_width(120.0)
                            .hint_text("LIVE"),
                    );
                    let armed = self.config_panel.confirm_input == "LIVE"
                        && !self.config_panel.mode_saving;
                    let btn_color = if armed {
                        egui::Color32::from_rgb(255, 80, 80)
                    } else {
                        egui::Color32::GRAY
                    };
                    let label = if self.config_panel.mode_saving {
                        "Switching…"
                    } else {
                        "Confirm LIVE"
                    };
                    if ui
                        .add_enabled(
                            armed,
                            egui::Button::new(egui::RichText::new(label).color(btn_color)),
                        )
                        .clicked()
                    {
                        self.config_panel.mode_saving = true;
                        self.config_panel.mode_status = None;
                        let _ = self.cmd_tx.try_send(DashCmd::SetMode {
                            mode: "live".into(),
                            confirm_live: true,
                        });
                    }
                    if ui.button("Cancel").clicked() {
                        self.config_panel.pending_live_switch = false;
                        self.config_panel.confirm_input.clear();
                    }
                });
            } else {
                ui.horizontal(|ui| {
                    let is_live = current == "live";
                    let is_demo = current == "demo";

                    if ui
                        .add_enabled(
                            !is_demo && !self.config_panel.mode_saving,
                            egui::Button::new(
                                egui::RichText::new("⏼ Switch to DEMO")
                                    .color(egui::Color32::from_rgb(120, 220, 120))
                                    .strong(),
                            ),
                        )
                        .on_hover_text("Safe: simulated trading only")
                        .clicked()
                    {
                        self.config_panel.mode_saving = true;
                        self.config_panel.mode_status = None;
                        let _ = self.cmd_tx.try_send(DashCmd::SetMode {
                            mode: "demo".into(),
                            confirm_live: false,
                        });
                    }
                    if ui
                        .add_enabled(
                            !is_live && !self.config_panel.mode_saving,
                            egui::Button::new(
                                egui::RichText::new("⚠ Switch to LIVE")
                                    .color(egui::Color32::from_rgb(255, 80, 80))
                                    .strong(),
                            ),
                        )
                        .on_hover_text("Real on-chain trading — requires typed confirmation")
                        .clicked()
                    {
                        self.config_panel.pending_live_switch = true;
                        self.config_panel.confirm_input.clear();
                        self.config_panel.mode_status = None;
                    }
                });
            }

            ui.add_space(4.0);

            if self.config_panel.restart_required {
                ui.colored_label(
                    egui::Color32::from_rgb(255, 200, 60),
                    "⟳ Restart the bot service for the new mode to take effect.",
                );
            }
            if let Some(status) = &self.config_panel.mode_status {
                let color = if status.starts_with('✓') {
                    egui::Color32::from_rgb(100, 220, 100)
                } else {
                    egui::Color32::from_rgb(220, 90, 90)
                };
                ui.colored_label(color, status);
            }
        });
    }
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn pnl_color(pnl: f64) -> egui::Color32 {
    if pnl > 0.0 {
        egui::Color32::from_rgb(100, 220, 100)
    } else if pnl < 0.0 {
        egui::Color32::from_rgb(220, 90, 90)
    } else {
        egui::Color32::GRAY
    }
}

fn parse_entry_meta_v3(json: &str) -> Option<V3TapeWire> {
    let s = json.trim();
    if s.is_empty() {
        return None;
    }
    serde_json::from_str(s).ok()
}

/// One-line V3 tape for grids (bv_persist, sell_press, absorb, dumps, sm_exits).
fn format_v3_tape_compact(t: &V3TapeWire) -> String {
    format!(
        "bv {:.2} · sp {:.2} · ab {:.2} · d{} · sm{}",
        t.bv_persist, t.sell_press, t.absorb, t.dumps, t.sm_exits
    )
}

fn time_kill_tier_color(tier: &str) -> egui::Color32 {
    match tier {
        "strong" => egui::Color32::from_rgb(110, 200, 130),
        "weak" => egui::Color32::from_rgb(255, 120, 100),
        "neutral" => egui::Color32::from_rgb(230, 200, 90),
        "fixed" => egui::Color32::from_rgb(160, 170, 190),
        _ => egui::Color32::LIGHT_GRAY,
    }
}

fn format_age(closed_at: i64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64;
    let age = (now - closed_at).max(0);
    if age < 60 {
        format!("{}s ago", age)
    } else if age < 3600 {
        format!("{}m ago", age / 60)
    } else if age < 86400 {
        format!("{}h ago", age / 3600)
    } else {
        format!("{}d ago", age / 86400)
    }
}

fn short_addr(addr: &str) -> String {
    if addr.len() > 12 {
        format!("{}…{}", &addr[..6], &addr[addr.len() - 4..])
    } else {
        addr.to_string()
    }
}

/// Short mint + 📋 copies `full_mint` (not the abbreviated label).
fn render_mint_with_copy(
    ui: &mut egui::Ui,
    ctx: &egui::Context,
    full_mint: &str,
    mint_copy_flash_until: &mut Option<Instant>,
) {
    ui.horizontal(|ui| {
        ui.label(
            egui::RichText::new(short_addr(full_mint))
                .monospace()
                .color(egui::Color32::from_rgb(200, 210, 230)),
        );
        let tip = format!(
            "Copy mint\n\n{full_mint}\n\nSolscan:\nhttps://solscan.io/token/{full_mint}\n\nJupiter:\nhttps://jup.ag/tokens/{full_mint}"
        );
        if ui
            .add(egui::Button::new("📋").small())
            .on_hover_text(tip)
            .clicked()
        {
            ctx.copy_text(full_mint.to_string());
            *mint_copy_flash_until = Some(Instant::now() + Duration::from_millis(1600));
        }
    });
}

fn render_open_reason(ui: &mut egui::Ui, reason: &OpenReason, sol_price: Option<f64>) {
    match reason {
        OpenReason::DevStats(s) => {
            let vol_str = if let Some(p) = sol_price {
                format!("${:.0}", s.average_volume * p)
            } else {
                format!("{:.0} SOL", s.average_volume)
            };
            ui.colored_label(
                egui::Color32::from_rgb(100, 180, 255),
                format!(
                    "DEV  coins:{} avgpnl:{:.1}% vol:{}",
                    s.total_coins, s.trader_pnl_average, vol_str
                ),
            );
        }
        OpenReason::TraderStats => {
            ui.colored_label(egui::Color32::from_rgb(255, 200, 80), "TRADER");
        }
    }
}

// ── UI ────────────────────────────────────────────────────────────────────────

impl eframe::App for Dashboard {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain_events();

        // ── Chart window ──────────────────────────────────────────────────────
        if let Some((mint, chart)) = self.chart_window.clone() {
            let mut open = true;
            egui::Window::new(format!("Chart: {}", short_addr(&mint)))
                .open(&mut open)
                .resizable(true)
                .default_size([820.0, 460.0])
                .show(ctx, |ui| {
                    if ui.button("Dev Stats").clicked() {
                        let _ = self.cmd_tx.try_send(DashCmd::FetchDevStats(mint.clone()));
                        self.loading_dev = Some(mint.clone());
                    }
                    ui.separator();

                    if chart.points.is_empty() {
                        ui.label("No trade tape for this mint yet.");
                        return;
                    }

                    use egui_plot::{HLine, Line, MarkerShape, Plot, PlotPoints, Points, Polygon, VLine};

                    let price_mult = self.sol_price.unwrap_or(1.0);
                    let y_name = if self.sol_price.is_some() {
                        "Market Cap ($)"
                    } else {
                        "Market Cap (SOL)"
                    };
                    let prefix = if self.sol_price.is_some() { "$" } else { "" };
                    let dec = if self.sol_price.is_some() { 0 } else { 1 };
                    let t0 = chart.t0;
                    let series: Vec<(i64, f64)> =
                        chart.points.iter().map(|p| (p.t, p.mcap)).collect();
                    let ref_entry_mcap = chart
                        .markers
                        .first()
                        .map(|m| m.entry_mcap)
                        .unwrap_or(series.first().map(|(_, m)| *m).unwrap_or(1.0));
                    let mut hover_tip = String::new();

                    let x_min = chart
                        .points
                        .iter()
                        .map(|p| chart_x_sec(p.t, t0))
                        .chain(
                            chart
                                .markers
                                .iter()
                                .flat_map(|m| {
                                    [
                                        chart_x_sec(m.entry_at, t0),
                                        chart_x_sec(m.closed_at, t0),
                                    ]
                                }),
                        )
                        .fold(f64::INFINITY, f64::min);
                    let x_hi = chart
                        .points
                        .iter()
                        .map(|p| chart_x_sec(p.t, t0))
                        .chain(
                            chart
                                .markers
                                .iter()
                                .flat_map(|m| {
                                    [
                                        chart_x_sec(m.entry_at, t0),
                                        chart_x_sec(m.closed_at, t0),
                                    ]
                                }),
                        )
                        .fold(f64::NEG_INFINITY, f64::max)
                        .max(1.0);
                    let x_lo = if x_min.is_finite() { x_min } else { -10.0 };
                    let y_bounds = chart_plot_y_bounds(
                        &chart.points,
                        &chart.markers,
                        price_mult,
                        x_lo,
                        x_hi,
                    );
                    let mut plot = Plot::new("price_chart")
                        .height(340.0)
                        .allow_drag(true)
                        .allow_zoom(true)
                        .show_axes(true)
                        .show_grid(true)
                        .set_margin_fraction(egui::vec2(0.12, 0.08))
                        .x_axis_label("Time (s, BUY = 0)")
                        .y_axis_label(y_name)
                        .x_axis_formatter(|mark, _| format!("{:.0}s", mark.value))
                        .label_formatter(|name, value| {
                            if name.is_empty() {
                                format!("{:.0}s", value.x)
                            } else {
                                name.to_string()
                            }
                        });
                    let x_pad = 4.0;
                    plot = plot
                        .include_x(x_lo - x_pad)
                        .include_x(x_hi + x_pad);
                    if let Some((y_lo, y_hi)) = y_bounds {
                        let pad = ((y_hi - y_lo) * 0.12).max(y_hi * 0.05).max(1.0);
                        plot = plot.include_y(y_lo - pad).include_y(y_hi + pad);
                    }

                    plot.show(ui, |plot_ui| {
                            // Hold zones under the price line.
                            for marker in &chart.markers {
                                let x_entry = chart_x_sec(marker.entry_at, t0);
                                let x_exit = chart_x_sec(marker.closed_at, t0);
                                if x_exit < x_entry {
                                    continue;
                                }
                                let entry_y = marker.entry_mcap * price_mult;
                                let exit_y = marker.exit_mcap * price_mult;
                                if !entry_y.is_finite()
                                    || !exit_y.is_finite()
                                    || entry_y <= 0.0
                                    || exit_y <= 0.0
                                {
                                    continue;
                                }
                                let (zone_y_lo, zone_y_hi) = chart_plot_y_bounds(
                                    &chart.points,
                                    &[],
                                    price_mult,
                                    x_entry,
                                    x_exit,
                                )
                                .unwrap_or((entry_y.min(exit_y), entry_y.max(exit_y)));
                                let zone_pad = ((zone_y_hi - zone_y_lo) * 0.06).max(1.0);
                                let hold_fill =
                                    egui::Color32::from_rgba_premultiplied(80, 160, 255, 14);
                                plot_ui.polygon(
                                    Polygon::new(PlotPoints::new(vec![
                                        [x_entry, zone_y_lo - zone_pad],
                                        [x_exit, zone_y_lo - zone_pad],
                                        [x_exit, zone_y_hi + zone_pad],
                                        [x_entry, zone_y_hi + zone_pad],
                                    ]))
                                    .fill_color(hold_fill)
                                    .allow_hover(false),
                                );
                            }

                            let line_pts: PlotPoints = chart
                                .points
                                .iter()
                                .map(|p| {
                                    [
                                        chart_x_sec(p.t, t0),
                                        p.mcap * price_mult,
                                    ]
                                })
                                .collect();
                            plot_ui.line(
                                Line::new(line_pts)
                                    .color(egui::Color32::from_rgb(120, 175, 255))
                                    .width(2.0)
                                    .name(""),
                            );

                            for marker in &chart.markers {
                                let x_entry = chart_x_sec(marker.entry_at, t0);
                                let x_exit = chart_x_sec(marker.closed_at, t0);
                                if x_exit < x_entry {
                                    continue;
                                }
                                let entry_y = marker.entry_mcap * price_mult;
                                let exit_y = marker.exit_mcap * price_mult;
                                if !entry_y.is_finite()
                                    || !exit_y.is_finite()
                                    || entry_y <= 0.0
                                    || exit_y <= 0.0
                                {
                                    continue;
                                }

                                let vline_entry = egui::Color32::from_rgb(70, 210, 90);
                                let vline_exit = if marker.pnl >= 0.0 {
                                    egui::Color32::from_rgb(70, 210, 90)
                                } else {
                                    egui::Color32::from_rgb(230, 85, 85)
                                };
                                plot_ui.vline(
                                    VLine::new(x_entry)
                                        .color(vline_entry)
                                        .width(1.5)
                                        .allow_hover(false),
                                );
                                plot_ui.vline(
                                    VLine::new(x_exit)
                                        .color(vline_exit)
                                        .width(1.5)
                                        .style(egui_plot::LineStyle::dashed_dense())
                                        .allow_hover(false),
                                );

                                plot_ui.hline(
                                    HLine::new(entry_y)
                                        .color(vline_entry.gamma_multiply(0.55))
                                        .width(1.0)
                                        .style(egui_plot::LineStyle::dotted_dense())
                                        .allow_hover(false),
                                );

                                plot_ui.points(
                                    Points::new(PlotPoints::new(vec![[x_entry, entry_y]]))
                                        .color(vline_entry)
                                        .radius(8.0)
                                        .shape(MarkerShape::Up)
                                        .name(format!("BUY {prefix}{:.*}", dec, entry_y)),
                                );
                                plot_ui.points(
                                    Points::new(PlotPoints::new(vec![[x_exit, exit_y]]))
                                        .color(vline_exit)
                                        .radius(8.0)
                                        .shape(MarkerShape::Down)
                                        .name(format!(
                                            "SELL +{:.0}s {prefix}{:.*} ({:+.0}%)",
                                            x_exit, dec, exit_y, marker.pnl
                                        )),
                                );
                            }

                            if let Some(hover) = plot_ui.pointer_coordinate() {
                                let t_sec = hover.x.round() as i64;
                                let mcap_plot = mcap_at_time(&series, t_sec);
                                let pct = if ref_entry_mcap > 0.0 && chart_mcap_valid(mcap_plot) {
                                    (mcap_plot / ref_entry_mcap - 1.0) * 100.0
                                } else {
                                    0.0
                                };
                                let t_label = if hover.x >= 0.0 {
                                    format!("+{:.0}s", hover.x)
                                } else {
                                    format!("{:.0}s", hover.x)
                                };
                                let mcap_disp = mcap_plot * price_mult;
                                hover_tip = if dec == 0 {
                                    format!(
                                        "t = {t_label} | mcap = {prefix}{mcap_disp:.0} | {pct:+.1}% vs entry",
                                        pct = pct,
                                    )
                                } else {
                                    format!(
                                        "t = {t_label} | mcap = {prefix}{mcap_disp:.1} | {pct:+.1}% vs entry",
                                        pct = pct,
                                    )
                                };
                            }
                        });

                    if hover_tip.is_empty() {
                        ui.label(
                            egui::RichText::new("Hover chart for time / mcap / % from entry")
                                .weak(),
                        );
                    } else {
                        ui.label(egui::RichText::new(hover_tip).monospace());
                    }
                });
            if !open {
                self.chart_window = None;
            }
        }

        // ── Dev stats popup ───────────────────────────────────────────────────
        if let Some((mint, stats_opt)) = self.selected_dev.clone() {
            let mut open = true;
            egui::Window::new(format!("Dev: {}", short_addr(&mint)))
                .open(&mut open)
                .resizable(false)
                .show(ctx, |ui| match stats_opt {
                    None => {
                        ui.label("No dev stats available for this mint.");
                        ui.small(
                            egui::RichText::new("(coin not indexed or developer unknown)")
                                .color(egui::Color32::GRAY),
                        );
                    }
                    Some(stats) => {
                        egui::Grid::new("dev_popup")
                            .num_columns(2)
                            .spacing([12.0, 4.0])
                            .show(ui, |ui| {
                                ui.label("Total coins:");
                                ui.label(stats.total_coins.to_string());
                                ui.end_row();
                                ui.label("Avg trader PnL:");
                                ui.colored_label(
                                    pnl_color(stats.trader_pnl_average),
                                    format!("{:.1}%", stats.trader_pnl_average),
                                );
                                ui.end_row();
                                ui.label("Avg volume:");
                                ui.label(self.usd_val(stats.average_volume, 0));
                                ui.end_row();
                                ui.label("Avg holders:");
                                ui.label(stats.total_holders_average.to_string());
                                ui.end_row();
                                ui.label("Median trades:");
                                ui.label(stats.median_total_trades.to_string());
                                ui.end_row();
                                ui.label("Median MCAP:");
                                ui.label(stats.median_market_cap.format_usd(self.sol_price, 0));
                                ui.end_row();
                                ui.label("Buy/sell ratio:");
                                ui.label(format!("{:.2}", stats.average_unique_buy_to_sell_ratio));
                                ui.end_row();
                                ui.label("Avg buy size:");
                                ui.label(
                                    stats.average_buy_trader_size.format_usd(self.sol_price, 2),
                                );
                                ui.end_row();
                            });
                    }
                });
            if !open {
                self.selected_dev = None;
            }
        }

        // ── Config window ─────────────────────────────────────────────────────
        if self.config_panel.open {
            let mut open = true;
            egui::Window::new("⚙ Configuration")
                .open(&mut open)
                .resizable(false)
                .min_width(340.0)
                .show(ctx, |ui| {
                    ui.heading("Trade Settings");
                    ui.add_space(8.0);

                    egui::Frame::group(ui.style()).show(ui, |ui| {
                        ui.set_min_width(300.0);
                        ui.label(egui::RichText::new("Buy Size").strong());
                        ui.add_space(4.0);

                        ui.horizontal(|ui| {
                            ui.label("Amount (SOL):");

                            let valid = self
                                .config_panel
                                .buy_size_input
                                .parse::<f64>()
                                .map(|v| v > 0.0)
                                .unwrap_or(false);

                            let input_color = if valid {
                                egui::Color32::WHITE
                            } else {
                                egui::Color32::from_rgb(255, 120, 120)
                            };

                            ui.add(
                                egui::TextEdit::singleline(&mut self.config_panel.buy_size_input)
                                    .desired_width(90.0)
                                    .text_color(input_color)
                                    .hint_text("min 0.4 (server)"),
                            );

                            ui.label(egui::RichText::new("SOL").color(egui::Color32::GRAY));
                        });

                        ui.add_space(4.0);

                        // Feedback / current value
                        if let Some(status) = &self.config_panel.buy_size_status {
                            let color = if status.starts_with('✓') {
                                egui::Color32::from_rgb(100, 220, 100)
                            } else {
                                egui::Color32::from_rgb(220, 90, 90)
                            };
                            ui.colored_label(color, status);
                        } else if let Some(remote) = self.config_panel.buy_size_remote {
                            ui.colored_label(
                                egui::Color32::GRAY,
                                format!("Server value: {:.4} SOL", remote),
                            );
                        } else {
                            ui.colored_label(egui::Color32::GRAY, "Loading from server…");
                        }

                        ui.add_space(6.0);

                        ui.horizontal(|ui| {
                            let parsed = self
                                .config_panel
                                .buy_size_input
                                .parse::<f64>()
                                .ok()
                                .filter(|&v| v > 0.0);

                            let dirty = parsed
                                .zip(self.config_panel.buy_size_remote)
                                .map(|(a, b)| (a - b).abs() > 1e-9)
                                .unwrap_or(parsed.is_some());

                            let can_save =
                                dirty && parsed.is_some() && !self.config_panel.buy_size_saving;

                            let save_label = if self.config_panel.buy_size_saving {
                                "Saving…"
                            } else {
                                "Save"
                            };
                            let save_color = if can_save {
                                egui::Color32::from_rgb(100, 220, 100)
                            } else {
                                egui::Color32::GRAY
                            };

                            if ui
                                .add_enabled(
                                    can_save,
                                    egui::Button::new(
                                        egui::RichText::new(save_label).color(save_color),
                                    ),
                                )
                                .clicked()
                            {
                                if let Some(v) = parsed {
                                    self.config_panel.buy_size_saving = true;
                                    self.config_panel.buy_size_status = None;
                                    let _ = self.cmd_tx.try_send(DashCmd::SetBuySize(v));
                                }
                            }

                            if ui.button("Reload").clicked() {
                                self.config_panel.buy_size_status = None;
                                self.config_panel.buy_size_input.clear();
                                let _ = self.cmd_tx.try_send(DashCmd::FetchBuySize);
                            }
                        });
                    });

                    ui.add_space(12.0);
                    ui.heading("Execution Mode");
                    ui.add_space(6.0);
                    self.render_mode_block(ui);
                });
            if !open {
                self.config_panel.open = false;
            }
        }

        // ── Periodic HTTP poll for mode + tx log ──────────────────────────────
        // /mode and /tx-log are not pushed through WS (HTTP is the source of
        // truth). Re-poll every 5s while the dashboard is alive so a switch
        // performed from another client is reflected here too.
        let should_poll = self
            .last_http_poll
            .map(|t| t.elapsed() >= Duration::from_secs(5))
            .unwrap_or(true);
        if should_poll {
            let _ = self.cmd_tx.try_send(DashCmd::FetchMode);
            let _ = self.cmd_tx.try_send(DashCmd::FetchTxLog);
            let _ = self.cmd_tx.try_send(DashCmd::FetchOpenPositions);
            self.last_http_poll = Some(std::time::Instant::now());
        }

        // ── Top bar ───────────────────────────────────────────────────────────
        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.heading("Position Dashboard");
                ui.separator();

                if self.connected {
                    ui.colored_label(egui::Color32::from_rgb(100, 220, 100), "● CONNECTED");
                } else {
                    ui.colored_label(egui::Color32::from_rgb(220, 90, 90), "● DISCONNECTED");
                }
                ui.separator();

                // ── Mode badge (DEMO=green, LIVE=red, unknown=gray) ──────────
                match self.mode.as_deref() {
                    Some("live") => {
                        ui.colored_label(
                            egui::Color32::from_rgb(255, 80, 80),
                            egui::RichText::new("⚠ LIVE").strong().size(15.0),
                        )
                        .on_hover_text("Real on-chain trading is ACTIVE");
                    }
                    Some("demo") => {
                        ui.colored_label(
                            egui::Color32::from_rgb(120, 220, 120),
                            egui::RichText::new("● DEMO").strong().size(15.0),
                        )
                        .on_hover_text("Simulated trading — no real funds at risk");
                    }
                    _ => {
                        ui.colored_label(egui::Color32::GRAY, "mode: …");
                    }
                }
                if self.config_panel.restart_required {
                    ui.colored_label(
                        egui::Color32::from_rgb(255, 200, 60),
                        egui::RichText::new("⟳ restart required").italics(),
                    )
                    .on_hover_text("Mode change saved to filter_config.yaml — restart the bot service to apply");
                }
                ui.separator();

                let (lbl, col) = if self.paused {
                    ("▶ RESUME", egui::Color32::from_rgb(100, 220, 100))
                } else {
                    ("⏸ PAUSE", egui::Color32::from_rgb(220, 170, 50))
                };
                if ui
                    .add(egui::Button::new(egui::RichText::new(lbl).color(col)))
                    .clicked()
                {
                    self.paused = !self.paused;
                    let _ = self.cmd_tx.try_send(DashCmd::Ws(WsCmd::SetPaused {
                        paused: self.paused,
                    }));
                }
                ui.separator();

                // ── ⚙ Config button ───────────────────────────────────────────
                let cfg_col = if self.config_panel.open {
                    egui::Color32::from_rgb(255, 215, 0)
                } else {
                    egui::Color32::from_rgb(180, 180, 180)
                };
                if ui
                    .add(egui::Button::new(
                        egui::RichText::new("⚙ Config").color(cfg_col),
                    ))
                    .on_hover_text("Open configuration panel")
                    .clicked()
                {
                    self.config_panel.open = !self.config_panel.open;
                    if self.config_panel.open {
                        self.config_panel.buy_size_status = None;
                        let _ = self.cmd_tx.try_send(DashCmd::FetchBuySize);
                    }
                }
                ui.separator();

                match self.balance {
                    Some(b) => {
                        ui.label("Balance:");
                        ui.colored_label(egui::Color32::from_rgb(255, 215, 0), self.usd_val(b, 2));
                        if let Some(p) = self.sol_price {
                            ui.label(
                                egui::RichText::new(format!("(SOL: ${:.2})", p))
                                    .color(egui::Color32::GRAY),
                            );
                        }
                    }
                    None => {
                        ui.label("Balance: —");
                    }
                }

                ui.separator();
                match &self.pubkey.clone() {
                    Some(pk) => {
                        ui.label("Wallet:");
                        if ui
                            .add(
                                egui::Button::new(
                                    egui::RichText::new(format!("📋 {}", short_addr(pk)))
                                        .monospace()
                                        .color(egui::Color32::from_rgb(180, 180, 255)),
                                )
                                .frame(false),
                            )
                            .on_hover_text(format!("Click to copy: {}", pk))
                            .clicked()
                        {
                            ctx.copy_text(pk.clone());
                        }
                    }
                    None => {
                        ui.colored_label(egui::Color32::GRAY, "Wallet: …");
                    }
                }

                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.label(format!("open: {}", self.open.len()));
                    // Quick buy-size readout on the right
                    if let Some(bs) = self.config_panel.buy_size_remote {
                        ui.separator();
                        ui.colored_label(
                            egui::Color32::from_rgb(180, 220, 255),
                            format!("buy: {:.4} SOL", bs),
                        );
                    }
                });
            });
        });

        // ── Stats bar ─────────────────────────────────────────────────────────
        if !self.history.is_empty() {
            egui::TopBottomPanel::top("stats_bar").show(ctx, |ui| {
                let total = self.history.len();
                let wins = self
                    .history
                    .iter()
                    .filter(|t| t.realized_pnl_pct > 0.0)
                    .count();
                let winrate = wins as f64 / total as f64 * 100.0;
                let avg_pnl =
                    self.history.iter().map(|t| t.realized_pnl_pct).sum::<f64>() / total as f64;
                ui.horizontal(|ui| {
                    ui.label(format!("Trades: {total}"));
                    ui.separator();
                    ui.label("Winrate:");
                    ui.colored_label(pnl_color(winrate - 50.0), format!("{winrate:.1}%"));
                    ui.separator();
                    ui.label("Avg PnL:");
                    ui.colored_label(pnl_color(avg_pnl), format!("{avg_pnl:+.2}%"));
                });
            });
        }

        // ── Central panel ─────────────────────────────────────────────────────
        egui::CentralPanel::default().show(ctx, |ui| {
            if let Some(until) = self.mint_copy_flash_until {
                if Instant::now() < until {
                    ui.horizontal(|ui| {
                        ui.colored_label(
                            egui::Color32::from_rgb(120, 200, 130),
                            "✓ Copied",
                        );
                    });
                    ctx.request_repaint_after(Duration::from_millis(120));
                } else {
                    self.mint_copy_flash_until = None;
                }
            }

            let third = (ui.available_height() / 3.0).max(80.0);

            // ── Open positions ────────────────────────────────────────────────
            ui.label(egui::RichText::new("OPEN").strong().size(14.0));
            ui.separator();
            let mut open_addresses: Vec<String> = self.open.keys().cloned().collect();
            open_addresses.sort();
            egui::ScrollArea::vertical()
                .id_salt("open_scroll")
                .max_height(third)
                .show(ui, |ui| {
                    egui::Grid::new("open_grid")
                        .num_columns(8)
                        .striped(true)
                        .min_col_width(72.0)
                        .show(ui, |ui| {
                            ui.label(egui::RichText::new("Address").strong());
                            ui.label(egui::RichText::new("PnL %").strong());
                            ui.label(egui::RichText::new("Holdings").strong());
                            ui.label(egui::RichText::new("Entry MCAP ($)").strong());
                            ui.label(egui::RichText::new("Curr MCAP ($)").strong());
                            ui.label(egui::RichText::new("Source").strong());
                            ui.label(egui::RichText::new("V3 @ entry").strong());
                            ui.label(egui::RichText::new("Time kill").strong());
                            ui.end_row();
                            for addr in &open_addresses {
                                if let Some(pos) = self.open.get(addr) {
                                    render_mint_with_copy(
                                        ui,
                                        ctx,
                                        pos.address.as_str(),
                                        &mut self.mint_copy_flash_until,
                                    );
                                    ui.colored_label(
                                        pnl_color(pos.pnl),
                                        format!("{:+.2}%", pos.pnl),
                                    );
                                    ui.label(format!("{:.4}", pos.holdings));
                                    ui.label(self.usd_val(pos.enter_mcap, 0));
                                    ui.label(self.usd_val(pos.market_cap, 0));
                                    render_open_reason(ui, &pos.open_reason, self.sol_price);
                                    if let Some(ref t) = pos.v3_tape {
                                        let s = format_v3_tape_compact(t);
                                        ui.label(
                                            egui::RichText::new(&s)
                                                .small()
                                                .monospace()
                                                .color(egui::Color32::from_rgb(190, 205, 230)),
                                        )
                                        .on_hover_text(&s);
                                    } else {
                                        ui.label(
                                            egui::RichText::new("—")
                                                .small()
                                                .color(egui::Color32::GRAY),
                                        );
                                    }
                                    match (&pos.time_kill_tier, pos.time_kill_after_secs) {
                                        (Some(tier), Some(secs)) => {
                                            ui.colored_label(
                                                time_kill_tier_color(tier),
                                                format!("{tier} · {secs}s"),
                                            )
                                            .on_hover_text(
                                                "Adaptive time-kill window: tier from entry tape + live mcap velocity; \
                                                 position closes if held ≥ window and PnL < min profit. \
                                                 \"fixed\" = non-adaptive config window.",
                                            );
                                        }
                                        _ => {
                                            ui.label(
                                                egui::RichText::new("…")
                                                    .small()
                                                    .color(egui::Color32::GRAY),
                                            );
                                        }
                                    }
                                    ui.end_row();
                                }
                            }
                        });
                });

            ui.add_space(6.0);

            // ── Tx log ────────────────────────────────────────────────────────
            ui.horizontal(|ui| {
                ui.label(egui::RichText::new("TX LOG").strong().size(14.0));
                if let Some(m) = self.mode.as_deref() {
                    let (txt, col) = match m {
                        "live" => ("LIVE", egui::Color32::from_rgb(255, 90, 90)),
                        "demo" => ("DEMO", egui::Color32::from_rgb(120, 220, 120)),
                        _ => ("?", egui::Color32::GRAY),
                    };
                    ui.colored_label(col, txt);
                }
                ui.label(
                    egui::RichText::new(format!("({})", self.tx_log.len()))
                        .color(egui::Color32::GRAY),
                );
            });
            ui.separator();
            let tx_ctx = ctx.clone();
            egui::ScrollArea::vertical()
                .id_salt("tx_log_scroll")
                .max_height(third.min(180.0))
                .show(ui, |ui| {
                    egui::Grid::new("tx_log_grid")
                        .num_columns(8)
                        .striped(true)
                        .min_col_width(52.0)
                        .show(ui, |ui| {
                            ui.label(egui::RichText::new("Time").strong());
                            ui.label(egui::RichText::new("Kind").strong());
                            ui.label(egui::RichText::new("Mode").strong());
                            ui.label(egui::RichText::new("Mint").strong());
                            ui.label(egui::RichText::new("SOL act.").strong());
                            ui.label(egui::RichText::new("SOL est.").strong());
                            ui.label(egui::RichText::new("V3 / time kill").strong());
                            ui.label(egui::RichText::new("Status").strong());
                            ui.end_row();

                            // Newest first.
                            for row in self.tx_log.iter().rev() {
                                ui.label(
                                    egui::RichText::new(format_age(row.ts))
                                        .color(egui::Color32::GRAY),
                                );
                                let (lbl, col) = match row.kind {
                                    TxEventKind::Buy => (
                                        "BUY",
                                        egui::Color32::from_rgb(120, 200, 255),
                                    ),
                                    TxEventKind::Sell => (
                                        "SELL",
                                        egui::Color32::from_rgb(255, 200, 100),
                                    ),
                                };
                                ui.colored_label(col, lbl);
                                let mode_col = match row.mode.as_str() {
                                    "live" => egui::Color32::from_rgb(255, 90, 90),
                                    "demo" => egui::Color32::from_rgb(120, 220, 120),
                                    _ => egui::Color32::GRAY,
                                };
                                ui.colored_label(mode_col, row.mode.to_uppercase());
                                render_mint_with_copy(
                                    ui,
                                    &tx_ctx,
                                    &row.mint,
                                    &mut self.mint_copy_flash_until,
                                );
                                ui.label(format!("{:.4}", row.amount_sol));
                                match (row.kind, row.amount_sol_estimated) {
                                    (TxEventKind::Sell, Some(est)) => {
                                        ui.label(format!("{:.4}", est)).on_hover_text(
                                            "Estimated from bonding curve / Jupiter quote × slippage; \
                                             act. = wallet lamport delta from tx meta.",
                                        );
                                    }
                                    _ => {
                                        ui.label(
                                            egui::RichText::new("—")
                                                .small()
                                                .color(egui::Color32::GRAY),
                                        );
                                    }
                                }

                                match row.kind {
                                    TxEventKind::Buy => {
                                        if let Some(ref t) = row.v3_tape {
                                            let s = format_v3_tape_compact(t);
                                            ui.label(
                                                egui::RichText::new(&s)
                                                    .small()
                                                    .monospace()
                                                    .color(egui::Color32::from_rgb(180, 200, 235)),
                                            )
                                            .on_hover_text("V3 tape at entry (buy)");
                                        } else {
                                            ui.label(
                                                egui::RichText::new("—")
                                                    .small()
                                                    .color(egui::Color32::GRAY),
                                            );
                                        }
                                    }
                                    TxEventKind::Sell => {
                                        if let Some(ref d) = row.time_kill_detail {
                                            ui.label(
                                                egui::RichText::new(d.as_str())
                                                    .small()
                                                    .monospace()
                                                    .color(egui::Color32::from_rgb(240, 190, 120)),
                                            )
                                            .on_hover_text(
                                                "TIME KILL: adaptive tier + kill window seconds, or fixed window from config.",
                                            );
                                        } else {
                                            ui.label(
                                                egui::RichText::new("—")
                                                    .small()
                                                    .color(egui::Color32::GRAY),
                                            );
                                        }
                                    }
                                }

                                // Live: hide raw signatures in-grid; show compact status + optional
                                // close hint, full sig only in tooltip / copy.
                                if let Some(sig) = &row.signature {
                                    ui.horizontal(|ui| {
                                        let (st_lbl, st_col) = match row.status.as_str() {
                                            "sent" | "confirmed" => {
                                                ("ok", egui::Color32::from_rgb(110, 190, 130))
                                            }
                                            "failed" => ("failed", egui::Color32::from_rgb(220, 90, 90)),
                                            _ => (row.status.as_str(), egui::Color32::LIGHT_GRAY),
                                        };
                                        ui.colored_label(st_col, st_lbl);
                                        if let Some(ref r) = row.reason {
                                            if !r.is_empty() {
                                                let short = if r.len() > 28 {
                                                    format!("{}…", &r[..28])
                                                } else {
                                                    r.clone()
                                                };
                                                ui.label(
                                                    egui::RichText::new(short)
                                                        .small()
                                                        .color(egui::Color32::GRAY),
                                                );
                                            }
                                        }
                                        let tip = format!(
                                            "Signature (click 📋 to copy)\n{sig}\n\nSolscan:\nhttps://solscan.io/tx/{sig}"
                                        );
                                        if ui
                                            .add(egui::Button::new("📋").small())
                                            .on_hover_text(&tip)
                                            .clicked()
                                        {
                                            tx_ctx.copy_text(sig.clone());
                                        }
                                    });
                                } else {
                                    let (lbl, col) = match row.status.as_str() {
                                        "failed" => (
                                            row.reason.clone().unwrap_or_else(|| "failed".into()),
                                            egui::Color32::from_rgb(220, 90, 90),
                                        ),
                                        _ => (
                                            "simulated".into(),
                                            egui::Color32::GRAY,
                                        ),
                                    };
                                    ui.colored_label(col, lbl);
                                }
                                ui.end_row();
                            }
                        });
                });

            ui.add_space(6.0);

            // ── History ───────────────────────────────────────────────────────
            ui.label(egui::RichText::new("HISTORY").strong().size(14.0));
            ui.separator();

            let cmd_tx = self.cmd_tx.clone();
            egui::ScrollArea::vertical()
                .id_salt("history_scroll")
                .show(ui, |ui| {
                    egui::Grid::new("history_grid")
                        .num_columns(7)
                        .striped(true)
                        .min_col_width(72.0)
                        .show(ui, |ui| {
                            ui.label(egui::RichText::new("Time").strong());
                            ui.label(egui::RichText::new("Mint").strong());
                            ui.label(egui::RichText::new("PnL %").strong());
                            ui.label(egui::RichText::new("Invested ($)").strong());
                            ui.label(egui::RichText::new("Entry MCAP ($)").strong());
                            ui.label(egui::RichText::new("V3 @ close").strong());
                            ui.label(egui::RichText::new("Close Reason").strong());
                            ui.end_row();

                            for row in &self.history {
                                ui.label(
                                    egui::RichText::new(format_age(row.closed_at))
                                        .color(egui::Color32::GRAY),
                                );
                                ui.horizontal(|ui| {
                                    render_mint_with_copy(
                                        ui,
                                        ctx,
                                        &row.mint,
                                        &mut self.mint_copy_flash_until,
                                    );
                                    if ui
                                        .add(egui::Button::new("📈").small())
                                        .on_hover_text("Open chart")
                                        .clicked()
                                    {
                                        let _ = cmd_tx.try_send(DashCmd::FetchChart(row.mint.clone()));
                                    }
                                });
                                ui.colored_label(
                                    pnl_color(row.realized_pnl_pct),
                                    format!("{:+.2}%", row.realized_pnl_pct),
                                );
                                ui.label(self.usd_val(row.invested_sol, 2));
                                ui.label(self.usd_val(row.entry_mcap_sol, 0));
                                if let Some(t) = parse_entry_meta_v3(&row.entry_meta) {
                                    let s = format_v3_tape_compact(&t);
                                    ui.label(
                                        egui::RichText::new(&s)
                                            .small()
                                            .monospace()
                                            .color(egui::Color32::from_rgb(175, 195, 225)),
                                    )
                                    .on_hover_text(format!(
                                        "Tape snapshot persisted at full close (entry_meta JSON).\n{s}"
                                    ));
                                } else {
                                    ui.label(
                                        egui::RichText::new("—")
                                            .small()
                                            .color(egui::Color32::DARK_GRAY),
                                    );
                                }
                                ui.label(&row.close_reason);
                                ui.end_row();
                            }
                        });
                });
        });

        if self.connected {
            ctx.request_repaint_after(Duration::from_millis(100));
        }
    }
}

// ── Background thread ─────────────────────────────────────────────────────────

fn spawn_ws_thread(
    tx: mpsc::SyncSender<AppEvent>,
    ctx: egui::Context,
    cmd_rx: tokio::sync::mpsc::Receiver<DashCmd>,
    ws_url: String,
    http_url: String,
) {
    thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        rt.block_on(ws_loop(tx, ctx, cmd_rx, ws_url, http_url));
    });
}

async fn ws_loop(
    tx: mpsc::SyncSender<AppEvent>,
    ctx: egui::Context,
    mut cmd_rx: tokio::sync::mpsc::Receiver<DashCmd>,
    ws_url: String,
    http_url: String,
) {
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::connect_async;
    use tokio_tungstenite::tungstenite::Message;

    // SOL price fetcher
    let tx_price = tx.clone();
    let ctx_price = ctx.clone();
    tokio::spawn(async move {
        #[derive(Deserialize)]
        struct BinanceTicker {
            price: String,
        }
        loop {
            match reqwest::get("https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT").await {
                Ok(resp) => {
                    if let Ok(json) = resp.json::<BinanceTicker>().await {
                        if let Ok(p) = json.price.parse::<f64>() {
                            let _ = tx_price.send(AppEvent::SolPrice(p));
                            ctx_price.request_repaint();
                        }
                    }
                }
                Err(e) => eprintln!("[price] fetch error: {}", e),
            }
            tokio::time::sleep(Duration::from_secs(10)).await;
        }
    });

    loop {
        match connect_async(ws_url.as_str()).await {
            Ok((ws, _)) => {
                let _ = tx.send(AppEvent::Connected);
                ctx.request_repaint();

                // Fetch bot trades
                {
                    let url = format!("{}/bot-trades", http_url);
                    let tx2 = tx.clone();
                    let ctx2 = ctx.clone();
                    tokio::spawn(async move {
                        if let Ok(resp) = reqwest::get(&url).await {
                            if let Ok(rows) = resp.json::<Vec<BotTradeRow>>().await {
                                let _ = tx2.send(AppEvent::BotTrades(rows));
                                ctx2.request_repaint();
                            }
                        }
                    });
                }

                // Fetch status
                {
                    #[derive(serde::Deserialize)]
                    struct StatusResp {
                        paused: bool,
                        balance_sol: f64,
                        #[serde(default)]
                        mode: Option<String>,
                        #[serde(default)]
                        wallet: Option<String>,
                    }
                    let url = format!("{}/status", http_url);
                    let tx2 = tx.clone();
                    let ctx2 = ctx.clone();
                    tokio::spawn(async move {
                        if let Ok(resp) = reqwest::get(&url).await {
                            if let Ok(s) = resp.json::<StatusResp>().await {
                                let _ = tx2.send(AppEvent::Status {
                                    paused: s.paused,
                                    mode: s.mode,
                                });
                                if let Some(pk) = s.wallet {
                                    let _ = tx2.send(AppEvent::Pubkey(pk));
                                }
                                let _ = tx2.send(AppEvent::Msg(WsMsg::BalanceUpdate {
                                    balance: s.balance_sol,
                                }));
                                ctx2.request_repaint();
                            }
                        }
                    });
                }

                // Fetch pubkey
                {
                    #[derive(serde::Deserialize)]
                    struct PubkeyResp {
                        pubkey: String,
                    }
                    let url = format!("{}/pubkey", http_url);
                    let tx2 = tx.clone();
                    let ctx2 = ctx.clone();
                    tokio::spawn(async move {
                        if let Ok(resp) = reqwest::get(&url).await {
                            if let Ok(p) = resp.json::<PubkeyResp>().await {
                                let _ = tx2.send(AppEvent::Pubkey(p.pubkey));
                                ctx2.request_repaint();
                            }
                        }
                    });
                }

                // Fetch buy size
                {
                    let url = format!("{}/buy-size", http_url);
                    let tx2 = tx.clone();
                    let ctx2 = ctx.clone();
                    tokio::spawn(async move {
                        fetch_buy_size(&url, &tx2, &ctx2).await;
                    });
                }

                fetch_open_positions_http(&http_url, &tx, &ctx);

                let (mut sink, mut stream) = ws.split();
                loop {
                    tokio::select! {
                        msg = stream.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    if let Ok(parsed) = serde_json::from_str::<WsMsg>(&text) {
                                        if let WsMsg::PositionOpen { .. } = &parsed {
                                            // WS broadcast is lossy on reconnect; refresh OPEN from HTTP.
                                            fetch_open_positions_http(&http_url, &tx, &ctx);
                                        }
                                        if let WsMsg::PositionClose { .. } = &parsed {
                                            let url = format!("{}/bot-trades", http_url);
                                            let tx2 = tx.clone(); let ctx2 = ctx.clone();
                                            tokio::spawn(async move {
                                                tokio::time::sleep(Duration::from_millis(1500)).await;
                                                if let Ok(resp) = reqwest::get(&url).await {
                                                    if let Ok(rows) = resp.json::<Vec<BotTradeRow>>().await {
                                                        let _ = tx2.send(AppEvent::BotTrades(rows));
                                                        ctx2.request_repaint();
                                                    }
                                                }
                                            });
                                        }
                                        let _ = tx.send(AppEvent::Msg(parsed));
                                        ctx.request_repaint();
                                    } else {
                                        eprintln!(
                                            "[dashboard] WS JSON parse failed ({} bytes): {}",
                                            text.len(),
                                            &text[..text.len().min(200)]
                                        );
                                    }
                                }
                                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => break,
                                _ => {}
                            }
                        }
                        cmd = cmd_rx.recv() => {
                            match cmd {
                                Some(DashCmd::Ws(ws_cmd)) => {
                                    if let Ok(json) = serde_json::to_string(&ws_cmd) {
                                        let _ = sink.send(Message::Text(json.into())).await;
                                    }
                                }
                                Some(DashCmd::FetchBuySize) => {
                                    let url = format!("{}/buy-size", http_url);
                                    let tx2 = tx.clone(); let ctx2 = ctx.clone();
                                    tokio::spawn(async move { fetch_buy_size(&url, &tx2, &ctx2).await; });
                                }
                                Some(DashCmd::SetBuySize(sol)) => {
                                    let url = format!("{}/buy-size", http_url);
                                    let tx2 = tx.clone(); let ctx2 = ctx.clone();
                                    tokio::spawn(async move { set_buy_size(&url, sol, &tx2, &ctx2).await; });
                                }
                                Some(DashCmd::FetchDevStats(mint)) => {
                                    let url = format!("{}/dev-stats/{}", http_url, mint);
                                    let tx2 = tx.clone(); let ctx2 = ctx.clone();
                                    tokio::spawn(async move {
                                        let stats = match reqwest::get(&url).await {
                                            Ok(resp) => {
                                                let text = resp.text().await.unwrap_or_default();
                                                serde_json::from_str::<Option<DevStats>>(&text).ok().flatten()
                                            }
                                            Err(_) => None,
                                        };
                                        let _ = tx2.send(AppEvent::DevStats { mint, stats });
                                        ctx2.request_repaint();
                                    });
                                }
                                Some(DashCmd::FetchChart(mint)) => {
                                    let url = format!("{}/chart/{}", http_url, mint);
                                    let tx2 = tx.clone(); let ctx2 = ctx.clone();
                                    tokio::spawn(async move {
                                        let data = match reqwest::get(&url).await {
                                            Ok(resp) => resp.json::<ChartData>().await.ok(),
                                            Err(_) => None,
                                        };
                                        let _ = tx2.send(AppEvent::ChartData { mint, data });
                                        ctx2.request_repaint();
                                    });
                                }
                                Some(DashCmd::FetchMode) => {
                                    let url = format!("{}/mode", http_url);
                                    let tx2 = tx.clone(); let ctx2 = ctx.clone();
                                    tokio::spawn(async move {
                                        if let Ok(resp) = reqwest::get(&url).await
                                            && let Ok(info) = resp.json::<ModeInfo>().await {
                                                let _ = tx2.send(AppEvent::ModeInfo(info));
                                                ctx2.request_repaint();
                                            }
                                    });
                                }
                                Some(DashCmd::FetchTxLog) => {
                                    let url = format!("{}/tx-log", http_url);
                                    let tx2 = tx.clone(); let ctx2 = ctx.clone();
                                    tokio::spawn(async move {
                                        if let Ok(resp) = reqwest::get(&url).await
                                            && let Ok(events) = resp.json::<Vec<WsMsg>>().await {
                                                let rows: Vec<TxLogRow> = events
                                                    .into_iter()
                                                    .filter_map(|m| match m {
                                                        WsMsg::TxEvent {
                                                            kind,
                                                            mint,
                                                            signature,
                                                            amount_sol,
                                                            amount_sol_estimated,
                                                            status,
                                                            reason,
                                                            mode,
                                                            ts,
                                                            v3_tape,
                                                            time_kill_detail,
                                                        } => Some(TxLogRow {
                                                            kind,
                                                            mint,
                                                            signature,
                                                            amount_sol,
                                                            amount_sol_estimated,
                                                            status,
                                                            reason,
                                                            mode,
                                                            ts,
                                                            v3_tape,
                                                            time_kill_detail,
                                                        }),
                                                        _ => None,
                                                    })
                                                    .collect();
                                                let _ = tx2.send(AppEvent::TxLog(rows));
                                                ctx2.request_repaint();
                                            }
                                    });
                                }
                                Some(DashCmd::FetchOpenPositions) => {
                                    let url = http_url.clone();
                                    let tx2 = tx.clone();
                                    let ctx2 = ctx.clone();
                                    tokio::spawn(async move {
                                        fetch_open_positions_http(&url, &tx2, &ctx2);
                                    });
                                }
                                Some(DashCmd::SetMode { mode, confirm_live }) => {
                                    let url = format!("{}/mode", http_url);
                                    let tx2 = tx.clone(); let ctx2 = ctx.clone();
                                    tokio::spawn(async move {
                                        let client = reqwest::Client::new();
                                        let mut req = client.put(&url).json(&serde_json::json!({"mode": mode}));
                                        if confirm_live {
                                            req = req.header("X-Confirm-Live", "yes");
                                        }
                                        match req.send().await {
                                            Ok(resp) => {
                                                let status = resp.status();
                                                let text = resp.text().await.unwrap_or_default();
                                                if status.is_success() {
                                                    #[derive(serde::Deserialize)]
                                                    struct R { mode: String, restart_required: bool }
                                                    if let Ok(r) = serde_json::from_str::<R>(&text) {
                                                        let _ = tx2.send(AppEvent::ModeSetOk { mode: r.mode, restart_required: r.restart_required });
                                                    } else {
                                                        let _ = tx2.send(AppEvent::ModeSetErr(format!("bad response: {}", text)));
                                                    }
                                                } else {
                                                    let _ = tx2.send(AppEvent::ModeSetErr(format!("HTTP {}: {}", status, text)));
                                                }
                                            }
                                            Err(e) => {
                                                let _ = tx2.send(AppEvent::ModeSetErr(e.to_string()));
                                            }
                                        }
                                        ctx2.request_repaint();
                                    });
                                }
                                None => {}
                            }
                        }
                    }
                }

                let _ = tx.send(AppEvent::Disconnected);
                ctx.request_repaint();
            }
            Err(_) => {}
        }
        tokio::time::sleep(Duration::from_secs(3)).await;
    }
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

fn fetch_open_positions_http(
    http_url: &str,
    tx: &mpsc::SyncSender<AppEvent>,
    ctx: &egui::Context,
) {
    let url = format!("{}/positions", http_url);
    let tx = tx.clone();
    let ctx = ctx.clone();
    tokio::spawn(async move {
        match reqwest::get(&url).await {
            Ok(resp) if resp.status().is_success() => {
                if let Ok(rows) = resp.json::<Vec<OpenPositionWire>>().await {
                    let _ = tx.send(AppEvent::OpenPositions(rows));
                    ctx.request_repaint();
                } else {
                    eprintln!("[dashboard] GET /positions: JSON decode failed");
                }
            }
            Ok(resp) => {
                eprintln!("[dashboard] GET /positions: HTTP {}", resp.status());
            }
            Err(e) => eprintln!("[dashboard] GET /positions: {e}"),
        }
    });
}

async fn fetch_buy_size(url: &str, tx: &mpsc::SyncSender<AppEvent>, ctx: &egui::Context) {
    #[derive(Deserialize)]
    struct BuySizeResp {
        sol: f64,
    }
    if let Ok(resp) = reqwest::get(url).await {
        if let Ok(b) = resp.json::<BuySizeResp>().await {
            let _ = tx.send(AppEvent::BuySize(b.sol));
            ctx.request_repaint();
        }
    }
}

async fn set_buy_size(url: &str, sol: f64, tx: &mpsc::SyncSender<AppEvent>, ctx: &egui::Context) {
    let body = serde_json::json!({ "sol": sol });
    let client = reqwest::Client::new();
    match client.put(url).json(&body).send().await {
        Ok(resp) if resp.status().is_success() => {
            let _ = tx.send(AppEvent::BuySizeSetOk);
        }
        Ok(resp) => {
            let _ = tx.send(AppEvent::BuySizeSetErr(format!("HTTP {}", resp.status())));
        }
        Err(e) => {
            let _ = tx.send(AppEvent::BuySizeSetErr(e.to_string()));
        }
    }
    ctx.request_repaint();
}

// ── Config ────────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
struct DashboardConfig {
    ws_url: String,
    http_url: String,
}

/// Resolves `dashboard_config.json` for dev (cwd), portable runs (next to the binary),
/// and macOS `.app` bundles (`Contents/Resources/`), where the process cwd is not the project dir.
///
/// `cargo-bundle` may place files that used `../` in globs under `Resources/_up_/…`; we check that too.
fn resolve_dashboard_config_path() -> PathBuf {
    if let Ok(p) = std::env::var("DASHBOARD_CONFIG") {
        return PathBuf::from(p);
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let resources = dir.join("../Resources");
            for rel in [
                "dashboard_config.json",
                "_up_/dashboard_config.json",
            ] {
                let p = resources.join(rel);
                if p.is_file() {
                    return p;
                }
            }
            let beside = dir.join("dashboard_config.json");
            if beside.is_file() {
                return beside;
            }
        }
    }
    PathBuf::from("dashboard_config.json")
}

fn load_config() -> DashboardConfig {
    let path = resolve_dashboard_config_path();
    let content = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "dashboard_config.json not found (tried {}): {}",
            path.display(),
            e
        )
    });
    serde_json::from_str(&content).unwrap_or_else(|e| {
        panic!(
            "invalid dashboard_config.json ({}): {}",
            path.display(),
            e
        )
    })
}

fn main() -> eframe::Result<()> {
    let config = load_config();

    let (tx, rx) = mpsc::sync_channel::<AppEvent>(1024);
    let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel::<DashCmd>(32);

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("Position Dashboard")
            .with_inner_size([1100.0, 700.0]),
        ..Default::default()
    };

    eframe::run_native(
        "Position Dashboard",
        options,
        Box::new(move |cc| {
            spawn_ws_thread(
                tx,
                cc.egui_ctx.clone(),
                cmd_rx,
                config.ws_url.clone(),
                config.http_url.clone(),
            );
            Ok(Box::new(Dashboard::new(cc, rx, cmd_tx)))
        }),
    )
}
