#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use eframe::egui;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::mpsc, thread, time::Duration};

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

#[derive(Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WsMsg {
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
}

#[derive(Deserialize, Clone)]
struct ChartMarker {
    entry_mcap: f64,
    exit_mcap: f64,
    pnl: f64,
    reason: String,
}

#[derive(Deserialize, Clone)]
struct ChartData {
    prices: Vec<f64>,
    markers: Vec<ChartMarker>,
}

// ── Commands ──────────────────────────────────────────────────────────────────

#[derive(Serialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
enum WsCmd {
    SetPaused { paused: bool },
}

enum DashCmd {
    Ws(WsCmd),
    FetchDevStats(String),
    FetchChart(String),
    FetchBuySize,
    SetBuySize(f64),
}

// ── App events ────────────────────────────────────────────────────────────────

enum AppEvent {
    Connected,
    Disconnected,
    Msg(WsMsg),
    BotTrades(Vec<BotTradeRow>),
    Status {
        paused: bool,
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
}

// ── Positions ─────────────────────────────────────────────────────────────────

struct Position {
    address: String,
    open_reason: OpenReason,
    pnl: f64,
    holdings: f64,
    market_cap: f64,
    enter_mcap: f64,
}

// ── Config panel ──────────────────────────────────────────────────────────────

struct ConfigPanel {
    open: bool,
    buy_size_remote: Option<f64>,
    buy_size_input: String,
    buy_size_status: Option<String>,
    buy_size_saving: bool,
}

impl ConfigPanel {
    fn new() -> Self {
        Self {
            open: false,
            buy_size_remote: None,
            buy_size_input: String::new(),
            buy_size_status: None,
            buy_size_saving: false,
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
    config_panel: ConfigPanel,
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
            config_panel: ConfigPanel::new(),
        }
    }

    fn drain_events(&mut self) {
        while let Ok(ev) = self.rx.try_recv() {
            match ev {
                AppEvent::Connected => self.connected = true,
                AppEvent::Disconnected => self.connected = false,
                AppEvent::BotTrades(rows) => self.history = rows,
                AppEvent::Status { paused } => self.paused = paused,
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
                        self.chart_window = Some((mint, d));
                    }
                }
                AppEvent::Msg(msg) => match msg {
                    WsMsg::PositionOpen {
                        address,
                        open_reason,
                        enter_mcap,
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
                            },
                        );
                    }
                    WsMsg::PositionUpdate {
                        address,
                        pnl,
                        holdings,
                        market_cap,
                    } => {
                        if let Some(pos) = self.open.get_mut(&address) {
                            pos.pnl = pnl;
                            pos.holdings = holdings;
                            pos.market_cap = market_cap;
                        }
                    }
                    WsMsg::PositionClose { address, .. } => {
                        self.open.remove(&address);
                    }
                    WsMsg::BalanceUpdate { balance } => self.balance = Some(balance),
                    WsMsg::PausedState { paused } => self.paused = paused,
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
                .default_size([700.0, 400.0])
                .show(ctx, |ui| {
                    if ui.button("Dev Stats").clicked() {
                        let _ = self.cmd_tx.try_send(DashCmd::FetchDevStats(mint.clone()));
                        self.loading_dev = Some(mint.clone());
                    }
                    ui.separator();

                    use egui_plot::{Line, MarkerShape, Plot, PlotPoints, Points};

                    Plot::new("price_chart")
                        .height(300.0)
                        .allow_drag(true)
                        .allow_zoom(true)
                        .show(ui, |plot_ui| {
                            let price_mult = self.sol_price.unwrap_or(1.0);
                            let y_name = if self.sol_price.is_some() {
                                "Market Cap ($)"
                            } else {
                                "Market Cap (SOL)"
                            };

                            let line_pts: PlotPoints = chart
                                .prices
                                .iter()
                                .enumerate()
                                .map(|(i, &p)| [i as f64, p * price_mult])
                                .collect();
                            plot_ui.line(
                                Line::new(line_pts)
                                    .color(egui::Color32::from_rgb(150, 200, 255))
                                    .name(y_name),
                            );

                            for marker in &chart.markers {
                                let entry_idx = chart
                                    .prices
                                    .iter()
                                    .enumerate()
                                    .min_by(|x, y| {
                                        let a = *x.1;
                                        let b = *y.1;
                                        (a - marker.entry_mcap)
                                            .abs()
                                            .partial_cmp(&(b - marker.entry_mcap).abs())
                                            .unwrap()
                                    })
                                    .map(|(i, _)| i)
                                    .unwrap_or(0);

                                let exit_idx = chart.prices[entry_idx..]
                                    .iter()
                                    .enumerate()
                                    .min_by(|x, y| {
                                        let a = *x.1;
                                        let b = *y.1;
                                        (a - marker.exit_mcap)
                                            .abs()
                                            .partial_cmp(&(b - marker.exit_mcap).abs())
                                            .unwrap()
                                    })
                                    .map(|(i, _)| entry_idx + i)
                                    .unwrap_or(entry_idx);

                                let entry_color = egui::Color32::from_rgb(80, 220, 80);
                                let exit_color = if marker.pnl >= 0.0 {
                                    egui::Color32::from_rgb(80, 220, 80)
                                } else {
                                    egui::Color32::from_rgb(220, 80, 80)
                                };

                                let entry_val = marker.entry_mcap * price_mult;
                                let exit_val = marker.exit_mcap * price_mult;
                                let prefix = if self.sol_price.is_some() { "$" } else { "" };
                                let dec = if self.sol_price.is_some() { 0 } else { 1 };

                                plot_ui.points(
                                    Points::new(PlotPoints::new(vec![[
                                        entry_idx as f64,
                                        entry_val,
                                    ]]))
                                    .color(entry_color)
                                    .radius(8.0)
                                    .shape(MarkerShape::Up)
                                    .name(format!("Entry {}{:.*}", prefix, dec, entry_val)),
                                );
                                plot_ui.points(
                                    Points::new(PlotPoints::new(vec![[exit_idx as f64, exit_val]]))
                                        .color(exit_color)
                                        .radius(8.0)
                                        .shape(MarkerShape::Down)
                                        .name(format!(
                                            "Exit {}{:.*} ({:+.1}%) {}",
                                            prefix, dec, exit_val, marker.pnl, marker.reason
                                        )),
                                );
                            }
                        });
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
                                    .hint_text("e.g. 0.6"),
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
                });
            if !open {
                self.config_panel.open = false;
            }
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
                        .num_columns(6)
                        .striped(true)
                        .min_col_width(90.0)
                        .show(ui, |ui| {
                            ui.label(egui::RichText::new("Address").strong());
                            ui.label(egui::RichText::new("PnL %").strong());
                            ui.label(egui::RichText::new("Holdings").strong());
                            ui.label(egui::RichText::new("Entry MCAP ($)").strong());
                            ui.label(egui::RichText::new("Curr MCAP ($)").strong());
                            ui.label(egui::RichText::new("Source").strong());
                            ui.end_row();
                            for addr in &open_addresses {
                                if let Some(pos) = self.open.get(addr) {
                                    ui.label(
                                        egui::RichText::new(short_addr(&pos.address)).monospace(),
                                    );
                                    ui.colored_label(
                                        pnl_color(pos.pnl),
                                        format!("{:+.2}%", pos.pnl),
                                    );
                                    ui.label(format!("{:.4}", pos.holdings));
                                    ui.label(self.usd_val(pos.enter_mcap, 0));
                                    ui.label(self.usd_val(pos.market_cap, 0));
                                    render_open_reason(ui, &pos.open_reason, self.sol_price);
                                    ui.end_row();
                                }
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
                        .num_columns(6)
                        .striped(true)
                        .min_col_width(80.0)
                        .show(ui, |ui| {
                            ui.label(egui::RichText::new("Time").strong());
                            ui.label(egui::RichText::new("Mint").strong());
                            ui.label(egui::RichText::new("PnL %").strong());
                            ui.label(egui::RichText::new("Invested ($)").strong());
                            ui.label(egui::RichText::new("Entry MCAP ($)").strong());
                            ui.label(egui::RichText::new("Close Reason").strong());
                            ui.end_row();

                            for row in &self.history {
                                ui.label(
                                    egui::RichText::new(format_age(row.closed_at))
                                        .color(egui::Color32::GRAY),
                                );
                                let short = short_addr(&row.mint);
                                let btn = egui::Button::new(
                                    egui::RichText::new(short)
                                        .monospace()
                                        .color(egui::Color32::from_rgb(120, 180, 255)),
                                )
                                .frame(false);
                                if ui.add(btn).on_hover_text("Click for chart").clicked() {
                                    let _ = cmd_tx.try_send(DashCmd::FetchChart(row.mint.clone()));
                                }
                                ui.colored_label(
                                    pnl_color(row.realized_pnl_pct),
                                    format!("{:+.2}%", row.realized_pnl_pct),
                                );
                                ui.label(self.usd_val(row.invested_sol, 2));
                                ui.label(self.usd_val(row.entry_mcap_sol, 0));
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
                    }
                    let url = format!("{}/status", http_url);
                    let tx2 = tx.clone();
                    let ctx2 = ctx.clone();
                    tokio::spawn(async move {
                        if let Ok(resp) = reqwest::get(&url).await {
                            if let Ok(s) = resp.json::<StatusResp>().await {
                                let _ = tx2.send(AppEvent::Status { paused: s.paused });
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

                let (mut sink, mut stream) = ws.split();
                loop {
                    tokio::select! {
                        msg = stream.next() => {
                            match msg {
                                Some(Ok(Message::Text(text))) => {
                                    if let Ok(parsed) = serde_json::from_str::<WsMsg>(&text) {
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

fn load_config() -> DashboardConfig {
    let content =
        std::fs::read_to_string("dashboard_config.json").expect("dashboard_config.json not found");
    serde_json::from_str(&content).expect("invalid dashboard_config.json")
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
