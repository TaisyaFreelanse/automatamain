use solana_address::Address;

use crate::{
    autobuy::{
        exit_engine::{ExitProfile, PositionPhase},
        open_reason::OpenReason,
    },
    generalize::general_pool::Pool,
    helper::Amount,
    learning::LearningTradeSnapshot,
    scoring::features::EarlyTapePoint,
};

pub struct Position {
    pub pool: Box<dyn Pool>,
    pub initial_holdings: Amount,
    pub holdings: Amount,
    pub enter_mcap: Amount,
    pub highest_mcap: f64,
    /// Unified profit floor — exit if pnl() drops at or below this value.
    /// Negative = loss threshold (e.g. -25.0), positive = locked-in profit floor (e.g. 5.0).
    pub exit_profit_floor: f64,
    pub tp1_triggered: bool,
    pub tp2_triggered: bool,
    pub tp3_triggered: bool,
    pub tp4_triggered: bool,
    pub tp5_triggered: bool,
    /// Partial profit-lock at bonding-curve mcap ceiling (moonbag + trailing on remainder).
    pub mcap_ceiling_triggered: bool,
    pub trailing_active: bool,
    /// Blocks duplicate full-close signals while the sell tx is in-flight.
    pub is_closing: bool,
    /// Blocks a second partial sell from being queued before the first one executes.
    pub pending_partial_sell: bool,
    pub entry_time: u64,
    /// Cumulative SOL returned across all partial and final sells.
    pub total_returned: f64,
    /// Actual SOL we spent to enter (used by strategy controller and dev/wallet ranking).
    pub spent_sol: f64,
    /// Token developer at entry time (None for legacy callers).
    pub dev_address: Option<Address>,
    /// Snapshot of early buyers, captured at scoring time. Used on close to
    /// credit/debit the smart-money registry.
    pub early_buyers: Vec<Address>,
    /// Self-learning: scoring-time feature snapshot (logged on full close).
    pub learning_snapshot: Option<LearningTradeSnapshot>,
    /// Why the position was opened (dashboard OPEN / HTTP snapshot).
    pub open_reason: Option<OpenReason>,
    // --- Adaptive time-kill (V3): entry snapshot + short mcap derivative ----------
    pub tk_entry_buyers: u64,
    pub tk_entry_smart: u32,
    pub tk_entry_b2s: f64,
    pub tk_last_secs: u64,
    pub tk_last_mcap: f64,
    pub tk_prev_secs: u64,
    pub tk_prev_mcap: f64,
    /// Last adaptive time-kill tier label: weak | neutral | strong | fixed.
    pub last_time_kill_tier: String,
    pub last_time_kill_after_secs: u64,
    // --- Exit Engine V4 -------------------------------------------------------
    pub exit_profile: ExitProfile,
    pub hold_mode: bool,
    pub exit_phase: PositionPhase,
    pub live_score: i32,
    pub live_prev_velocity: f64,
    pub live_buyers_per_sec: f64,
    pub live_prev_buyers_per_sec: f64,
    pub last_live_score_at: u64,
    /// Minimum live score from entry snapshot (`score_total / 2`).
    pub live_score_entry_floor: i32,
    pub peak_profit_pct: f64,
    pub live_tape_prev: Option<EarlyTapePoint>,
    pub live_tape_curr: Option<EarlyTapePoint>,
    /// Recent raw bonding-curve mcaps (100ms ticks) for median / outlier filter on exit.
    pub exit_mcap_ticks: Vec<f64>,
    /// Consecutive ticks with filtered PnL at or below `exit_profit_floor` (SL confirm).
    pub sl_below_floor_streak: u8,
}

impl Position {
    /// `entry_mcap_fill_sol`: when `Some` (e.g. RPC snapshot right after a pump
    /// buy lands), use as `enter_mcap` instead of `pool.market_cap()` so entry
    /// matches the fill, not a possibly newer WS pool tick.
    pub fn new(
        pool: Box<dyn Pool>,
        buy_amount: Amount,
        current_time: u64,
        entry_mcap_fill_sol: Option<f64>,
    ) -> Self {
        let enter_mcap = if let Some(m) = entry_mcap_fill_sol.filter(|m| *m > 0.0) {
            Amount::from_float_native(m)
        } else {
            pool.market_cap().amount()
        };
        Self {
            highest_mcap: enter_mcap.to_float(),
            enter_mcap,
            pool,
            initial_holdings: buy_amount,
            holdings: buy_amount,
            exit_profit_floor: -16.0,
            tp1_triggered: false,
            tp2_triggered: false,
            tp3_triggered: false,
            tp4_triggered: false,
            tp5_triggered: false,
            mcap_ceiling_triggered: false,
            trailing_active: false,
            is_closing: false,
            pending_partial_sell: false,
            entry_time: current_time,
            total_returned: 0.0,
            spent_sol: 0.0,
            dev_address: None,
            early_buyers: Vec::new(),
            learning_snapshot: None,
            open_reason: None,
            tk_entry_buyers: 0,
            tk_entry_smart: 0,
            tk_entry_b2s: 0.0,
            tk_last_secs: 0,
            tk_last_mcap: 0.0,
            tk_prev_secs: 0,
            tk_prev_mcap: 0.0,
            last_time_kill_tier: String::new(),
            last_time_kill_after_secs: 0,
            exit_profile: ExitProfile::Neutral,
            hold_mode: false,
            exit_phase: PositionPhase::Exploration,
            live_score: 0,
            live_prev_velocity: 0.0,
            live_buyers_per_sec: 0.0,
            live_prev_buyers_per_sec: 0.0,
            last_live_score_at: 0,
            live_score_entry_floor: 1,
            peak_profit_pct: 0.0,
            live_tape_prev: None,
            live_tape_curr: None,
            exit_mcap_ticks: Vec::new(),
            sl_below_floor_streak: 0,
        }
    }

    pub fn push_exit_mcap_tick(&mut self, raw_mcap: f64, max_ticks: usize) {
        if crate::autobuy::exit_engine::exit_mcap_valid(raw_mcap) {
            self.exit_mcap_ticks.push(raw_mcap);
            if self.exit_mcap_ticks.len() > max_ticks {
                let drop = self.exit_mcap_ticks.len() - max_ticks;
                self.exit_mcap_ticks.drain(0..drop);
            }
        }
    }

    pub fn pnl(&self) -> f64 {
        self.pnl_at_mcap(self.pool.market_cap().amount().to_float())
    }

    pub fn pnl_at_mcap(&self, mcap: f64) -> f64 {
        let enter_mcap = self.enter_mcap.to_float();
        if enter_mcap == 0.0 {
            return 0.0;
        }
        (mcap / enter_mcap - 1.0) * 100.0
    }

    /// PnL from median-filtered mcap tape (guards SL / trailing against single bad ticks).
    pub fn pnl_filtered(&self, band_low: f64, band_high: f64) -> f64 {
        let enter = self.enter_mcap.to_float();
        let mcap = crate::autobuy::exit_engine::filtered_exit_mcap(
            &self.exit_mcap_ticks,
            enter,
            band_low,
            band_high,
        );
        self.pnl_at_mcap(mcap)
    }

    pub fn filtered_market_cap(&self, band_low: f64, band_high: f64) -> f64 {
        crate::autobuy::exit_engine::filtered_exit_mcap(
            &self.exit_mcap_ticks,
            self.enter_mcap.to_float(),
            band_low,
            band_high,
        )
    }

    /// Advance second-resolution mcap samples for adaptive time-kill velocity.
    pub fn time_kill_note_mcap_sample(&mut self, wall_secs: u64, mcap: f64) {
        if self.tk_last_secs == 0 {
            self.tk_last_secs = wall_secs;
            self.tk_last_mcap = mcap;
            self.tk_prev_secs = wall_secs;
            self.tk_prev_mcap = mcap;
            return;
        }
        if wall_secs == self.tk_last_secs {
            self.tk_last_mcap = mcap;
            return;
        }
        self.tk_prev_secs = self.tk_last_secs;
        self.tk_prev_mcap = self.tk_last_mcap;
        self.tk_last_secs = wall_secs;
        self.tk_last_mcap = mcap;
    }

    /// % move of bonding mcap **per second**, normalized by entry mcap (≈ PnL%/s scale).
    pub fn time_kill_mcap_velocity_pct_per_sec(&self, enter_mcap: f64) -> f64 {
        if enter_mcap <= 0.0 {
            return 0.0;
        }
        let dt = self
            .tk_last_secs
            .saturating_sub(self.tk_prev_secs)
            .max(1) as f64;
        let dm = self.tk_last_mcap - self.tk_prev_mcap;
        (dm / enter_mcap) * 100.0 / dt
    }
}
