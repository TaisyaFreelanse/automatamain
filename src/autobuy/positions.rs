use solana_address::Address;

use crate::{
    generalize::general_pool::Pool,
    helper::Amount,
    learning::LearningTradeSnapshot,
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
            trailing_active: false,
            is_closing: false,
            pending_partial_sell: false,
            entry_time: current_time,
            total_returned: 0.0,
            spent_sol: 0.0,
            dev_address: None,
            early_buyers: Vec::new(),
            learning_snapshot: None,
            tk_entry_buyers: 0,
            tk_entry_smart: 0,
            tk_entry_b2s: 0.0,
            tk_last_secs: 0,
            tk_last_mcap: 0.0,
            tk_prev_secs: 0,
            tk_prev_mcap: 0.0,
            last_time_kill_tier: String::new(),
            last_time_kill_after_secs: 0,
        }
    }

    pub fn pnl(&self) -> f64 {
        let enter_mcap = self.enter_mcap.to_float();
        if enter_mcap == 0.0 {
            return 0.0;
        }
        (self.pool.market_cap().amount().to_float() / enter_mcap - 1.0) * 100.0
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
