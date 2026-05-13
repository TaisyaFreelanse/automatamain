//! Risk gate. Lives inside `PositionManagerActor` and decides whether a
//! buy is allowed *right now*, based on:
//!
//! - max_open_positions (hard cap)
//! - daily trade budget
//! - daily loss cap (negative SOL)
//! - daily profit lock (positive SOL — stop after target)
//! - loss streak pause
//! - market regime pause (rolling window of last N closed trades)
//!
//! Day rolls over by UTC date.

use serde::Serialize;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::scoring::config::StrategyConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum BuyDecision {
    Allow,
    BlockMaxOpen,
    BlockDailyTrades,
    BlockDailyLoss,
    BlockProfitLock,
    BlockLossStreak,
    BlockRegime,
}

#[derive(Clone, Debug, Serialize)]
pub struct StrategySnapshot {
    pub day_unix: u64,
    pub daily_trades: u32,
    pub daily_pnl_sol: f64,
    pub loss_streak: u32,
    pub paused_until_unix: u64,
    pub recent_outcomes_window: u32,
    pub recent_losses: u32,
    pub last_block_reason: Option<String>,
}

pub struct StrategyController {
    cfg: StrategyConfig,
    day_unix: u64,
    daily_trades: u32,
    daily_pnl_sol: f64,
    loss_streak: u32,
    paused_until_unix: u64,
    recent_outcomes: VecDeque<bool>, // true = win
    last_block_reason: Option<String>,
}

impl StrategyController {
    pub fn new(cfg: StrategyConfig) -> Self {
        Self {
            cfg,
            day_unix: current_day(),
            daily_trades: 0,
            daily_pnl_sol: 0.0,
            loss_streak: 0,
            paused_until_unix: 0,
            recent_outcomes: VecDeque::new(),
            last_block_reason: None,
        }
    }

    pub fn cfg(&self) -> &StrategyConfig {
        &self.cfg
    }

    /// Roll the daily counters when UTC date changes. Pause windows persist
    /// across days deliberately (a 30-min pause shouldn't be erased at
    /// midnight).
    fn roll_day_if_needed(&mut self) {
        let today = current_day();
        if today != self.day_unix {
            self.day_unix = today;
            self.daily_trades = 0;
            self.daily_pnl_sol = 0.0;
            // Loss streak intentionally NOT reset here — a fresh day after
            // 5 consecutive losses still smells bad.
        }
    }

    pub fn can_open(&mut self, currently_open: u32) -> BuyDecision {
        self.roll_day_if_needed();
        let now = unix_now();

        if currently_open >= self.cfg.max_open_positions {
            self.last_block_reason = Some("max_open".into());
            return BuyDecision::BlockMaxOpen;
        }
        if self.daily_trades >= self.cfg.max_trades_per_day {
            self.last_block_reason = Some("daily_trades".into());
            return BuyDecision::BlockDailyTrades;
        }
        if self.daily_pnl_sol <= self.cfg.max_daily_loss_sol {
            self.last_block_reason = Some("daily_loss".into());
            return BuyDecision::BlockDailyLoss;
        }
        if self.daily_pnl_sol >= self.cfg.daily_profit_lock_sol {
            self.last_block_reason = Some("profit_lock".into());
            return BuyDecision::BlockProfitLock;
        }
        if now < self.paused_until_unix {
            self.last_block_reason = Some("paused".into());
            // Distinguish loss-streak vs regime in last_block_reason
            // (we don't carry the cause separately here, so just call it
            // BlockLossStreak — UI can read paused_until from the snapshot).
            return BuyDecision::BlockLossStreak;
        }
        BuyDecision::Allow
    }

    /// Notify a position has just been opened. Counts toward daily trade
    /// budget *at open time* so a single token can't burn the whole budget
    /// while waiting for closure.
    pub fn note_position_opened(&mut self) {
        self.roll_day_if_needed();
        self.daily_trades += 1;
    }

    /// Closed trade outcome. `pnl_sol` is the realized PnL of the *whole*
    /// position in SOL.
    pub fn note_position_closed(&mut self, pnl_sol: f64) {
        self.roll_day_if_needed();
        self.daily_pnl_sol += pnl_sol;

        let win = pnl_sol > 0.0;
        if win {
            self.loss_streak = 0;
        } else {
            self.loss_streak += 1;
            if self.loss_streak >= self.cfg.loss_streak_limit {
                let now = unix_now();
                self.paused_until_unix = now + self.cfg.loss_streak_pause_secs;
                eprintln!(
                    "[strategy] loss streak {} → pause until {}",
                    self.loss_streak, self.paused_until_unix
                );
            }
        }

        // Rolling window for regime detection.
        self.recent_outcomes.push_back(win);
        let window = self.cfg.market_regime_window as usize;
        while self.recent_outcomes.len() > window {
            self.recent_outcomes.pop_front();
        }
        if self.cfg.market_regime_pause
            && self.recent_outcomes.len() >= window.max(1)
        {
            let losses = self.recent_outcomes.iter().filter(|w| !**w).count() as f64;
            let ratio = losses / self.recent_outcomes.len() as f64;
            if ratio >= self.cfg.market_regime_loss_ratio {
                let now = unix_now();
                let until = now + self.cfg.market_regime_pause_secs;
                if until > self.paused_until_unix {
                    self.paused_until_unix = until;
                    eprintln!(
                        "[strategy] regime: {}/{} losses → pause until {}",
                        losses as u32,
                        self.recent_outcomes.len(),
                        until
                    );
                }
            }
        }
    }

    pub fn snapshot(&self) -> StrategySnapshot {
        let losses = self.recent_outcomes.iter().filter(|w| !**w).count() as u32;
        StrategySnapshot {
            day_unix: self.day_unix,
            daily_trades: self.daily_trades,
            daily_pnl_sol: self.daily_pnl_sol,
            loss_streak: self.loss_streak,
            paused_until_unix: self.paused_until_unix,
            recent_outcomes_window: self.recent_outcomes.len() as u32,
            recent_losses: losses,
            last_block_reason: self.last_block_reason.clone(),
        }
    }
}

fn current_day() -> u64 {
    let secs = unix_now();
    secs - (secs % 86_400)
}

fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}
