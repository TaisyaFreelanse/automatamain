//! Adaptive Exit Engine V4 — Phase 1: entry profiles, lite live score, hold/runner,
//! adaptive trailing. Phase 2+ (momentum-decay full exit, profit staircase) behind flags.

use serde::{Deserialize, Serialize};

use crate::{
    autobuy::positions::Position,
    learning::LearningTradeSnapshot,
};

/// Entry snapshot thresholds (mirrors `SmartBuyConfig` time-kill fields).
#[derive(Clone, Copy, Debug)]
pub struct TkEntryThresholds {
    pub strong_min_buyers: u64,
    pub strong_min_b2s: f64,
    pub weak_max_buyers: u64,
    pub weak_max_b2s: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExitProfile {
    Weak,
    Neutral,
    Strong,
    Runner,
}

impl ExitProfile {
    pub fn as_str(self) -> &'static str {
        match self {
            ExitProfile::Weak => "weak",
            ExitProfile::Neutral => "neutral",
            ExitProfile::Strong => "strong",
            ExitProfile::Runner => "runner",
        }
    }
}

/// Per-profile TP / trailing parameters (V4 doc tables, calibrated to live tape).
/// YAML may override a subset; omitted fields fall back to [`default_profile_weak`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExitTpProfile {
    #[serde(default = "def_tpprof_tp1_pct")]
    pub tp1_pct: f64,
    #[serde(default = "def_tpprof_tp1_sell_pct")]
    pub tp1_sell_pct: f64,
    #[serde(default = "def_tpprof_tp2_pct")]
    pub tp2_pct: f64,
    #[serde(default = "def_tpprof_tp2_sell_pct")]
    pub tp2_sell_pct: f64,
    #[serde(default = "def_tpprof_tp3_pct")]
    pub tp3_pct: f64,
    #[serde(default = "def_tpprof_tp3_sell_pct")]
    pub tp3_sell_pct: f64,
    #[serde(default = "def_tpprof_tp4_pct")]
    pub tp4_pct: f64,
    #[serde(default = "def_tpprof_tp4_sell_pct")]
    pub tp4_sell_pct: f64,
    #[serde(default = "def_tpprof_tp5_pct")]
    pub tp5_pct: f64,
    #[serde(default = "def_tpprof_tp5_sell_pct")]
    pub tp5_sell_pct: f64,
    #[serde(default = "def_tpprof_trailing_stop_drawdown_pct")]
    pub trailing_stop_drawdown_pct: f64,
    #[serde(default = "def_tpprof_trailing_activate_profit_pct")]
    pub trailing_activate_profit_pct: f64,
    #[serde(default = "def_tpprof_trailing_floor_profit_pct")]
    pub trailing_floor_profit_pct: f64,
    #[serde(default = "def_tpprof_smart_stop_activate_profit_pct")]
    pub smart_stop_activate_profit_pct: f64,
    #[serde(default = "def_tpprof_smart_stop_floor_profit_pct")]
    pub smart_stop_floor_profit_pct: f64,
}

impl Default for ExitTpProfile {
    fn default() -> Self {
        default_profile_weak()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ExitEngineV4Config {
    #[serde(default = "default_exit_v4_enabled")]
    pub enabled: bool,
    /// Recompute lite live score at most every N seconds per position.
    #[serde(default = "default_live_score_refresh_secs")]
    pub live_score_refresh_secs: u64,
    #[serde(default = "default_profile_weak")]
    pub weak: ExitTpProfile,
    #[serde(default = "default_profile_neutral")]
    pub neutral: ExitTpProfile,
    #[serde(default = "default_profile_strong")]
    pub strong: ExitTpProfile,
    #[serde(default = "default_profile_runner")]
    pub runner: ExitTpProfile,
    /// HOLD / runner: minimum entry score_total (doc 13 is too high for our scale).
    #[serde(default = "default_hold_min_entry_score")]
    pub hold_min_entry_score: i32,
    #[serde(default = "default_hold_min_b2s")]
    pub hold_min_b2s: f64,
    #[serde(default = "default_hold_min_smart")]
    pub hold_min_smart: u32,
    #[serde(default = "default_hold_max_bundle_similar")]
    pub hold_max_bundle_similar: f64,
    #[serde(default = "default_hold_momentum_overheated_pct")]
    pub hold_momentum_overheated_pct: f64,
    /// Strong tier: skip time-kill if any TP fired and PnL is above this (%).
    #[serde(default = "default_strong_time_kill_min_after_tp")]
    pub strong_time_kill_min_profit_after_tp: f64,
    /// Phase 2: full exit on momentum decay (off in Phase 1).
    #[serde(default)]
    pub momentum_decay_exit_enabled: bool,
}

fn default_exit_v4_enabled() -> bool {
    true
}

fn default_live_score_refresh_secs() -> u64 {
    4
}

fn default_hold_min_entry_score() -> i32 {
    7
}

fn default_hold_min_b2s() -> f64 {
    2.0
}

fn default_hold_min_smart() -> u32 {
    3
}

fn default_hold_max_bundle_similar() -> f64 {
    0.35
}

fn default_hold_momentum_overheated_pct() -> f64 {
    55.0
}

fn default_strong_time_kill_min_after_tp() -> f64 {
    8.0
}

fn default_profile_weak() -> ExitTpProfile {
    ExitTpProfile {
        tp1_pct: 30.0,
        tp1_sell_pct: 40.0,
        tp2_pct: 50.0,
        tp2_sell_pct: 40.0,
        tp3_pct: 100.0,
        tp3_sell_pct: 15.0,
        tp4_pct: 150.0,
        tp4_sell_pct: 10.0,
        tp5_pct: 200.0,
        tp5_sell_pct: 10.0,
        trailing_stop_drawdown_pct: 14.0,
        trailing_activate_profit_pct: 50.0,
        trailing_floor_profit_pct: 15.0,
        smart_stop_activate_profit_pct: 50.0,
        smart_stop_floor_profit_pct: 5.0,
    }
}

fn def_tpprof_tp1_pct() -> f64 {
    default_profile_weak().tp1_pct
}
fn def_tpprof_tp1_sell_pct() -> f64 {
    default_profile_weak().tp1_sell_pct
}
fn def_tpprof_tp2_pct() -> f64 {
    default_profile_weak().tp2_pct
}
fn def_tpprof_tp2_sell_pct() -> f64 {
    default_profile_weak().tp2_sell_pct
}
fn def_tpprof_tp3_pct() -> f64 {
    default_profile_weak().tp3_pct
}
fn def_tpprof_tp3_sell_pct() -> f64 {
    default_profile_weak().tp3_sell_pct
}
fn def_tpprof_tp4_pct() -> f64 {
    default_profile_weak().tp4_pct
}
fn def_tpprof_tp4_sell_pct() -> f64 {
    default_profile_weak().tp4_sell_pct
}
fn def_tpprof_tp5_pct() -> f64 {
    default_profile_weak().tp5_pct
}
fn def_tpprof_tp5_sell_pct() -> f64 {
    default_profile_weak().tp5_sell_pct
}
fn def_tpprof_trailing_stop_drawdown_pct() -> f64 {
    default_profile_weak().trailing_stop_drawdown_pct
}
fn def_tpprof_trailing_activate_profit_pct() -> f64 {
    default_profile_weak().trailing_activate_profit_pct
}
fn def_tpprof_trailing_floor_profit_pct() -> f64 {
    default_profile_weak().trailing_floor_profit_pct
}
fn def_tpprof_smart_stop_activate_profit_pct() -> f64 {
    default_profile_weak().smart_stop_activate_profit_pct
}
fn def_tpprof_smart_stop_floor_profit_pct() -> f64 {
    default_profile_weak().smart_stop_floor_profit_pct
}

fn default_profile_neutral() -> ExitTpProfile {
    ExitTpProfile {
        tp1_pct: 30.0,
        tp1_sell_pct: 35.0,
        tp2_pct: 50.0,
        tp2_sell_pct: 25.0,
        tp3_pct: 100.0,
        tp3_sell_pct: 12.0,
        tp4_pct: 150.0,
        tp4_sell_pct: 10.0,
        tp5_pct: 200.0,
        tp5_sell_pct: 10.0,
        trailing_stop_drawdown_pct: 20.0,
        trailing_activate_profit_pct: 60.0,
        trailing_floor_profit_pct: 25.0,
        smart_stop_activate_profit_pct: 50.0,
        smart_stop_floor_profit_pct: 5.0,
    }
}

fn default_profile_strong() -> ExitTpProfile {
    ExitTpProfile {
        tp1_pct: 30.0,
        tp1_sell_pct: 30.0,
        tp2_pct: 50.0,
        tp2_sell_pct: 15.0,
        tp3_pct: 100.0,
        tp3_sell_pct: 10.0,
        tp4_pct: 150.0,
        tp4_sell_pct: 10.0,
        tp5_pct: 200.0,
        tp5_sell_pct: 10.0,
        trailing_stop_drawdown_pct: 26.0,
        trailing_activate_profit_pct: 60.0,
        trailing_floor_profit_pct: 35.0,
        smart_stop_activate_profit_pct: 50.0,
        smart_stop_floor_profit_pct: 5.0,
    }
}

fn default_profile_runner() -> ExitTpProfile {
    ExitTpProfile {
        tp1_pct: 50.0,
        tp1_sell_pct: 10.0,
        tp2_pct: 80.0,
        tp2_sell_pct: 10.0,
        tp3_pct: 150.0,
        tp3_sell_pct: 15.0,
        tp4_pct: 250.0,
        tp4_sell_pct: 12.0,
        tp5_pct: 350.0,
        tp5_sell_pct: 10.0,
        trailing_stop_drawdown_pct: 32.0,
        trailing_activate_profit_pct: 55.0,
        trailing_floor_profit_pct: 30.0,
        smart_stop_activate_profit_pct: 45.0,
        smart_stop_floor_profit_pct: 8.0,
    }
}

impl Default for ExitEngineV4Config {
    fn default() -> Self {
        Self {
            enabled: default_exit_v4_enabled(),
            live_score_refresh_secs: default_live_score_refresh_secs(),
            weak: default_profile_weak(),
            neutral: default_profile_neutral(),
            strong: default_profile_strong(),
            runner: default_profile_runner(),
            hold_min_entry_score: default_hold_min_entry_score(),
            hold_min_b2s: default_hold_min_b2s(),
            hold_min_smart: default_hold_min_smart(),
            hold_max_bundle_similar: default_hold_max_bundle_similar(),
            hold_momentum_overheated_pct: default_hold_momentum_overheated_pct(),
            strong_time_kill_min_profit_after_tp: default_strong_time_kill_min_after_tp(),
            momentum_decay_exit_enabled: false,
        }
    }
}

impl ExitEngineV4Config {
    pub fn profile_params(&self, profile: ExitProfile) -> &ExitTpProfile {
        match profile {
            ExitProfile::Weak => &self.weak,
            ExitProfile::Neutral => &self.neutral,
            ExitProfile::Strong => &self.strong,
            ExitProfile::Runner => &self.runner,
        }
    }
}

/// Lite in-position metrics (no TokenBucket — tape + mcap velocity).
#[derive(Clone, Debug, Default)]
pub struct LiveMetrics {
    pub buyers_per_sec: f64,
    pub smart_wallet_count: u32,
    pub buy_sell_ratio: f64,
    pub holder_growth_rate: f64,
    pub volume_delta: f64,
    pub momentum_decay: bool,
    pub bundle_detected: bool,
    pub momentum_overheated: bool,
}

pub fn calculate_live_score(metrics: &LiveMetrics) -> i32 {
    let mut score = 0;
    if metrics.buyers_per_sec > 4.0 {
        score += 2;
    }
    if metrics.smart_wallet_count >= 5 {
        score += 3;
    } else if metrics.smart_wallet_count >= 3 {
        score += 2;
    }
    if metrics.buy_sell_ratio > 2.0 {
        score += 2;
    } else if metrics.buy_sell_ratio > 1.5 {
        score += 1;
    }
    if metrics.holder_growth_rate > 1.5 {
        score += 2;
    }
    if metrics.volume_delta > 0.0 {
        score += 1;
    }
    if metrics.momentum_decay {
        score -= 4;
    }
    if metrics.bundle_detected {
        score -= 3;
    }
    score
}

pub fn momentum_decay_detected(
    vel_now: f64,
    vel_prev: f64,
    sell_pressure: f64,
    volume_delta: f64,
) -> bool {
    let vel_drop = vel_prev > 0.01 && vel_now < vel_prev * 0.7;
    vel_drop && sell_pressure > 1.2 && volume_delta < 0.0
}

pub fn should_enable_hold_mode(
    cfg: &ExitEngineV4Config,
    live_score: i32,
    smart_wallets: u32,
    buy_sell_ratio: f64,
    bundle_detected: bool,
    momentum_overheated: bool,
    entry_score: i32,
) -> bool {
    let score_gate = live_score >= 8 || entry_score >= cfg.hold_min_entry_score;
    score_gate
        && smart_wallets >= cfg.hold_min_smart
        && buy_sell_ratio >= cfg.hold_min_b2s
        && !bundle_detected
        && !momentum_overheated
}

/// Adaptive trailing % — capped by profile baseline, boosted for high live score.
pub fn adaptive_trailing(
    profile_base_pct: f64,
    live_score: i32,
    profit_pct: f64,
) -> f64 {
    let mut pct = profile_base_pct;
    if live_score >= 12 {
        pct = pct.max(30.0);
    } else if live_score >= 9 {
        pct = pct.max(24.0);
    } else if profit_pct > 50.0 {
        pct = pct.max(18.0);
    } else {
        pct = pct.max(14.0);
    }
    pct.min(42.0)
}

pub fn build_live_metrics(
    pos: &Position,
    vel_pct_per_sec: f64,
    profit_pct: f64,
    snap: Option<&LearningTradeSnapshot>,
) -> LiveMetrics {
    let smart = pos.tk_entry_smart;
    let b2s = if pos.tk_entry_b2s > 0.0 {
        pos.tk_entry_b2s
    } else {
        snap.map(|s| s.buy_to_sell_ratio).unwrap_or(1.0)
    };
    let sell_pressure = snap.map(|s| s.sell_pressure_score).unwrap_or(0.0);
    let bundle_similar = snap.map(|s| s.bundle_similar).unwrap_or(0.0);
    let bundle_identical = snap.map(|s| s.bundle_identical).unwrap_or(0.0);
    let entry_vel = snap.map(|s| s.velocity_pct).unwrap_or(0.0);

    let buyers_per_sec = (vel_pct_per_sec * 12.0).max(0.0)
        + if pos.tk_entry_buyers >= 35 { 2.5 } else { 0.0 };
    let holder_growth_rate = vel_pct_per_sec.max(0.0) * 20.0;
    let volume_delta = vel_pct_per_sec;
    let momentum_decay =
        momentum_decay_detected(vel_pct_per_sec, pos.live_prev_velocity, sell_pressure, volume_delta);
    let bundle_detected = bundle_similar > 0.4 || bundle_identical > 0.25;
    let momentum_overheated = entry_vel >= 55.0 || profit_pct >= 120.0;

    LiveMetrics {
        buyers_per_sec,
        smart_wallet_count: smart,
        buy_sell_ratio: b2s,
        holder_growth_rate,
        volume_delta,
        momentum_decay,
        bundle_detected,
        momentum_overheated,
    }
}

/// Entry-time profile from tk snapshot + optional hold upgrade.
pub fn resolve_entry_profile(
    tk: TkEntryThresholds,
    v4: &ExitEngineV4Config,
    pos: &Position,
    snap: Option<&LearningTradeSnapshot>,
) -> (ExitProfile, bool) {
    let entry_score = snap.map(|s| s.score_total).unwrap_or(0);
    let b2s = pos.tk_entry_b2s.max(snap.map(|s| s.buy_to_sell_ratio).unwrap_or(0.0));
    let smart = pos.tk_entry_smart.max(snap.map(|s| s.smart_wallet_count).unwrap_or(0));
    let bundle_similar = snap.map(|s| s.bundle_similar).unwrap_or(0.0);
    let bundle_identical = snap.map(|s| s.bundle_identical).unwrap_or(0.0);
    let entry_vel = snap.map(|s| s.velocity_pct).unwrap_or(0.0);

    let mut strong = 0u32;
    let mut weak = 0u32;
    if smart >= 1 {
        strong += 1;
    }
    if pos.tk_entry_buyers >= tk.strong_min_buyers {
        strong += 1;
    }
    if b2s >= tk.strong_min_b2s {
        strong += 1;
    }
    if pos.tk_entry_smart == 0 {
        weak += 1;
    }
    if pos.tk_entry_buyers > 0 && pos.tk_entry_buyers < tk.weak_max_buyers {
        weak += 1;
    }
    if b2s > 0.0 && b2s < tk.weak_max_b2s {
        weak += 1;
    }

    let base = if strong >= 2 && weak <= 1 {
        ExitProfile::Strong
    } else if weak >= 2 && strong <= 1 {
        ExitProfile::Weak
    } else {
        ExitProfile::Neutral
    };

    let bundle_detected =
        bundle_similar > v4.hold_max_bundle_similar || bundle_identical > 0.2;
    let momentum_overheated = entry_vel >= v4.hold_momentum_overheated_pct;
    let live_stub = LiveMetrics {
        smart_wallet_count: smart,
        buy_sell_ratio: b2s,
        buyers_per_sec: if pos.tk_entry_buyers >= 38 { 5.0 } else { 2.0 },
        holder_growth_rate: 2.0,
        volume_delta: 1.0,
        momentum_decay: false,
        bundle_detected,
        momentum_overheated,
    };
    let live_score = calculate_live_score(&live_stub);

    let hold = should_enable_hold_mode(
        v4,
        live_score,
        smart,
        b2s,
        bundle_detected,
        momentum_overheated,
        entry_score,
    );

    let profile = if hold && matches!(base, ExitProfile::Strong | ExitProfile::Neutral) {
        ExitProfile::Runner
    } else {
        base
    };

    (profile, hold)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_score_penalizes_decay() {
        let m = LiveMetrics {
            momentum_decay: true,
            buy_sell_ratio: 3.0,
            smart_wallet_count: 5,
            buyers_per_sec: 5.0,
            holder_growth_rate: 2.0,
            volume_delta: 1.0,
            ..Default::default()
        };
        let s = calculate_live_score(&m);
        assert!(s < calculate_live_score(&LiveMetrics {
            momentum_decay: false,
            ..m
        }));
    }

    #[test]
    fn adaptive_trailing_caps() {
        assert!(adaptive_trailing(14.0, 15, 10.0) <= 42.0);
        assert!(adaptive_trailing(14.0, 15, 10.0) >= 14.0);
    }
}
