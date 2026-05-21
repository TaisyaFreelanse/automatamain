//! Adaptive Exit Engine V4 — full doc architecture:
//! Exploration → Momentum → Expansion → Distribution, live score, profiles,
//! HOLD/RUNNER, adaptive trailing, momentum decay, profit staircase, re-expansion, moonbag.

use serde::{Deserialize, Serialize};

use crate::{
    autobuy::positions::Position,
    learning::LearningTradeSnapshot,
    scoring::{
        live_position::LivePositionSnapshot,
    },
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

/// Position lifecycle state (doc §2); transitions driven by live score + decay.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PositionPhase {
    #[default]
    Exploration,
    Momentum,
    Expansion,
    Distribution,
}

impl PositionPhase {
    pub fn as_str(self) -> &'static str {
        match self {
            PositionPhase::Exploration => "exploration",
            PositionPhase::Momentum => "momentum",
            PositionPhase::Expansion => "expansion",
            PositionPhase::Distribution => "distribution",
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
    /// No V4 decay exit and no lite-score phase downgrade for this long after entry.
    #[serde(default = "default_exit_grace_secs")]
    pub exit_grace_secs: u64,
    #[serde(default = "default_profile_weak")]
    pub weak: ExitTpProfile,
    #[serde(default = "default_profile_neutral")]
    pub neutral: ExitTpProfile,
    #[serde(default = "default_profile_strong")]
    pub strong: ExitTpProfile,
    #[serde(default = "default_profile_runner")]
    pub runner: ExitTpProfile,
    /// HOLD MODE (doc §4).
    #[serde(default = "default_hold_min_live_score")]
    pub hold_min_live_score: i32,
    #[serde(default = "default_hold_min_b2s")]
    pub hold_min_b2s: f64,
    #[serde(default = "default_hold_min_smart")]
    pub hold_min_smart: u32,
    #[serde(default = "default_hold_max_bundle_similar")]
    pub hold_max_bundle_similar: f64,
    #[serde(default = "default_hold_momentum_overheated_pct")]
    pub hold_momentum_overheated_pct: f64,
    /// FSM thresholds (live score).
    #[serde(default = "default_phase_momentum_min")]
    pub phase_momentum_min_score: i32,
    #[serde(default = "default_phase_expansion_min")]
    pub phase_expansion_min_score: i32,
    #[serde(default = "default_phase_distribution_max")]
    pub phase_distribution_max_score: i32,
    /// Consecutive ticks with a real momentum/volume decay signal before full exit.
    #[serde(default = "default_momentum_decay_confirm_ticks")]
    pub momentum_decay_confirm_ticks: u8,
    /// Strong tier: skip time-kill if any TP fired and PnL is above this (%).
    #[serde(default = "default_strong_time_kill_min_after_tp")]
    pub strong_time_kill_min_profit_after_tp: f64,
    #[serde(default = "default_true")]
    pub momentum_decay_exit_enabled: bool,
    #[serde(default = "default_true")]
    pub profit_staircase_enabled: bool,
    #[serde(default = "default_true")]
    pub runtime_runner_upgrade_enabled: bool,
    #[serde(default = "default_runner_upgrade_min_live_score")]
    pub runner_upgrade_min_live_score: i32,
    #[serde(default = "default_true")]
    pub re_expansion_enabled: bool,
    #[serde(default = "default_re_expansion_min_score")]
    pub re_expansion_min_score: i32,
    #[serde(default = "default_re_expansion_min_profit_pct")]
    pub re_expansion_min_profit_pct: f64,
    #[serde(default = "default_true")]
    pub adaptive_moonbag_enabled: bool,
    #[serde(default = "default_bundle_tolerance")]
    pub bundle_similar_tolerance: f64,
}

fn default_true() -> bool {
    true
}

fn default_hold_min_live_score() -> i32 {
    13
}

fn default_phase_momentum_min() -> i32 {
    5
}

fn default_phase_expansion_min() -> i32 {
    10
}

fn default_phase_distribution_max() -> i32 {
    2
}

fn default_momentum_decay_confirm_ticks() -> u8 {
    2
}

fn default_runner_upgrade_min_live_score() -> i32 {
    11
}

fn default_re_expansion_min_score() -> i32 {
    8
}

fn default_re_expansion_min_profit_pct() -> f64 {
    20.0
}

fn default_bundle_tolerance() -> f64 {
    0.08
}

fn default_exit_v4_enabled() -> bool {
    true
}

fn default_live_score_refresh_secs() -> u64 {
    4
}

fn default_exit_grace_secs() -> u64 {
    3
}

/// Floor for in-position `live_score` (matches entry profile resolution).
pub fn entry_live_score_floor(snap: Option<&crate::learning::LearningTradeSnapshot>) -> i32 {
    snap.map(|s| (s.score_total / 2).max(1))
        .unwrap_or(1)
}

pub fn in_exit_grace_period(held_secs: u64, cfg: &ExitEngineV4Config) -> bool {
    held_secs < cfg.exit_grace_secs
}

/// Stop-loss grace: no SL (even confirmed ticks) until held this long after entry.
pub fn in_sl_grace_period(held_secs: u64, sl_grace_secs: u64) -> bool {
    held_secs < sl_grace_secs
}

const EXIT_MCAP_ABS_MAX: f64 = 200_000.0;

pub fn exit_mcap_valid(mcap: f64) -> bool {
    mcap.is_finite() && mcap > 0.0 && mcap <= EXIT_MCAP_ABS_MAX
}

pub fn exit_mcap_median(mcaps: &[f64]) -> Option<f64> {
    let mut v: Vec<f64> = mcaps.iter().copied().filter(|m| exit_mcap_valid(*m)).collect();
    if v.is_empty() {
        return None;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    Some(v[v.len() / 2])
}

/// Reject bonding-curve outliers vs tape median (same idea as dashboard chart filter).
pub fn exit_mcap_matches_band(mcap: f64, median: Option<f64>, band_low: f64, band_high: f64) -> bool {
    if !exit_mcap_valid(mcap) {
        return false;
    }
    let Some(med) = median else {
        return true;
    };
    if med <= 0.0 {
        return true;
    }
    mcap >= med * band_low && mcap <= med * band_high
}

/// Median of recent in-band ticks; falls back to median / last raw / `enter_mcap`.
pub fn filtered_exit_mcap(
    samples: &[f64],
    enter_mcap: f64,
    band_low: f64,
    band_high: f64,
) -> f64 {
    let median = exit_mcap_median(samples);
    let in_band: Vec<f64> = samples
        .iter()
        .copied()
        .filter(|m| exit_mcap_matches_band(*m, median, band_low, band_high))
        .collect();
    if let Some(m) = exit_mcap_median(&in_band) {
        return m;
    }
    if let Some(m) = median {
        return m;
    }
    if let Some(&last) = samples.iter().rev().find(|m| exit_mcap_valid(**m)) {
        return last;
    }
    enter_mcap.max(0.0)
}

fn default_hold_min_b2s() -> f64 {
    2.2
}

fn default_hold_min_smart() -> u32 {
    5
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
            exit_grace_secs: default_exit_grace_secs(),
            weak: default_profile_weak(),
            neutral: default_profile_neutral(),
            strong: default_profile_strong(),
            runner: default_profile_runner(),
            hold_min_live_score: default_hold_min_live_score(),
            hold_min_b2s: default_hold_min_b2s(),
            hold_min_smart: default_hold_min_smart(),
            hold_max_bundle_similar: default_hold_max_bundle_similar(),
            hold_momentum_overheated_pct: default_hold_momentum_overheated_pct(),
            phase_momentum_min_score: default_phase_momentum_min(),
            phase_expansion_min_score: default_phase_expansion_min(),
            phase_distribution_max_score: default_phase_distribution_max(),
            momentum_decay_confirm_ticks: default_momentum_decay_confirm_ticks(),
            strong_time_kill_min_profit_after_tp: default_strong_time_kill_min_after_tp(),
            momentum_decay_exit_enabled: default_true(),
            profit_staircase_enabled: default_true(),
            runtime_runner_upgrade_enabled: default_true(),
            runner_upgrade_min_live_score: default_runner_upgrade_min_live_score(),
            re_expansion_enabled: default_true(),
            re_expansion_min_score: default_re_expansion_min_score(),
            re_expansion_min_profit_pct: default_re_expansion_min_profit_pct(),
            adaptive_moonbag_enabled: default_true(),
            bundle_similar_tolerance: default_bundle_tolerance(),
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

/// In-position metrics (live TokenBucket tape when available).
#[derive(Clone, Debug, Default)]
pub struct LiveMetrics {
    pub buyers_per_sec: f64,
    pub smart_wallet_count: u32,
    pub buy_sell_ratio: f64,
    pub holder_growth_rate: f64,
    pub volume_delta: f64,
    pub liquidity_delta: f64,
    pub sell_pressure_score: f64,
    pub smart_wallet_exits: u32,
    pub momentum_decay: bool,
    pub volume_decay: bool,
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
    if metrics.liquidity_delta > 0.0 {
        score += 1;
    }
    if metrics.sell_pressure_score > 1.2 {
        score -= 2;
    }
    if metrics.smart_wallet_exits >= 2 {
        score -= 3;
    } else if metrics.smart_wallet_exits >= 1 {
        score -= 1;
    }
    if metrics.momentum_decay {
        score -= 4;
    }
    if metrics.volume_decay {
        score -= 2;
    }
    if metrics.bundle_detected {
        score -= 3;
    }
    score
}

pub fn live_metrics_from_snapshot(
    snap: &LivePositionSnapshot,
    buyers_per_sec_prev: f64,
    vel_now: f64,
    vel_prev: f64,
    entry_velocity_pct: f64,
    profit_pct: f64,
    cfg: &ExitEngineV4Config,
) -> LiveMetrics {
    let volume_decay = snap.volume_delta < 0.0 && vel_now < vel_prev * 0.85;
    let momentum_decay = momentum_decay_detected(
        snap.buyers_per_sec,
        buyers_per_sec_prev,
        snap.sell_pressure_score,
        snap.volume_delta,
    ) || (vel_prev > 0.01
        && vel_now < vel_prev * 0.7
        && snap.sell_pressure_score > 1.2
        && snap.volume_delta < 0.0);
    let bundle_detected = snap.bundle_similar > cfg.hold_max_bundle_similar
        || snap.bundle_identical > 0.2;
    LiveMetrics {
        buyers_per_sec: snap.buyers_per_sec,
        smart_wallet_count: snap.smart_wallet_count,
        buy_sell_ratio: snap.buy_sell_ratio,
        holder_growth_rate: snap.holder_growth_rate,
        volume_delta: snap.volume_delta,
        liquidity_delta: snap.liquidity_delta,
        sell_pressure_score: snap.sell_pressure_score,
        smart_wallet_exits: snap.smart_wallet_exits,
        momentum_decay,
        volume_decay,
        bundle_detected,
        momentum_overheated: entry_velocity_pct >= cfg.hold_momentum_overheated_pct
            || profit_pct >= 120.0,
    }
}

/// Fallback when TokenBucket is not yet in cache (mcap velocity + entry snapshot).
pub fn live_metrics_lite(
    pos: &Position,
    vel_pct_per_sec: f64,
    profit_pct: f64,
    snap: Option<&LearningTradeSnapshot>,
    cfg: &ExitEngineV4Config,
) -> LiveMetrics {
    let sell_pressure = snap.map(|s| s.sell_pressure_score).unwrap_or(0.0);
    let entry_vel = snap.map(|s| s.velocity_pct).unwrap_or(0.0);
    let bundle_similar = snap.map(|s| s.bundle_similar).unwrap_or(0.0);
    let bundle_identical = snap.map(|s| s.bundle_identical).unwrap_or(0.0);
    let smart_exits = snap.map(|s| s.smart_wallet_early_exits).unwrap_or(0);
    let b2s = if pos.tk_entry_b2s > 0.0 {
        pos.tk_entry_b2s
    } else {
        snap.map(|s| s.buy_to_sell_ratio).unwrap_or(1.0)
    };
    let buyers_per_sec = (vel_pct_per_sec * 12.0).max(0.0)
        + if pos.tk_entry_buyers >= 35 { 2.5 } else { 0.0 };
    let volume_delta = vel_pct_per_sec;
    let momentum_decay = momentum_decay_detected(
        vel_pct_per_sec,
        pos.live_prev_velocity,
        sell_pressure,
        volume_delta,
    );
    LiveMetrics {
        buyers_per_sec,
        smart_wallet_count: pos.tk_entry_smart,
        buy_sell_ratio: b2s,
        holder_growth_rate: vel_pct_per_sec.max(0.0) * 20.0,
        volume_delta,
        liquidity_delta: vel_pct_per_sec,
        sell_pressure_score: sell_pressure,
        smart_wallet_exits: smart_exits,
        momentum_decay,
        volume_decay: volume_delta < 0.0,
        bundle_detected: bundle_similar > cfg.hold_max_bundle_similar || bundle_identical > 0.25,
        momentum_overheated: entry_vel >= cfg.hold_momentum_overheated_pct || profit_pct >= 120.0,
    }
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

/// Full-exit decay: needs tape-derived decay and/or explicit velocity/sell/volume collapse.
pub fn live_momentum_decay_for_exit(metrics: &LiveMetrics) -> bool {
    metrics.momentum_decay || metrics.volume_decay
}

/// Tick-level decay check using latest live snapshot fields (not entry snapshot sell pressure).
pub fn live_decay_signal_from_tape(
    buyers_per_sec: f64,
    prev_buyers_per_sec: f64,
    sell_pressure: f64,
    volume_delta: f64,
    metrics: &LiveMetrics,
) -> bool {
    live_momentum_decay_for_exit(metrics)
        || momentum_decay_detected(buyers_per_sec, prev_buyers_per_sec, sell_pressure, volume_delta)
}

/// Per live-tape refresh (and lite fallback): score, phase, decay inputs.
pub fn log_v4_live_snapshot(
    mint: &str,
    held_secs: u64,
    live_score: i32,
    live_floor: i32,
    phase: PositionPhase,
    profile: ExitProfile,
    hold_mode: bool,
    buyers_per_sec: f64,
    prev_buyers_per_sec: f64,
    sell_pressure: f64,
    volume_delta: f64,
    mcap_vel_pct_per_sec: f64,
    profit_pct: f64,
    metrics: &LiveMetrics,
    decay_streak: u8,
    decay_confirm_ticks: u8,
) {
    eprintln!(
        "[EXIT V4] {mint} held={held_secs}s live_score={live_score} floor={live_floor} \
         phase={} profile={} hold={hold_mode} bps={buyers_per_sec:.2} prev_bps={prev_buyers_per_sec:.2} \
         sell_press={sell_pressure:.3} vol_delta={volume_delta:.4} mcap_vel={mcap_vel_pct_per_sec:.3}%/s \
         profit={profit_pct:.1}% mom_decay={} vol_decay={} decay_streak={decay_streak}/{decay_confirm_ticks}",
        phase.as_str(),
        profile.as_str(),
        metrics.momentum_decay,
        metrics.volume_decay,
    );
}

pub fn should_enable_hold_mode(
    cfg: &ExitEngineV4Config,
    metrics: &LiveMetrics,
    live_score: i32,
) -> bool {
    live_score >= cfg.hold_min_live_score
        && metrics.smart_wallet_count >= cfg.hold_min_smart
        && metrics.buy_sell_ratio > cfg.hold_min_b2s
        && !metrics.bundle_detected
        && !metrics.momentum_overheated
}

/// Doc §6 adaptive trailing (weak 12–15%, strong 20–25%, runner 35–45%).
pub fn adaptive_trailing(profile: ExitProfile, live_score: i32, profit_pct: f64) -> f64 {
    let profile_floor = match profile {
        ExitProfile::Weak => 14.0,
        ExitProfile::Neutral => 18.0,
        ExitProfile::Strong => 22.0,
        ExitProfile::Runner => 38.0,
    };
    let doc: f64 = if live_score >= 15 {
        40.0
    } else if live_score >= 11 {
        24.0
    } else if profit_pct > 50.0 {
        18.0
    } else {
        14.0
    };
    doc.max(profile_floor).min(45.0)
}

/// Doc §8 profit lock staircase — minimum locked profit vs session peak PnL.
pub fn profit_lock_staircase_floor(peak_profit_pct: f64) -> Option<f64> {
    if peak_profit_pct >= 400.0 {
        Some(180.0)
    } else if peak_profit_pct >= 200.0 {
        Some(90.0)
    } else if peak_profit_pct >= 100.0 {
        Some(40.0)
    } else if peak_profit_pct >= 50.0 {
        Some(15.0)
    } else {
        None
    }
}

pub fn transition_position_phase(
    current: PositionPhase,
    live_score: i32,
    momentum_decay: bool,
    profit_pct: f64,
    cfg: &ExitEngineV4Config,
) -> PositionPhase {
    if cfg.re_expansion_enabled
        && current == PositionPhase::Distribution
        && live_score >= cfg.re_expansion_min_score
        && profit_pct >= cfg.re_expansion_min_profit_pct
    {
        return PositionPhase::Expansion;
    }
    if momentum_decay || live_score <= cfg.phase_distribution_max_score {
        return PositionPhase::Distribution;
    }
    if live_score >= cfg.phase_expansion_min_score {
        return PositionPhase::Expansion;
    }
    if live_score >= cfg.phase_momentum_min_score {
        return PositionPhase::Momentum;
    }
    PositionPhase::Exploration
}

/// Phase adjusts TP/trailing behaviour on top of exit profile.
pub fn apply_phase_to_tp(mut base: ExitTpProfile, phase: PositionPhase) -> ExitTpProfile {
    match phase {
        PositionPhase::Exploration => {}
        PositionPhase::Momentum => {
            base.trailing_activate_profit_pct *= 1.05;
        }
        PositionPhase::Expansion => {
            base.trailing_stop_drawdown_pct = (base.trailing_stop_drawdown_pct * 1.12).min(45.0);
            base.tp3_sell_pct *= 0.85;
            base.tp4_sell_pct *= 0.85;
            base.tp5_sell_pct *= 0.85;
        }
        PositionPhase::Distribution => {
            base.tp1_sell_pct = (base.tp1_sell_pct * 1.2).min(55.0);
            base.tp2_sell_pct = (base.tp2_sell_pct * 1.15).min(50.0);
            base.trailing_activate_profit_pct *= 0.85;
        }
    }
    base
}

/// Doc §9 adaptive moonbag — smaller partial sells on TP3+ when tape is strong.
pub fn adaptive_moonbag_sell_pct(base_sell: f64, live_score: i32, phase: PositionPhase) -> f64 {
    if !matches!(phase, PositionPhase::Expansion | PositionPhase::Momentum) {
        return base_sell;
    }
    if live_score >= 12 {
        base_sell * 0.5
    } else if live_score >= 9 {
        base_sell * 0.75
    } else {
        base_sell
    }
}

/// ML / tier bias at entry (uses scoring tier string).
pub fn ml_profile_bias(tier: &str, base: ExitProfile) -> ExitProfile {
    let t = tier.to_ascii_lowercase();
    if t.contains("aplus") || t.contains("a+") {
        if base == ExitProfile::Weak {
            ExitProfile::Strong
        } else {
            ExitProfile::Runner
        }
    } else if t == "a" {
        if matches!(base, ExitProfile::Weak | ExitProfile::Neutral) {
            ExitProfile::Strong
        } else {
            base
        }
    } else {
        base
    }
}

pub fn maybe_upgrade_runner(
    cfg: &ExitEngineV4Config,
    current: ExitProfile,
    metrics: &LiveMetrics,
    live_score: i32,
    hold_already: bool,
) -> (ExitProfile, bool) {
    if !cfg.runtime_runner_upgrade_enabled || hold_already {
        return (current, hold_already);
    }
    if live_score >= cfg.runner_upgrade_min_live_score
        && should_enable_hold_mode(cfg, metrics, live_score)
        && matches!(current, ExitProfile::Strong | ExitProfile::Neutral)
    {
        return (ExitProfile::Runner, true);
    }
    (current, hold_already)
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

    let metrics = live_metrics_lite(pos, 0.0, 0.0, snap, v4);
    let live_score = calculate_live_score(&metrics).max(entry_score / 2);
    let hold = should_enable_hold_mode(v4, &metrics, live_score);

    let tier = snap.map(|s| s.tier.as_str()).unwrap_or("");
    let biased = ml_profile_bias(tier, base);
    let profile = if hold && matches!(biased, ExitProfile::Strong | ExitProfile::Neutral) {
        ExitProfile::Runner
    } else {
        biased
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
        let t = adaptive_trailing(ExitProfile::Weak, 15, 10.0);
        assert!(t <= 45.0);
        assert!(t >= 14.0);
    }

    #[test]
    fn profit_staircase() {
        assert_eq!(profit_lock_staircase_floor(55.0), Some(15.0));
        assert_eq!(profit_lock_staircase_floor(120.0), Some(40.0));
    }

    #[test]
    fn distribution_phase_needs_decay_or_very_low_score() {
        let cfg = ExitEngineV4Config {
            phase_distribution_max_score: 2,
            ..ExitEngineV4Config::default()
        };
        assert_eq!(
            transition_position_phase(
                PositionPhase::Exploration,
                3,
                false,
                5.0,
                &cfg,
            ),
            PositionPhase::Exploration
        );
        assert_eq!(
            transition_position_phase(
                PositionPhase::Exploration,
                2,
                false,
                5.0,
                &cfg,
            ),
            PositionPhase::Distribution
        );
        assert_eq!(
            transition_position_phase(
                PositionPhase::Exploration,
                8,
                true,
                5.0,
                &cfg,
            ),
            PositionPhase::Distribution
        );
    }

    #[test]
    fn filtered_exit_mcap_rejects_stale_low_tick() {
        // Entry ~74; one stale ~59 tick must not dominate median.
        let samples = [73.5, 74.0, 59.0, 73.8, 74.2];
        let filtered = filtered_exit_mcap(&samples, 73.67, 0.02, 50.0);
        assert!(filtered > 65.0, "filtered was {filtered}");
    }
}
