//! Selection-quality stack: replaces the single hard CreatorStatisticsFilter
//! with an A+/A/SKIP score engine, anti-bundle (similar-cluster) detection, smart-money
//! tracking, dynamic dev ranking, and a strategy controller (daily caps,
//! loss-streak pause, regime pause, max open positions).
//!
//! All of these run *before* `InitiateBuy` (or, for the strategy controller,
//! at the gate of `InitiateBuy` inside the position manager).
//!
//! Nothing in this module touches the broker / execution / UI layer.

pub mod anti_bundle;
pub mod anti_rug;
pub mod config;
pub mod dev_ranker;
pub mod features;
pub mod live_position;
pub mod score_engine;
pub mod smart_money;
pub mod strategy_controller;
