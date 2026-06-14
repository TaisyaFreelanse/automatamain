use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[derive(Debug, serde::Serialize)]
pub struct FeedSnapshot {
    pub name: String,
    pub subscribed: u64,
    pub reconnects: u64,
    /// Reconnects forced by the idle watchdog (stream nominally connected but no
    /// created/trade event produced within the idle window). Subset of reconnects.
    pub idle_reconnects: u64,
    pub stream_errors: u64,

    pub messages: u64,
    pub events: u64,
    pub parse_errors: u64,
    pub err_logs: u64,
    pub bytes_in: u64,

    pub dropped_failed_tx: u64,
    pub dropped_no_program_data: u64,
    pub dropped_self_dup: u64,
    pub duplicates_cross_feed: u64,

    pub lines_total: u64,
    pub lines_program_data: u64,

    pub last_message_unix: u64,
    pub started_unix: u64,
    pub uptime_secs: u64,

    pub msg_per_sec_avg: f64,
    pub events_per_sec_avg: f64,
    pub bytes_per_sec_avg: f64,

    /// events / max(1, messages_processed_after_prefilter).
    /// >0.0 means messages we kept actually produced events.
    pub useful_msg_ratio: f64,
    /// Overall efficiency: events / messages received from Helius.
    /// Helps estimate how many incoming messages translate into anything we use.
    pub events_per_msg_total: f64,
}

pub struct FeedMetrics {
    pub name: &'static str,
    pub subscribed: AtomicU64,
    pub reconnects: AtomicU64,
    pub idle_reconnects: AtomicU64,
    pub stream_errors: AtomicU64,

    pub messages: AtomicU64,
    pub events: AtomicU64,
    pub parse_errors: AtomicU64,
    pub err_logs: AtomicU64,
    pub bytes_in: AtomicU64,

    pub dropped_failed_tx: AtomicU64,
    pub dropped_no_program_data: AtomicU64,
    pub dropped_self_dup: AtomicU64,
    pub duplicates_cross_feed: AtomicU64,

    pub lines_total: AtomicU64,
    pub lines_program_data: AtomicU64,

    pub last_message_unix: AtomicU64,
    pub started_unix: u64,
}

impl FeedMetrics {
    pub fn new(name: &'static str) -> Arc<Self> {
        Arc::new(Self {
            name,
            subscribed: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            idle_reconnects: AtomicU64::new(0),
            stream_errors: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            events: AtomicU64::new(0),
            parse_errors: AtomicU64::new(0),
            err_logs: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            dropped_failed_tx: AtomicU64::new(0),
            dropped_no_program_data: AtomicU64::new(0),
            dropped_self_dup: AtomicU64::new(0),
            duplicates_cross_feed: AtomicU64::new(0),
            lines_total: AtomicU64::new(0),
            lines_program_data: AtomicU64::new(0),
            last_message_unix: AtomicU64::new(0),
            started_unix: now_unix(),
        })
    }

    pub fn note_subscribed(&self) {
        self.subscribed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_reconnect(&self) {
        self.reconnects.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_idle_reconnect(&self) {
        self.idle_reconnects.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_stream_error(&self) {
        self.stream_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_message(&self, bytes: u64) {
        self.messages.fetch_add(1, Ordering::Relaxed);
        self.bytes_in.fetch_add(bytes, Ordering::Relaxed);
        self.last_message_unix.store(now_unix(), Ordering::Relaxed);
    }
    pub fn note_event(&self) {
        self.events.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_parse_error(&self) {
        self.parse_errors.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_err_log(&self) {
        self.err_logs.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_dropped_failed_tx(&self) {
        self.dropped_failed_tx.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_dropped_no_program_data(&self) {
        self.dropped_no_program_data.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_dropped_self_dup(&self) {
        self.dropped_self_dup.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_cross_dup(&self) {
        self.duplicates_cross_feed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn add_lines(&self, total: u64, program_data: u64) {
        self.lines_total.fetch_add(total, Ordering::Relaxed);
        self.lines_program_data
            .fetch_add(program_data, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> FeedSnapshot {
        let now = now_unix();
        let uptime = now.saturating_sub(self.started_unix).max(1);
        let messages = self.messages.load(Ordering::Relaxed);
        let events = self.events.load(Ordering::Relaxed);
        let bytes = self.bytes_in.load(Ordering::Relaxed);
        let dropped_failed = self.dropped_failed_tx.load(Ordering::Relaxed);
        let dropped_npd = self.dropped_no_program_data.load(Ordering::Relaxed);
        let dropped_self = self.dropped_self_dup.load(Ordering::Relaxed);

        let processed_after_prefilter = messages
            .saturating_sub(dropped_failed)
            .saturating_sub(dropped_npd)
            .saturating_sub(dropped_self)
            .max(1);

        FeedSnapshot {
            name: self.name.to_string(),
            subscribed: self.subscribed.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            idle_reconnects: self.idle_reconnects.load(Ordering::Relaxed),
            stream_errors: self.stream_errors.load(Ordering::Relaxed),
            messages,
            events,
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            err_logs: self.err_logs.load(Ordering::Relaxed),
            bytes_in: bytes,
            dropped_failed_tx: dropped_failed,
            dropped_no_program_data: dropped_npd,
            dropped_self_dup: dropped_self,
            duplicates_cross_feed: self.duplicates_cross_feed.load(Ordering::Relaxed),
            lines_total: self.lines_total.load(Ordering::Relaxed),
            lines_program_data: self.lines_program_data.load(Ordering::Relaxed),
            last_message_unix: self.last_message_unix.load(Ordering::Relaxed),
            started_unix: self.started_unix,
            uptime_secs: uptime,
            msg_per_sec_avg: messages as f64 / uptime as f64,
            events_per_sec_avg: events as f64 / uptime as f64,
            bytes_per_sec_avg: bytes as f64 / uptime as f64,
            useful_msg_ratio: events as f64 / processed_after_prefilter as f64,
            events_per_msg_total: events as f64 / messages.max(1) as f64,
        }
    }
}

/// Bounded LRU of recently seen tx signatures used to count how often the
/// same transaction is delivered through more than one logsSubscribe.
pub struct SignatureDedup {
    capacity: usize,
    order: VecDeque<String>,
    owner: HashMap<String, &'static str>,
}

impl SignatureDedup {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity),
            owner: HashMap::with_capacity(capacity),
        }
    }

    /// Returns true if this signature has already been observed by another
    /// feed. The first feed to claim the signature is recorded as its owner.
    pub fn observe(&mut self, signature: &str, feed: &'static str) -> bool {
        if let Some(prev) = self.owner.get(signature) {
            return *prev != feed;
        }
        if self.order.len() >= self.capacity
            && let Some(old) = self.order.pop_front()
        {
            self.owner.remove(&old);
        }
        self.order.push_back(signature.to_string());
        self.owner.insert(signature.to_string(), feed);
        false
    }
}

pub type SharedDedup = Arc<Mutex<SignatureDedup>>;

pub fn new_dedup(capacity: usize) -> SharedDedup {
    Arc::new(Mutex::new(SignatureDedup::new(capacity)))
}

/// Lightweight LRU for self-feed dedup. Same tx delivered twice on the same
/// logsSubscribe stream (after a resubscribe or a re-broadcast) should be
/// dropped without re-doing parse / launchpad / DB work.
pub struct SelfDedup {
    capacity: usize,
    order: VecDeque<String>,
    seen: HashSet<String>,
}

impl SelfDedup {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::with_capacity(capacity),
            seen: HashSet::with_capacity(capacity),
        }
    }

    /// Returns true if this signature was already observed in the same feed.
    pub fn check_and_remember(&mut self, signature: &str) -> bool {
        if self.seen.contains(signature) {
            return true;
        }
        if self.order.len() >= self.capacity
            && let Some(old) = self.order.pop_front()
        {
            self.seen.remove(&old);
        }
        self.order.push_back(signature.to_string());
        self.seen.insert(signature.to_string());
        false
    }
}

// --- Feed health (stall detection for metrics logger) ----------------------

/// Feed stall detection for the periodic metrics logger (see `main.rs`).
pub const FEED_STALL_MSG_AGE_SECS: u64 = 60;
/// Consecutive zero-rate intervals (each ~30s) before declaring msgs/s dead.
pub const FEED_ZERO_RATE_STREAK: u32 = 3;
/// Skip stall alerts right after process start while WS connects.
pub const FEED_STARTUP_GRACE_SECS: u64 = 90;

/// Interval `msgs/s` below this counts as zero (guards float noise).
const FEED_ZERO_RATE_EPS: f64 = 0.05;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedHealthEvent {
    Ok,
    Stall,
    Recovered,
}

/// Tracks per-feed message-rate streaks and emits stall / recovery events.
pub struct FeedHealthMonitor {
    zero_rate_streak: HashMap<String, u32>,
    was_stalled: HashMap<String, bool>,
}

impl FeedHealthMonitor {
    pub fn new() -> Self {
        Self {
            zero_rate_streak: HashMap::new(),
            was_stalled: HashMap::new(),
        }
    }

    /// Evaluate one feed snapshot against the latest interval message rate.
    pub fn evaluate(&mut self, snap: &FeedSnapshot, interval_rate: f64) -> FeedHealthEvent {
        if snap.uptime_secs < FEED_STARTUP_GRACE_SECS {
            return FeedHealthEvent::Ok;
        }
        // Feeds that were never subscribed (e.g. disabled pumpswap) are ignored.
        if snap.subscribed == 0 {
            return FeedHealthEvent::Ok;
        }

        let now = now_unix();
        let last_msg_age_s = if snap.last_message_unix == 0 {
            snap.uptime_secs
        } else {
            now.saturating_sub(snap.last_message_unix)
        };

        let zero_rate = interval_rate < FEED_ZERO_RATE_EPS;
        let streak = self
            .zero_rate_streak
            .entry(snap.name.clone())
            .or_insert(0);
        if zero_rate {
            *streak = streak.saturating_add(1);
        } else {
            *streak = 0;
        }

        let stalled = last_msg_age_s > FEED_STALL_MSG_AGE_SECS
            || *streak >= FEED_ZERO_RATE_STREAK;

        let was = *self.was_stalled.get(&snap.name).unwrap_or(&false);
        self.was_stalled.insert(snap.name.clone(), stalled);

        if stalled {
            FeedHealthEvent::Stall
        } else if was {
            FeedHealthEvent::Recovered
        } else {
            FeedHealthEvent::Ok
        }
    }

    pub fn zero_rate_streak(&self, feed: &str) -> u32 {
        self.zero_rate_streak.get(feed).copied().unwrap_or(0)
    }

    pub fn last_msg_age_secs(snap: &FeedSnapshot) -> u64 {
        let now = now_unix();
        if snap.last_message_unix == 0 {
            snap.uptime_secs
        } else {
            now.saturating_sub(snap.last_message_unix)
        }
    }
}

// --- Bot-level usefulness metrics --------------------------------------------

#[derive(Debug, serde::Serialize)]
pub struct BotSnapshot {
    pub creates_total: u64,
    pub creates_no_history: u64,
    pub creates_filter_rejected: u64,
    pub creates_passed_filter: u64,
    pub spam_dev_skipped: u64,
    pub score_skipped: u64,
    pub score_a: u64,
    pub score_a_plus: u64,
    pub score_b: u64,
    pub continuation_skipped: u64,
    pub parabolic_skipped: u64,
    pub strategy_blocked: u64,
    pub positions_initiated: u64,
    pub uptime_secs: u64,
}

pub struct BotMetrics {
    pub creates_total: AtomicU64,
    pub creates_no_history: AtomicU64,
    pub creates_filter_rejected: AtomicU64,
    pub creates_passed_filter: AtomicU64,
    pub spam_dev_skipped: AtomicU64,
    pub score_skipped: AtomicU64,
    pub score_a: AtomicU64,
    pub score_a_plus: AtomicU64,
    pub score_b: AtomicU64,
    pub continuation_skipped: AtomicU64,
    pub parabolic_skipped: AtomicU64,
    pub strategy_blocked: AtomicU64,
    pub positions_initiated: AtomicU64,
    pub started_unix: u64,
}

impl BotMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            creates_total: AtomicU64::new(0),
            creates_no_history: AtomicU64::new(0),
            creates_filter_rejected: AtomicU64::new(0),
            creates_passed_filter: AtomicU64::new(0),
            spam_dev_skipped: AtomicU64::new(0),
            score_skipped: AtomicU64::new(0),
            score_a: AtomicU64::new(0),
            score_a_plus: AtomicU64::new(0),
            score_b: AtomicU64::new(0),
            continuation_skipped: AtomicU64::new(0),
            parabolic_skipped: AtomicU64::new(0),
            strategy_blocked: AtomicU64::new(0),
            positions_initiated: AtomicU64::new(0),
            started_unix: now_unix(),
        })
    }

    pub fn note_create(&self) {
        self.creates_total.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_no_history(&self) {
        self.creates_no_history.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_filter_rejected(&self) {
        self.creates_filter_rejected.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_passed_filter(&self) {
        self.creates_passed_filter.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_spam_dev_skip(&self) {
        self.spam_dev_skipped.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_score_skip(&self) {
        self.score_skipped.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_score_a(&self) {
        self.score_a.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_score_a_plus(&self) {
        self.score_a_plus.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_score_b(&self) {
        self.score_b.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_continuation_skip(&self) {
        self.continuation_skipped.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_parabolic_skip(&self) {
        self.parabolic_skipped.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_strategy_blocked(&self) {
        self.strategy_blocked.fetch_add(1, Ordering::Relaxed);
    }
    pub fn note_position_initiated(&self) {
        self.positions_initiated.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> BotSnapshot {
        let uptime = now_unix().saturating_sub(self.started_unix).max(1);
        BotSnapshot {
            creates_total: self.creates_total.load(Ordering::Relaxed),
            creates_no_history: self.creates_no_history.load(Ordering::Relaxed),
            creates_filter_rejected: self.creates_filter_rejected.load(Ordering::Relaxed),
            creates_passed_filter: self.creates_passed_filter.load(Ordering::Relaxed),
            spam_dev_skipped: self.spam_dev_skipped.load(Ordering::Relaxed),
            score_skipped: self.score_skipped.load(Ordering::Relaxed),
            score_a: self.score_a.load(Ordering::Relaxed),
            score_a_plus: self.score_a_plus.load(Ordering::Relaxed),
            score_b: self.score_b.load(Ordering::Relaxed),
            continuation_skipped: self.continuation_skipped.load(Ordering::Relaxed),
            parabolic_skipped: self.parabolic_skipped.load(Ordering::Relaxed),
            strategy_blocked: self.strategy_blocked.load(Ordering::Relaxed),
            positions_initiated: self.positions_initiated.load(Ordering::Relaxed),
            uptime_secs: uptime,
        }
    }
}

#[cfg(test)]
mod feed_health_tests {
    use super::*;

    fn snap(name: &str, uptime: u64, last_msg_age: u64) -> FeedSnapshot {
        let now = now_unix();
        FeedSnapshot {
            name: name.to_string(),
            subscribed: 1,
            reconnects: 0,
            idle_reconnects: 0,
            stream_errors: 0,
            messages: 100,
            events: 10,
            parse_errors: 0,
            err_logs: 0,
            bytes_in: 0,
            dropped_failed_tx: 0,
            dropped_no_program_data: 0,
            dropped_self_dup: 0,
            duplicates_cross_feed: 0,
            lines_total: 0,
            lines_program_data: 0,
            last_message_unix: now.saturating_sub(last_msg_age),
            started_unix: now.saturating_sub(uptime),
            uptime_secs: uptime,
            msg_per_sec_avg: 0.0,
            events_per_sec_avg: 0.0,
            bytes_per_sec_avg: 0.0,
            useful_msg_ratio: 0.0,
            events_per_msg_total: 0.0,
        }
    }

    #[test]
    fn startup_grace_suppresses_alerts() {
        let mut m = FeedHealthMonitor::new();
        let s = snap("pump", FEED_STARTUP_GRACE_SECS - 1, 120);
        assert_eq!(m.evaluate(&s, 0.0), FeedHealthEvent::Ok);
    }

    #[test]
    fn last_msg_age_triggers_stall() {
        let mut m = FeedHealthMonitor::new();
        let s = snap("pump", 200, FEED_STALL_MSG_AGE_SECS + 1);
        assert_eq!(m.evaluate(&s, 5.0), FeedHealthEvent::Stall);
    }

    #[test]
    fn zero_rate_streak_triggers_stall() {
        let mut m = FeedHealthMonitor::new();
        let s = snap("pump", 200, 0);
        assert_eq!(m.evaluate(&s, 0.0), FeedHealthEvent::Ok);
        assert_eq!(m.evaluate(&s, 0.0), FeedHealthEvent::Ok);
        assert_eq!(m.evaluate(&s, 0.0), FeedHealthEvent::Stall);
    }

    #[test]
    fn recovery_after_stall() {
        let mut m = FeedHealthMonitor::new();
        let s = snap("pump", 200, FEED_STALL_MSG_AGE_SECS + 5);
        assert_eq!(m.evaluate(&s, 0.0), FeedHealthEvent::Stall);
        let healthy = snap("pump", 200, 1);
        assert_eq!(m.evaluate(&healthy, 10.0), FeedHealthEvent::Recovered);
    }
}
