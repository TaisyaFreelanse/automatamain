use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
    pub stream_errors: u64,
    pub messages: u64,
    pub events: u64,
    pub parse_errors: u64,
    pub err_logs: u64,
    pub bytes_in: u64,
    pub duplicates_cross_feed: u64,
    pub last_message_unix: u64,
    pub started_unix: u64,
    pub uptime_secs: u64,
    pub msg_per_sec_avg: f64,
    pub events_per_sec_avg: f64,
    pub bytes_per_sec_avg: f64,
}

pub struct FeedMetrics {
    pub name: &'static str,
    pub subscribed: AtomicU64,
    pub reconnects: AtomicU64,
    pub stream_errors: AtomicU64,
    pub messages: AtomicU64,
    pub events: AtomicU64,
    pub parse_errors: AtomicU64,
    pub err_logs: AtomicU64,
    pub bytes_in: AtomicU64,
    pub duplicates_cross_feed: AtomicU64,
    pub last_message_unix: AtomicU64,
    pub started_unix: u64,
}

impl FeedMetrics {
    pub fn new(name: &'static str) -> Arc<Self> {
        Arc::new(Self {
            name,
            subscribed: AtomicU64::new(0),
            reconnects: AtomicU64::new(0),
            stream_errors: AtomicU64::new(0),
            messages: AtomicU64::new(0),
            events: AtomicU64::new(0),
            parse_errors: AtomicU64::new(0),
            err_logs: AtomicU64::new(0),
            bytes_in: AtomicU64::new(0),
            duplicates_cross_feed: AtomicU64::new(0),
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
    pub fn note_cross_dup(&self) {
        self.duplicates_cross_feed.fetch_add(1, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> FeedSnapshot {
        let now = now_unix();
        let uptime = now.saturating_sub(self.started_unix).max(1);
        let messages = self.messages.load(Ordering::Relaxed);
        let events = self.events.load(Ordering::Relaxed);
        let bytes = self.bytes_in.load(Ordering::Relaxed);
        FeedSnapshot {
            name: self.name.to_string(),
            subscribed: self.subscribed.load(Ordering::Relaxed),
            reconnects: self.reconnects.load(Ordering::Relaxed),
            stream_errors: self.stream_errors.load(Ordering::Relaxed),
            messages,
            events,
            parse_errors: self.parse_errors.load(Ordering::Relaxed),
            err_logs: self.err_logs.load(Ordering::Relaxed),
            bytes_in: bytes,
            duplicates_cross_feed: self.duplicates_cross_feed.load(Ordering::Relaxed),
            last_message_unix: self.last_message_unix.load(Ordering::Relaxed),
            started_unix: self.started_unix,
            uptime_secs: uptime,
            msg_per_sec_avg: messages as f64 / uptime as f64,
            events_per_sec_avg: events as f64 / uptime as f64,
            bytes_per_sec_avg: bytes as f64 / uptime as f64,
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
            && let Some(old) = self.order.pop_front() {
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
