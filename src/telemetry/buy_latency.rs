//! End-to-end buy path timing (created → score → gate → sent → confirmed).

use solana_address::Address;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

#[derive(Clone, Debug, Default)]
struct WalletTiming {
    sent: Option<Instant>,
    confirmed: Option<Instant>,
}

#[derive(Clone, Debug)]
struct Trace {
    created: Instant,
    score_done: Option<Instant>,
    gate: Option<Instant>,
    wallets: HashMap<String, WalletTiming>,
}

#[derive(Clone)]
pub struct BuyLatencyRegistry {
    inner: Arc<Mutex<HashMap<Address, Trace>>>,
}

impl Default for BuyLatencyRegistry {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl BuyLatencyRegistry {
    pub fn on_created(&self, mint: Address) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.entry(mint).or_insert_with(|| Trace {
            created: Instant::now(),
            score_done: None,
            gate: None,
            wallets: HashMap::new(),
        });
    }

    pub fn on_score_done(&self, mint: Address) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(t) = g.get_mut(&mint) {
            t.score_done = Some(Instant::now());
        }
    }

    pub fn on_gate(&self, mint: Address) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(t) = g.get_mut(&mint) {
            t.gate = Some(Instant::now());
        }
    }

    pub fn on_sent(&self, mint: Address, wallet_id: &str) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(t) = g.get_mut(&mint) {
            t.wallets
                .entry(wallet_id.to_string())
                .or_default()
                .sent = Some(Instant::now());
        }
    }

    pub fn on_confirmed(&self, mint: Address, wallet_id: &str) {
        Self::mark_wallet_done(self, mint, wallet_id);
    }

    pub fn on_buy_failed(&self, mint: Address, wallet_id: &str) {
        Self::mark_wallet_done(self, mint, wallet_id);
    }

    fn mark_wallet_done(&self, mint: Address, wallet_id: &str) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let Some(t) = g.get_mut(&mint) else {
            return;
        };
        t.wallets
            .entry(wallet_id.to_string())
            .or_default()
            .confirmed = Some(Instant::now());
        let all_done = !t.wallets.is_empty()
            && t
                .wallets
                .values()
                .all(|w| w.sent.is_some() && w.confirmed.is_some());
        if all_done {
            let snapshot = t.clone();
            Self::log_and_remove(&mut g, mint, snapshot);
        }
    }

    fn ms(from: Instant, to: Instant) -> u128 {
        to.duration_since(from).as_millis()
    }

    fn log_and_remove(g: &mut HashMap<Address, Trace>, mint: Address, t: Trace) {
        let created = t.created;
        let score_ms = t
            .score_done
            .map(|s| Self::ms(created, s))
            .unwrap_or(0);
        let gate_ms = t
            .gate
            .map(|g| Self::ms(created, g))
            .unwrap_or(0);
        let gate_to_score_ms = match (t.score_done, t.gate) {
            (Some(s), Some(g)) => Self::ms(s, g),
            _ => 0,
        };

        let mut wallet_parts = Vec::new();
        for (wid, w) in &t.wallets {
            let sent_ms = w
                .sent
                .map(|s| Self::ms(created, s))
                .unwrap_or(0);
            let conf_ms = w
                .confirmed
                .map(|c| Self::ms(created, c))
                .unwrap_or(0);
            let sent_to_conf = match (w.sent, w.confirmed) {
                (Some(s), Some(c)) => Self::ms(s, c),
                _ => 0,
            };
            let gate_to_sent = match (t.gate, w.sent) {
                (Some(g), Some(s)) => Self::ms(g, s),
                _ => 0,
            };
            wallet_parts.push(format!(
                "{wid}:gate_to_sent_ms={gate_to_sent} sent_ms={sent_ms} confirmed_ms={conf_ms} sent_to_confirmed_ms={sent_to_conf}"
            ));
        }
        wallet_parts.sort();

        tracing::info!(
            mint = %mint,
            created_to_score_ms = score_ms,
            created_to_gate_ms = gate_ms,
            score_to_gate_ms = gate_to_score_ms,
            wallets = %wallet_parts.join(" | "),
            "[LATENCY] buy path"
        );
        g.remove(&mint);
    }
}
