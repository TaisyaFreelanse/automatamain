//! Batched persistence for hot-path trade tape + trader rows.

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use sqlx::PgPool;
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};

use crate::persistence::{
    coin_mcap_tape::{self, TapeRow},
    traders::{TraderEntry, TraderRepository},
};

const CHANNEL_CAP: usize = 32_768;
const FLUSH_INTERVAL_MS: u64 = 150;
const FLUSH_MAX_OPS: usize = 64;

enum WriteOp {
    Tape {
        coin: String,
        mcap_sol: f64,
        source: String,
    },
    Trade(TraderEntry),
}

pub struct WriteQueueMetrics {
    pub enqueued: AtomicU64,
    pub dropped: AtomicU64,
    pub flushed_ops: AtomicU64,
    pub flush_batches: AtomicU64,
}

impl Default for WriteQueueMetrics {
    fn default() -> Self {
        Self {
            enqueued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            flushed_ops: AtomicU64::new(0),
            flush_batches: AtomicU64::new(0),
        }
    }
}

#[derive(Clone)]
pub struct PersistenceWriteQueue {
    tx: mpsc::Sender<WriteOp>,
    pub metrics: Arc<WriteQueueMetrics>,
}

impl PersistenceWriteQueue {
    pub fn spawn(
        pool: PgPool,
        trades: Arc<dyn TraderRepository + Send + Sync>,
    ) -> Self {
        let (tx, mut rx) = mpsc::channel(CHANNEL_CAP);
        let metrics = Arc::new(WriteQueueMetrics::default());
        let metrics_worker = metrics.clone();

        tokio::spawn(async move {
            let mut buf: Vec<WriteOp> = Vec::with_capacity(FLUSH_MAX_OPS);
            loop {
                let deadline = sleep(Duration::from_millis(FLUSH_INTERVAL_MS));
                tokio::pin!(deadline);

                loop {
                    tokio::select! {
                        _ = &mut deadline => break,
                        msg = rx.recv() => {
                            match msg {
                                Some(op) => {
                                    buf.push(op);
                                    if buf.len() >= FLUSH_MAX_OPS {
                                        break;
                                    }
                                }
                                None => {
                                    if !buf.is_empty() {
                                        flush_batch(
                                            &pool,
                                            trades.clone(),
                                            &mut buf,
                                            &metrics_worker,
                                        )
                                        .await;
                                    }
                                    return;
                                }
                            }
                        }
                    }
                }

                if !buf.is_empty() {
                    flush_batch(&pool, trades.clone(), &mut buf, &metrics_worker).await;
                }
            }
        });

        Self { tx, metrics }
    }

    pub fn try_enqueue_tape(&self, coin: String, mcap_sol: f64, source: &str) {
        self.try_send(WriteOp::Tape {
            coin,
            mcap_sol,
            source: source.to_string(),
        });
    }

    pub fn try_enqueue_trade(&self, entry: TraderEntry) {
        self.try_send(WriteOp::Trade(entry));
    }

    fn try_send(&self, op: WriteOp) {
        self.metrics.enqueued.fetch_add(1, Ordering::Relaxed);
        if self.tx.try_send(op).is_err() {
            let n = self.metrics.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if n == 1 || n.is_multiple_of(1000) {
                eprintln!("[WRITE_QUEUE] dropped ops (total={n})");
            }
        }
    }
}

async fn flush_batch(
    pool: &PgPool,
    trades: Arc<dyn TraderRepository + Send + Sync>,
    buf: &mut Vec<WriteOp>,
    metrics: &WriteQueueMetrics,
) {
    let n = buf.len();
    metrics.flush_batches.fetch_add(1, Ordering::Relaxed);
    metrics
        .flushed_ops
        .fetch_add(n as u64, Ordering::Relaxed);

    let mut tape_rows: Vec<TapeRow> = Vec::new();
    let mut trade_ops: Vec<TraderEntry> = Vec::new();

    for op in buf.drain(..) {
        match op {
            WriteOp::Tape {
                coin,
                mcap_sol,
                source,
            } => {
                if let Some(row) = coin_mcap_tape::tape_row(&coin, mcap_sol, &source) {
                    tape_rows.push(row);
                }
            }
            WriteOp::Trade(e) => trade_ops.push(e),
        }
    }

    if !tape_rows.is_empty() {
        if let Err(e) = coin_mcap_tape::record_batch(pool, &tape_rows).await {
            eprintln!("[WRITE_QUEUE] tape batch failed: {e}");
        }
    }

    for entry in trade_ops {
        if let Err(e) = trades.save_trade(entry).await {
            eprintln!("[WRITE_QUEUE] save_trade failed: {e}");
        }
    }
}
