use std::sync::Arc;

use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::feed::metrics::FeedMetrics;
use crate::general::Slot;

#[derive(Clone)]
pub struct Feed<T> {
    pub tx: Sender<(Slot, T)>,
    pub metrics: Arc<FeedMetrics>,
}

impl<T> Feed<T> {
    pub fn with_metrics(metrics: Arc<FeedMetrics>) -> (Self, Receiver<(Slot, T)>) {
        let (tx, rx) = mpsc::channel(4096);
        (Self { tx, metrics }, rx)
    }
}
