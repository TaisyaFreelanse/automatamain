use std::{
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use futures::stream::StreamExt;
use solana_pubsub_client::nonblocking::pubsub_client::PubsubClient;
use solana_rpc_client_types::config::RpcTransactionLogsConfig;
use thiserror::Error;
use tokio::time::timeout;

use crate::feed::{
    feed::Feed,
    logs::log::HasLogsFilter,
    metrics::SharedDedup,
};

pub type Result<T> = core::result::Result<T, Error>;

const RECONNECT_BACKOFF_INITIAL_MS: u64 = 1_000;
const RECONNECT_BACKOFF_MAX_MS: u64 = 60_000;
const STREAM_IDLE_TIMEOUT_SECS: u64 = 120;
const RECONNECT_LOG_EVERY: u64 = 25;

impl<T> Feed<T>
where
    T: FromStr + HasLogsFilter + Send + Sync + Clone + 'static,
{
    pub async fn subscribe(
        self,
        ws_url: String,
        tx_config: RpcTransactionLogsConfig,
        dedup: SharedDedup,
    ) -> Result<()> {
        let feed_name: &'static str = self.metrics.name;
        let mut backoff_ms = RECONNECT_BACKOFF_INITIAL_MS;
        let failure_counter = AtomicU64::new(0);

        loop {
            let res = async {
                let client = PubsubClient::new(&ws_url).await?;
                let (mut log_notification, log_unsubscribe) =
                    PubsubClient::logs_subscribe(&client, T::logs_filter(), tx_config.clone())
                        .await?;

                failure_counter.store(0, Ordering::Relaxed);
                self.metrics.note_subscribed();
                println!("[{}] connected (subscribed)", feed_name);

                loop {
                    match timeout(
                        Duration::from_secs(STREAM_IDLE_TIMEOUT_SECS),
                        log_notification.next(),
                    )
                    .await
                    {
                        Ok(Some(log_info)) => {
                            // Approximate payload size: sum of all log string lengths.
                            let bytes: u64 = log_info
                                .value
                                .logs
                                .iter()
                                .map(|l| l.len() as u64)
                                .sum::<u64>()
                                + log_info.value.signature.len() as u64;
                            self.metrics.note_message(bytes);

                            // Cross-feed signature dedup. We don't drop the
                            // event (the other feed may emit a different
                            // event type for the same tx), we just count it.
                            if !log_info.value.signature.is_empty()
                                && let Ok(mut guard) = dedup.lock()
                                    && guard.observe(&log_info.value.signature, feed_name) {
                                        self.metrics.note_cross_dup();
                                    }

                            if log_info.value.err.is_some() {
                                self.metrics.note_err_log();
                                continue;
                            }

                            for log in log_info.value.logs {
                                match T::from_str(&log) {
                                    Ok(event) => {
                                        self.metrics.note_event();
                                        if self
                                            .tx
                                            .send((log_info.context.slot, event))
                                            .await
                                            .is_err()
                                        {
                                            println!("[{}] receiver dropped", feed_name);
                                            let _ = log_unsubscribe().await;
                                            return Ok(());
                                        }
                                    }
                                    Err(_) => {
                                        self.metrics.note_parse_error();
                                    }
                                }
                            }
                        }

                        Ok(None) => break,
                        Err(_) => break,
                    }
                }

                let _ = log_unsubscribe().await;
                Ok::<(), Error>(())
            }
            .await;

            let failures = failure_counter.fetch_add(1, Ordering::Relaxed) + 1;
            self.metrics.note_reconnect();
            let should_log = failures == 1 || failures.is_multiple_of(RECONNECT_LOG_EVERY);

            match res {
                Ok(_) => {
                    if should_log {
                        println!(
                            "[{}] stream ended cleanly, reconnecting (#{failures})",
                            feed_name
                        );
                    }
                }
                Err(e) => {
                    self.metrics.note_stream_error();
                    if should_log {
                        println!(
                            "[{}] stream error: {e}, reconnecting (#{failures})",
                            feed_name
                        );
                    }
                }
            }

            let jitter_ms = pseudo_jitter_ms(backoff_ms / 4);
            let sleep_ms = backoff_ms.saturating_add(jitter_ms);
            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;

            backoff_ms = (backoff_ms.saturating_mul(2)).min(RECONNECT_BACKOFF_MAX_MS);
        }
    }
}

fn pseudo_jitter_ms(span_ms: u64) -> u64 {
    if span_ms == 0 {
        return 0;
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    nanos % span_ms
}

#[derive(Error, Debug)]
pub enum Error {
    #[error("Solana websocket error: {0}")]
    PubSub(#[from] solana_pubsub_client::pubsub_client::PubsubClientError),
}
