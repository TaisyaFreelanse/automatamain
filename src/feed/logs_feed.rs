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

use crate::feed::{feed::Feed, logs::log::HasLogsFilter};

pub type Result<T> = core::result::Result<T, Error>;

// Reconnect backoff bounds. Keeps us from hammering the upstream WS endpoint
// (and burning RPC plan credits) when the provider is unreachable.
const RECONNECT_BACKOFF_INITIAL_MS: u64 = 1_000;
const RECONNECT_BACKOFF_MAX_MS: u64 = 60_000;
// Idle timeout on the underlying log stream. The previous 30s value caused
// frequent re-subscribes during quiet periods; pump logs in particular can be
// silent for >30s without the connection being dead.
const STREAM_IDLE_TIMEOUT_SECS: u64 = 120;
// Only print a reconnect line every N consecutive failures to avoid log spam.
const RECONNECT_LOG_EVERY: u64 = 25;

impl<T> Feed<T>
where
    T: FromStr + HasLogsFilter + Send + Sync + Clone + 'static,
{
    pub async fn subscribe(
        self,
        ws_url: String,
        tx_config: RpcTransactionLogsConfig,
    ) -> Result<()> {
        let mut backoff_ms = RECONNECT_BACKOFF_INITIAL_MS;
        let failure_counter = AtomicU64::new(0);

        loop {
            let res = async {
                let client = PubsubClient::new(&ws_url).await?;
                let (mut log_notification, log_unsubscribe) =
                    PubsubClient::logs_subscribe(&client, T::logs_filter(), tx_config.clone())
                        .await?;

                // We successfully connected & subscribed: reset the failure
                // counter and the backoff window.
                failure_counter.store(0, Ordering::Relaxed);
                println!("connected (subscribed)");

                loop {
                    match timeout(
                        Duration::from_secs(STREAM_IDLE_TIMEOUT_SECS),
                        log_notification.next(),
                    )
                    .await
                    {
                        Ok(Some(log_info)) => {
                            if log_info.value.err.is_some() {
                                continue;
                            }

                            for log in log_info.value.logs {
                                if let Ok(event) = T::from_str(&log) {
                                    if self
                                        .tx
                                        .send((log_info.context.slot, event))
                                        .await
                                        .is_err()
                                    {
                                        println!("receiver dropped");
                                        let _ = log_unsubscribe().await;
                                        return Ok(());
                                    }
                                }
                            }
                        }

                        Ok(None) => {
                            // Stream closed by the remote side.
                            break;
                        }

                        Err(_) => {
                            // No messages for STREAM_IDLE_TIMEOUT_SECS; the
                            // socket is most likely dead. Reconnect.
                            break;
                        }
                    }
                }

                let _ = log_unsubscribe().await;
                Ok::<(), Error>(())
            }
            .await;

            // Backoff bookkeeping. We always sleep at least `backoff_ms` and
            // grow it exponentially up to RECONNECT_BACKOFF_MAX_MS while we
            // keep failing. The jitter avoids two parallel feeds reconnecting
            // in lockstep and double-spending RPC credits.
            let failures = failure_counter.fetch_add(1, Ordering::Relaxed) + 1;

            let should_log = failures == 1 || failures % RECONNECT_LOG_EVERY == 0;
            match res {
                Ok(_) => {
                    if should_log {
                        println!("stream ended cleanly, reconnecting (#{failures})");
                    }
                }
                Err(e) => {
                    if should_log {
                        println!("stream error: {e}, reconnecting (#{failures})");
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

// Cheap, dependency-free jitter source. We don't need cryptographic
// randomness here, just enough variance to desynchronize multiple feeds.
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
