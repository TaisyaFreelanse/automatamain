use std::collections::HashMap;

use crate::{
    generalize::general_commands::Currency,
    helper::Amount,
    persistence::creators::CreatorStatistics,
};
use tokio::sync::{mpsc, oneshot};

type Address = solana_address::Address;

/* =========================================================
1. The Core Performance Tracker
========================================================= */

pub struct PerformanceTracker {
    creator_filter: CreatorStatistics,
    threshold: f64,
    best_market_cap: f64,
}

impl PerformanceTracker {
    /// The absolute floor baseline. Decay will not drop standards below these values.
    pub fn default_floor() -> CreatorStatistics {
        CreatorStatistics {
            median_market_cap: Currency::Native(Amount::from_float_native(50.0)),
            trader_pnl_average: 5.0,
            total_holders_average: 30,
            average_volume: 50.0,
            median_total_trades: 40,
            average_unique_buy_to_sell_ratio: 0.0,
            average_buy_trader_size: Currency::Native(Amount::from_float_native(1.0)),
            total_coins: 0,
        }
    }

    pub fn new(threshold: f64) -> Self {
        Self {
            creator_filter: Self::default_floor(),
            threshold,
            best_market_cap: 0.0,
        }
    }

    /// Smoothly adapts to changing market conditions by decaying the current ATH requirements.
    /// This prevents the bot from locking up on a massive outlier from hours ago.
    pub fn decay_baseline(&mut self) {
        let decay = 0.90; // Decay by 10% every 100 tokens
        let floor = Self::default_floor();

        self.best_market_cap = f64::max(self.best_market_cap * decay, 0.0);

        self.creator_filter.trader_pnl_average = f64::max(
            self.creator_filter.trader_pnl_average * decay,
            floor.trader_pnl_average,
        );

        self.creator_filter.total_holders_average = u64::max(
            (self.creator_filter.total_holders_average as f64 * decay) as u64,
            floor.total_holders_average,
        );

        self.creator_filter.average_volume = f64::max(
            self.creator_filter.average_volume * decay,
            floor.average_volume,
        );

        self.creator_filter.median_total_trades = u64::max(
            (self.creator_filter.median_total_trades as f64 * decay) as u64,
            floor.median_total_trades,
        );

        self.creator_filter.average_unique_buy_to_sell_ratio = f64::max(
            self.creator_filter.average_unique_buy_to_sell_ratio * decay,
            floor.average_unique_buy_to_sell_ratio,
        );

        let new_mc = f64::max(
            self.creator_filter.median_market_cap.amount().to_float() * decay,
            floor.median_market_cap.amount().to_float(),
        );
        self.creator_filter.median_market_cap = Currency::Native(Amount::from_float_native(new_mc));

        let new_size = f64::max(
            self.creator_filter
                .average_buy_trader_size
                .amount()
                .to_float()
                * decay,
            floor.average_buy_trader_size.amount().to_float(),
        );
        self.creator_filter.average_buy_trader_size =
            Currency::Native(Amount::from_float_native(new_size));
    }

    pub fn set_filter(&mut self, creator_stats: CreatorStatistics) {
        self.creator_filter = creator_stats;
    }

    pub fn compare(&self, creator_stats: &CreatorStatistics) -> bool {
        let baseline = &self.creator_filter;
        let mut score = 0.0;
        let mut total = 0.0;

        macro_rules! cmp {
            ($a:expr, $b:expr) => {{
                total += 1.0;
                if $a >= $b {
                    score += 1.0;
                }
            }};
        }

        cmp!(
            creator_stats.trader_pnl_average,
            baseline.trader_pnl_average
        );
        cmp!(
            creator_stats.total_holders_average,
            baseline.total_holders_average
        );
        cmp!(creator_stats.average_volume, baseline.average_volume);
        cmp!(
            creator_stats.median_total_trades,
            baseline.median_total_trades
        );
        cmp!(
            creator_stats.average_unique_buy_to_sell_ratio,
            baseline.average_unique_buy_to_sell_ratio
        );
        cmp!(
            creator_stats.median_market_cap.amount().to_float(),
            baseline.median_market_cap.amount().to_float()
        );
        cmp!(
            creator_stats.average_buy_trader_size.amount().to_float(),
            baseline.average_buy_trader_size.amount().to_float()
        );

        let final_score = if total == 0.0 { 0.0 } else { score / total };
        final_score >= self.threshold
    }
}

/* =========================================================
2. Performance Tracker Actor
========================================================= */

pub enum TrackerMessage {
    Compare {
        stats: CreatorStatistics,
        respond_to: oneshot::Sender<bool>,
    },
    GetBestMarketCap {
        respond_to: oneshot::Sender<f64>,
    },
    SetFilter {
        stats: CreatorStatistics,
    },
    TryUpdateAth {
        market_cap: f64,
        stats: CreatorStatistics,
        respond_to: oneshot::Sender<bool>,
    },
}

#[derive(Clone)]
pub struct PerformanceTrackerHandle {
    sender: mpsc::Sender<TrackerMessage>,
}

impl PerformanceTrackerHandle {
    pub fn new(threshold: f64) -> Self {
        let (sender, mut receiver) = mpsc::channel(100);
        let mut tracker = PerformanceTracker::new(threshold);
        let mut tokens_processed = 0;

        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                match msg {
                    TrackerMessage::Compare { stats, respond_to } => {
                        tokens_processed += 1;
                        if tokens_processed >= 100 {
                            tracker.decay_baseline();
                            tokens_processed = 0;
                        }

                        let result = tracker.compare(&stats);
                        let _ = respond_to.send(result);
                    }
                    TrackerMessage::GetBestMarketCap { respond_to } => {
                        let best_mc = tracker.creator_filter.median_market_cap.amount().to_float();
                        let _ = respond_to.send(best_mc);
                    }
                    TrackerMessage::SetFilter { stats } => {
                        tracker.set_filter(stats);
                    }
                    TrackerMessage::TryUpdateAth {
                        market_cap,
                        stats,
                        respond_to,
                    } => {
                        if market_cap > tracker.best_market_cap {
                            tracker.best_market_cap = market_cap;
                            tracker.set_filter(stats);

                            let _ = respond_to.send(true);
                        } else {
                            let _ = respond_to.send(false);
                        }
                    }
                }
            }
        });

        Self { sender }
    }

    pub async fn compare(&self, stats: CreatorStatistics) -> bool {
        let (send, recv) = oneshot::channel();
        let _ = self
            .sender
            .send(TrackerMessage::Compare {
                stats,
                respond_to: send,
            })
            .await;
        recv.await.unwrap_or(false)
    }

    pub async fn get_best_market_cap(&self) -> f64 {
        let (send, recv) = oneshot::channel();
        let _ = self
            .sender
            .send(TrackerMessage::GetBestMarketCap { respond_to: send })
            .await;
        recv.await.unwrap_or(0.0)
    }

    pub async fn set_filter(&self, stats: CreatorStatistics) {
        let _ = self.sender.send(TrackerMessage::SetFilter { stats }).await;
    }

    pub async fn try_update_ath(&self, market_cap: f64, stats: CreatorStatistics) -> bool {
        let (send, recv) = oneshot::channel();

        let _ = self
            .sender
            .send(TrackerMessage::TryUpdateAth {
                market_cap,
                stats,
                respond_to: send,
            })
            .await;

        recv.await.unwrap_or(false)
    }
}

/* =========================================================
3. Creator Registry Actor
========================================================= */

pub enum RegistryMessage {
    Save {
        address: Address,
        stats: CreatorStatistics,
    },
    Get {
        address: Address,
        respond_to: oneshot::Sender<Option<CreatorStatistics>>,
    },
}

#[derive(Clone)]
pub struct CreatorRegistryHandle {
    sender: mpsc::Sender<RegistryMessage>,
}

impl Default for CreatorRegistryHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl CreatorRegistryHandle {
    pub fn new() -> Self {
        let (sender, mut receiver) = mpsc::channel(100);

        let mut map: HashMap<Address, CreatorStatistics> = HashMap::new();

        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                match msg {
                    RegistryMessage::Save { address, stats } => {
                        map.insert(address, stats);
                    }
                    RegistryMessage::Get {
                        address,
                        respond_to,
                    } => {
                        let result = map.get(&address).cloned();
                        let _ = respond_to.send(result);
                    }
                }
            }
        });

        Self { sender }
    }

    pub async fn save(&self, address: Address, stats: CreatorStatistics) {
        let _ = self
            .sender
            .send(RegistryMessage::Save { address, stats })
            .await;
    }

    pub async fn get(&self, address: Address) -> Option<CreatorStatistics> {
        let (send, recv) = oneshot::channel();
        let _ = self
            .sender
            .send(RegistryMessage::Get {
                address,
                respond_to: send,
            })
            .await;
        recv.await.unwrap_or(None)
    }
}
