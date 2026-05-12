use solana_address::Address;
use tokio::sync::{
    mpsc::{self, Receiver},
    oneshot,
};

use crate::{
    helper::Amount,
    trading::{
        offer::Offer,
        trader::{Trader, TraderType},
    },
};

pub type TraderAddress = Address;

#[derive(Clone)]
pub struct Swarm {
    traders: Vec<(TraderAddress, Trader)>,
    #[allow(dead_code)]
    max_token_supply: Amount,
    decimals: u8,
    decimals_quote: u8,
}

impl Swarm {
    pub fn new(max_supply: Amount, decimals: u8, decimals_quote: u8) -> Self {
        Self {
            traders: vec![],
            max_token_supply: max_supply,
            decimals,
            decimals_quote,
        }
    }

    pub fn update(&mut self, trader_address: TraderAddress, offer: Offer, trader_type: TraderType) {
        match self.traders.iter_mut().find(|t| t.0 == trader_address) {
            Some((_address, trader)) => {
                trader.apply(offer);
            }
            None => {
                let mut trader = Trader::empty(trader_type, self.decimals, self.decimals_quote);
                trader.apply(offer);
                self.traders.push((trader_address, trader));
            }
        }
    }

    pub fn holders(&self) -> &[(TraderAddress, Trader)] {
        &self.traders
    }

    pub fn average_pln(&self, trader_type: TraderType) -> f64 {
        let filtered: Vec<&Trader> = self
            .holders()
            .iter()
            .filter(|(_, trader)| trader.trader_type() == trader_type)
            .map(|(_, trader)| trader)
            .collect();

        if filtered.is_empty() {
            return 0.0;
        }

        let total: f64 = filtered.iter().map(|trader| trader.pnl_percent()).sum();
        total / filtered.len() as f64
    }
}

pub enum SwarmMessage {
    GetPnl {
        trader_type: TraderType,
        to: oneshot::Sender<f64>,
    },

    GetTrader {
        trader_address: TraderAddress,
        to: oneshot::Sender<Trader>,
    },

    GetAllTraders {
        to: oneshot::Sender<usize>,
    },

    UpdateTrader {
        trader_address: TraderAddress,
        offer: Offer,
        trader_type: TraderType,
    },

    GetTraderTotalSpent {
        trader_address: TraderAddress,
        to: oneshot::Sender<Amount>,
    },

    GetTradersByType {
        trader_type: TraderType,
        to: oneshot::Sender<Vec<(TraderAddress, Trader)>>,
    },

    GetQuoteDecimals {
        to: oneshot::Sender<u8>,
    },

    GetTraderPnl {
        trader_address: TraderAddress,
        to: oneshot::Sender<Option<f64>>,
    },
}

pub struct SwarmActor {
    rx: Receiver<SwarmMessage>,
    domain: Swarm,
}

impl SwarmActor {
    pub fn init(
        max_supply: Amount,
        decimals: u8,
        decimals_quote: u8,
    ) -> (Self, mpsc::Sender<SwarmMessage>) {
        let (tx, rx) = mpsc::channel(2048);
        (
            Self {
                rx,
                domain: Swarm::new(max_supply, decimals, decimals_quote),
            },
            tx,
        )
    }

    pub async fn run(&mut self) {
        while let Some(message) = self.rx.recv().await {
            match message {
                SwarmMessage::GetPnl { trader_type, to } => {
                    let _ = to.send(self.domain.average_pln(trader_type));
                }
                SwarmMessage::GetTrader { trader_address, to } => {
                    let trader_opt = self
                        .domain
                        .holders()
                        .iter()
                        .find(|(addr, _)| *addr == trader_address)
                        .map(|(_, trader)| trader.clone());
                    if let Some(trader) = trader_opt {
                        let _ = to.send(trader);
                    }
                }
                SwarmMessage::UpdateTrader {
                    trader_address,
                    offer,
                    trader_type,
                } => {
                    self.domain.update(trader_address, offer, trader_type);
                }
                SwarmMessage::GetAllTraders { to } => {
                    let _ = to.send(self.domain.holders().len());
                }
                SwarmMessage::GetTraderTotalSpent { trader_address, to } => {
                    let total = self
                        .domain
                        .holders()
                        .iter()
                        .find(|(addr, _)| *addr == trader_address)
                        .map(|(_, trader)| trader.total_spent())
                        .unwrap_or(Amount::from_raw(0, self.domain.decimals_quote));
                    let _ = to.send(total);
                }
                SwarmMessage::GetTradersByType { trader_type, to } => {
                    let filtered: Vec<(TraderAddress, Trader)> = self
                        .domain
                        .holders()
                        .iter()
                        .filter(|(_, trader)| trader.trader_type() == trader_type)
                        .map(|(addr, trader)| (*addr, trader.clone()))
                        .collect();
                    let _ = to.send(filtered);
                }

                SwarmMessage::GetQuoteDecimals { to } => {
                    let _ = to.send(self.domain.decimals_quote);
                }

                SwarmMessage::GetTraderPnl { trader_address, to } => {
                    let pnl = self
                        .domain
                        .holders()
                        .iter()
                        .find(|(addr, _)| *addr == trader_address)
                        .map(|(_, trader)| trader.pnl_percent()); // Get PnL if trader exists

                    let _ = to.send(pnl);
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct SwarmHandler {
    tx: mpsc::Sender<SwarmMessage>,
}

impl SwarmHandler {
    pub fn new(tx: mpsc::Sender<SwarmMessage>) -> Self {
        Self { tx }
    }

    pub async fn update(
        &self,
        trader_address: TraderAddress,
        offer: Offer,
        trader_type: TraderType,
    ) {
        let _ = self
            .tx
            .send(SwarmMessage::UpdateTrader {
                trader_address,
                offer,
                trader_type,
            })
            .await;
    }

    pub async fn get_pnl(&self, trader_type: TraderType) -> f64 {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(SwarmMessage::GetPnl {
                trader_type,
                to: tx,
            })
            .await;
        rx.await.unwrap_or(0.0)
    }

    pub async fn get_trader(&self, trader_address: TraderAddress) -> Option<Trader> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(SwarmMessage::GetTrader {
                trader_address,
                to: tx,
            })
            .await;
        rx.await.ok()
    }

    pub async fn get_traders_count(&self) -> usize {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(SwarmMessage::GetAllTraders { to: tx }).await;
        rx.await.unwrap_or(0)
    }

    pub async fn get_trader_total_spent(&self, trader_address: TraderAddress) -> Amount {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(SwarmMessage::GetTraderTotalSpent {
                trader_address,
                to: tx,
            })
            .await;
        rx.await
            .unwrap_or(Amount::from_raw(0, self.get_quote_decimals().await))
    }

    pub async fn get_quote_decimals(&self) -> u8 {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(SwarmMessage::GetQuoteDecimals { to: tx })
            .await;
        rx.await.unwrap_or(0)
    }

    pub async fn get_traders_by_type(
        &self,
        trader_type: TraderType,
    ) -> Vec<(TraderAddress, Trader)> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(SwarmMessage::GetTradersByType {
                trader_type,
                to: tx,
            })
            .await;
        rx.await.unwrap_or_default()
    }

    pub async fn get_trader_pnl(&self, trader_address: TraderAddress) -> Option<f64> {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .tx
            .send(SwarmMessage::GetTraderPnl {
                trader_address,
                to: tx,
            })
            .await;

        // Flattens Result<Option<f64>, RecvError> into Option<f64>
        rx.await.unwrap_or(None)
    }
}
