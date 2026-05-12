use crate::{
    feed::logs::pump::{PumpEvent, TradeEvent},
    general::Slot,
    generalize::general_commands::{Action, TradeAction},
    helper::Amount,
    launchpads::{
        pump::{
            general::{PRECISION, pool_pda},
            pool::{Bonding, Migrated, PumpPool},
        },
        token_bucket::TokenBucket,
    },
    trading::{
        offer::Offer,
        swarm::{Swarm, SwarmActor, SwarmHandler},
        trader::TraderType,
    },
};
use std::{collections::HashMap, sync::Arc};

use solana_address::Address;
use tokio::sync::{RwLock, oneshot};

use tokio::sync::mpsc;

//trying not to fuck up everything
pub type AmmPoolAddress = solana_address::Address;

pub enum PumpLaunchpadCommand {
    GetMint {
        // ^ weird right? why would we...
        // || even weirder
        // \/ why amm pool?..
        amm_pool: AmmPoolAddress,
        respond_to: oneshot::Sender<Option<Address>>,
    },

    Event((Slot, PumpEvent), oneshot::Sender<()>),
    GetBucket {
        mint: Address,
        respond_to: oneshot::Sender<TokenBucket>,
    },
    TokenExists {
        mint: Address,
        respond_to: oneshot::Sender<bool>,
    },
}

pub struct PumpLaunchpadStorage {
    tokens: HashMap<AmmPoolAddress, TokenBucket>,
    sniper_threshold: u64,
}

impl PumpLaunchpadStorage {
    pub fn new(sniper_threshold: u64) -> Self {
        Self {
            tokens: HashMap::new(),
            sniper_threshold,
        }
    }

    pub async fn react(&mut self, action: PumpEvent, slot: u64, finish: oneshot::Sender<()>) {
        match action {
            PumpEvent::Create(create_event) => {
                const MAX_SUPPLY: crate::helper::Amount =
                    Amount::from_raw(1_000_000_000, PRECISION);
                self.tokens.insert(
                    pool_pda(&create_event.mint).0,
                    TokenBucket::new(
                        Box::new(PumpPool::<Bonding>::new(
                            create_event.mint,
                            create_event.creator,
                        )),
                        MAX_SUPPLY,
                        slot,
                    ),
                );
            }
            PumpEvent::TradeEvent(trade_event) => {
                let bucket = match self.tokens.get_mut(&pool_pda(&trade_event.mint).0) {
                    Some(bucket) => bucket,
                    None => return,
                };

                let trade: TradeAction = trade_event.clone().into();
                let user = trade_event.user;

                let mut role = TraderType::Regular;

                if (slot - bucket.created_at()) < self.sniper_threshold {
                    role = TraderType::Sniper;
                }

                for dev in bucket.pool().creators() {
                    if dev.as_array() == trade_event.user.as_array() {
                        role = TraderType::Creator;
                    }
                }

                bucket.update_pool(&PumpEvent::TradeEvent(trade_event));
                bucket.update_swarm(user, trade.into(), role).await;

                // println!("------------------------------------")
            }
        }
        finish.send(());
    }
}

pub struct PumpLaunchpadStorageActor {
    rx: mpsc::Receiver<PumpLaunchpadCommand>,
    domain: PumpLaunchpadStorage,
}

impl PumpLaunchpadStorageActor {
    pub fn new(sniper_threshold: u64) -> (Self, mpsc::Sender<PumpLaunchpadCommand>) {
        let (tx, rx) = mpsc::channel(4096);

        (
            Self {
                rx,
                domain: PumpLaunchpadStorage::new(sniper_threshold),
            },
            tx,
        )
    }

    pub async fn listen(&mut self) {
        while let Some(command) = self.rx.recv().await {
            match command {
                PumpLaunchpadCommand::GetMint {
                    amm_pool,
                    respond_to,
                } => {
                    let mint = match self.domain.tokens.get(&amm_pool) {
                        Some(bucket) => bucket.pool().mint(),
                        None => {
                            let _ = respond_to.send(None);
                            continue;
                        }
                    };

                    let _ = respond_to.send(Some(mint));
                }
                PumpLaunchpadCommand::Event((slot, pump_event), finish) => {
                    self.domain.react(pump_event, slot, finish).await;
                }
                PumpLaunchpadCommand::GetBucket { mint, respond_to } => {
                    let bucket = match self.domain.tokens.get(&pool_pda(&mint).0) {
                        Some(bucket) => bucket,
                        None => continue,
                    };

                    let _ = respond_to.send(bucket.clone());
                }
                PumpLaunchpadCommand::TokenExists { mint, respond_to } => {
                    // Convert the mint to the PDA to check the HashMap keys
                    let pool_address = pool_pda(&mint).0;
                    let exists = self.domain.tokens.contains_key(&pool_address);

                    let _ = respond_to.send(exists);
                }
            }
        }
    }
}
