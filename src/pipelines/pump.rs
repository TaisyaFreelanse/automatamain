use solana_rpc_client_types::config::RpcTransactionLogsConfig;
use tokio::sync::{mpsc::Sender, oneshot};

use crate::{
    feed::{
        feed::Feed,
        logs::{
            pump::{CreateEvent, PumpEvent},
            pump_amm::PumpAmmEvent,
        },
    },
    general::Slot,
    generalize::general_commands::Action,
    launchpads::{
        pump::{
            handler::PumpLaunchpadSenderExt,
            launchpad::{PumpLaunchpadCommand, PumpLaunchpadStorageActor},
            pool::{Bonding, PumpPool},
        },
        token_bucket::TokenBucket,
    },
    trading::swarm::SwarmHandler,
};

pub struct PumpPipeline {
    ws_url: String,
    config: RpcTransactionLogsConfig,
    general_tx: Sender<(Slot, Action, TokenBucket)>,

    sniper_threshold: u64,
    mayhem: bool,
}

impl PumpPipeline {
    pub fn init(
        ws_url: String,
        config: RpcTransactionLogsConfig,
        general_tx: Sender<(Slot, Action, TokenBucket)>,
        sniper_threshold: u64,
        mayhem: bool,
    ) -> Self {
        Self {
            ws_url,
            config,
            general_tx,
            sniper_threshold,
            mayhem,
        }
    }

    pub fn run(&mut self) {
        let (mut actor, handler) = PumpLaunchpadStorageActor::new(self.sniper_threshold);

        let (pump_feed, mut pump_rx) = Feed::<PumpEvent>::new();
        let (pumpswap_feed, mut pumpswap_rx) = Feed::<PumpAmmEvent>::new();

        tokio::spawn(async move {
            actor.listen().await;
        });

        tokio::spawn(pump_feed.subscribe(self.ws_url.clone(), self.config.clone()));
        tokio::spawn(pumpswap_feed.subscribe(self.ws_url.clone(), self.config.clone()));

        tokio::spawn({
            let handler = handler.clone();
            let general_tx = self.general_tx.clone();
            let mayhem = self.mayhem;

            async move {
                while let Some((slot, event)) = pump_rx.recv().await {
                    let mint = event.mint();

                    if let PumpEvent::Create(ref create) = event {
                        if create.is_mayhem_mode != mayhem {
                            continue;
                        }
                    }

                    let (waittx, waitrx) = oneshot::channel();
                    if handler
                        .send(PumpLaunchpadCommand::Event((slot, event.clone()), waittx))
                        .await
                        .is_err()
                    {
                        continue;
                    }

                    if waitrx.await.is_err() {
                        continue;
                    }

                    let (etx, exists) = oneshot::channel();
                    let _ = handler
                        .send(PumpLaunchpadCommand::TokenExists {
                            mint: mint,
                            respond_to: etx,
                        })
                        .await
                        .unwrap();

                    let token_exists = match exists.await {
                        Ok(exists) => exists,
                        Err(_) => false,
                    };

                    if !token_exists {
                        println!("token wasnt found");
                        continue;
                    }

                    let (otx, orx) = oneshot::channel();
                    if handler
                        .send(PumpLaunchpadCommand::GetBucket {
                            mint,
                            respond_to: otx,
                        })
                        .await
                        .is_err()
                    {
                        continue;
                    }

                    let bucket = match orx.await {
                        Ok(swarm) => swarm,
                        Err(_) => continue,
                    };

                    let _ = general_tx.send((slot, event.into(), bucket)).await;
                }
            }
        });

        // tokio::spawn({
        //     let handler = handler.clone();
        //     let general_tx = self.general_tx.clone();

        //     async move {
        //         while let Some((slot, event)) = pumpswap_rx.recv().await {
        //             let pool = event.pool();
        //             let action = event.clone().into_general(pool);

        //             match &action {
        //                 Action::Create(_) => (),
        //                 Action::Trade(trade_action) => match trade_action {
        //                     crate::generalize::general_commands::TradeAction::Buy(general_buy) => {
        //                         // base mint and are swapped
        //                         // most of the time those are honeypots!
        //                         // most of the tokens never reach 1 dollars lets be honest
        //                         if general_buy.bought.to_float()
        //                             < general_buy.spent.amount().to_float()
        //                         {
        //                             continue;
        //                         }
        //                     }
        //                     crate::generalize::general_commands::TradeAction::Sell(
        //                         general_sell,
        //                     ) => {
        //                         // same thing here
        //                         if general_sell.sold.to_float()
        //                             < general_sell.received.amount().to_float()
        //                         {
        //                             continue;
        //                         }
        //                     }
        //                 },
        //             }

        //             let mint = match handler.get_mint(event.pool()).await {
        //                 Some(mint) => mint,
        //                 None => continue,
        //             };

        //             let (otx, orx) = oneshot::channel();
        //             if handler
        //                 .send(PumpLaunchpadCommand::GetBucket {
        //                     mint,
        //                     respond_to: otx,
        //                 })
        //                 .await
        //                 .is_err()
        //             {
        //                 continue;
        //             }

        //             let bucket = match orx.await {
        //                 Ok(swarm) => swarm,
        //                 Err(_) => continue,
        //             };

        //             let _ = general_tx.send((slot, action, bucket)).await;
        //         }
        //     }
        // });
    }
}
