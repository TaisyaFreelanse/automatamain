use std::sync::Arc;

use solana_rpc_client_types::config::RpcTransactionLogsConfig;
use tokio::sync::{mpsc::Sender, oneshot};

use crate::{
    feed::{
        feed::Feed,
        logs::{
            pump::PumpEvent,
            pump_amm::PumpAmmEvent,
        },
        metrics::{FeedMetrics, SharedDedup},
    },
    general::Slot,
    generalize::general_commands::Action,
    launchpads::{
        pump::launchpad::{PumpLaunchpadCommand, PumpLaunchpadStorageActor},
        token_bucket::TokenBucket,
    },
};

pub struct PumpPipeline {
    ws_url: String,
    config: RpcTransactionLogsConfig,
    general_tx: Sender<(Slot, Action, TokenBucket)>,

    sniper_threshold: u64,
    mayhem: bool,

    pump_metrics: Arc<FeedMetrics>,
    pumpswap_metrics: Arc<FeedMetrics>,
    dedup: SharedDedup,
    enable_pumpswap: bool,
    launchpad_tx: Sender<PumpLaunchpadCommand>,
}

impl PumpPipeline {
    #[allow(clippy::too_many_arguments)]
    pub fn init(
        ws_url: String,
        config: RpcTransactionLogsConfig,
        general_tx: Sender<(Slot, Action, TokenBucket)>,
        sniper_threshold: u64,
        mayhem: bool,
        pump_metrics: Arc<FeedMetrics>,
        pumpswap_metrics: Arc<FeedMetrics>,
        dedup: SharedDedup,
        enable_pumpswap: bool,
    ) -> Self {
        let (mut actor, launchpad_tx) = PumpLaunchpadStorageActor::new(sniper_threshold);
        tokio::spawn(async move {
            actor.listen().await;
        });

        Self {
            ws_url,
            config,
            general_tx,
            sniper_threshold,
            mayhem,
            pump_metrics,
            pumpswap_metrics,
            dedup,
            enable_pumpswap,
            launchpad_tx,
        }
    }

    pub fn launchpad(&self) -> &Sender<PumpLaunchpadCommand> {
        &self.launchpad_tx
    }

    pub fn run(&mut self) {
        let handler = self.launchpad_tx.clone();

        let (pump_feed, mut pump_rx) = Feed::<PumpEvent>::with_metrics(self.pump_metrics.clone());
        let (pumpswap_feed, _pumpswap_rx) =
            Feed::<PumpAmmEvent>::with_metrics(self.pumpswap_metrics.clone());

        tokio::spawn(pump_feed.subscribe(
            self.ws_url.clone(),
            self.config.clone(),
            self.dedup.clone(),
        ));

        // The PumpSwap consumer below is intentionally disabled (commented
        // out). Subscribing to PumpSwap logs while nothing reads them just
        // burns Helius credits, so we gate the subscription behind a flag.
        if self.enable_pumpswap {
            tokio::spawn(pumpswap_feed.subscribe(
                self.ws_url.clone(),
                self.config.clone(),
                self.dedup.clone(),
            ));
        } else {
            drop(pumpswap_feed);
            println!("[pipeline] pumpswap feed disabled (no consumer)");
        }

        tokio::spawn({
            let handler = handler.clone();
            let general_tx = self.general_tx.clone();
            let mayhem = self.mayhem;

            async move {
                while let Some((slot, event)) = pump_rx.recv().await {
                    let mint = event.mint();

                    if let PumpEvent::Create(ref create) = event
                        && create.is_mayhem_mode != mayhem {
                            continue;
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
                    handler
                        .send(PumpLaunchpadCommand::TokenExists {
                            mint,
                            respond_to: etx,
                        })
                        .await
                        .unwrap();

                    let token_exists: bool = exists.await.unwrap_or_default();

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
