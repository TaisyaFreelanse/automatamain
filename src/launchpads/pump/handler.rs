use async_trait::async_trait;
use solana_address::Address;
use tokio::sync::oneshot;

use crate::{
    feed::logs::pump::PumpEvent,
    launchpads::pump::launchpad::{AmmPoolAddress, PumpLaunchpadCommand},
};

#[async_trait]
pub trait PumpLaunchpadSenderExt {
    async fn send_event(&self, event: PumpEvent, slot: u64, finish: oneshot::Sender<()>);
    async fn get_mint(&self, amm_pool: AmmPoolAddress) -> Option<Address>;
}

#[async_trait]
impl PumpLaunchpadSenderExt for tokio::sync::mpsc::Sender<PumpLaunchpadCommand> {
    async fn send_event(&self, event: PumpEvent, slot: u64, finish: oneshot::Sender<()>) {
        let _ = self.try_send(PumpLaunchpadCommand::Event((slot, event), finish));
    }

    async fn get_mint(&self, amm_pool: AmmPoolAddress) -> Option<Address> {
        let (tx, rx) = oneshot::channel();

        if let Err(_) = self
            .send(PumpLaunchpadCommand::GetMint {
                amm_pool,
                respond_to: tx,
            })
            .await
        {
            return None;
        }

        rx.await.ok().flatten()
    }
}
