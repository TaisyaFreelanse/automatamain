use crate::{feed::logs::pump_amm::PumpAmmEvent, generalize::general_commands::Action};
use tokio::sync::mpsc::{Receiver, Sender};

//exists due to lack of mint field in pump amm log
pub async fn generalize_pump_amm(_tx: Sender<Action>, mut rx: Receiver<PumpAmmEvent>) {
    while let Some(_data) = rx.recv().await {
        todo!()
    }
}
