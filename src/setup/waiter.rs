use solana_address::Address;
use std::collections::HashMap;
use tokio::sync::{mpsc, oneshot};

pub enum WaiterMessage {
    NotifyCreated(Address),
    WaitUntilCreated(Address, oneshot::Sender<()>),
}

enum TokenState {
    Created,
    Waiting(Vec<oneshot::Sender<()>>),
}

pub struct DatabaseCreateWaiter {
    state: HashMap<Address, TokenState>,
    receiver: mpsc::Receiver<WaiterMessage>,
}

impl DatabaseCreateWaiter {
    pub fn new() -> (Self, WaiterHandle) {
        let (sender, receiver) = mpsc::channel(1024);
        let actor = Self {
            state: HashMap::new(),
            receiver,
        };
        let handle = WaiterHandle { sender };
        (actor, handle)
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.receiver.recv().await {
            match msg {
                WaiterMessage::NotifyCreated(address) => {
                    if let Some(TokenState::Waiting(waiters)) = self.state.remove(&address) {
                        for waiter in waiters {
                            let _ = waiter.send(());
                        }
                    }

                    self.state.insert(address, TokenState::Created);
                }
                WaiterMessage::WaitUntilCreated(address, reply_tx) => {
                    match self.state.get_mut(&address) {
                        Some(TokenState::Created) => {
                            let _ = reply_tx.send(());
                        }
                        Some(TokenState::Waiting(waiters)) => {
                            waiters.push(reply_tx);
                        }
                        None => {
                            self.state
                                .insert(address, TokenState::Waiting(vec![reply_tx]));
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
pub struct WaiterHandle {
    sender: mpsc::Sender<WaiterMessage>,
}

impl WaiterHandle {
    pub async fn notify_created(&self, address: Address) {
        let _ = self
            .sender
            .send(WaiterMessage::NotifyCreated(address))
            .await;
    }

    pub async fn wait_for(&self, address: Address) {
        let (tx, rx) = oneshot::channel();
        let _ = self
            .sender
            .send(WaiterMessage::WaitUntilCreated(address, tx))
            .await;
        let _ = rx.await;
    }
}
