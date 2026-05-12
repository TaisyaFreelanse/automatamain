use tokio::sync::mpsc::{self, Receiver, Sender};

use crate::general::Slot;

#[derive(Clone)]
pub struct Feed<T> {
    pub tx: Sender<(Slot, T)>,
}

impl<T> Feed<T> {
    pub fn new() -> (Self, Receiver<(Slot, T)>) {
        let (tx, rx) = mpsc::channel(4096);

        (Self { tx }, rx)
    }
}
