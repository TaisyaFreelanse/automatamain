use tokio::sync::mpsc::{Receiver, Sender};

pub async fn generalize<T, U>(tx: Sender<T>, mut rx: Receiver<U>)
where
    U: Into<T> + Send + 'static,
    T: Send + 'static,
{
    while let Some(data) = rx.recv().await {
        if tx.send(data.into()).await.is_err() {
            break;
        }
    }
}
