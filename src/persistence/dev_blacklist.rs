use crate::persistence::error::Error;

#[derive(Clone, Debug)]
pub struct DevBlacklistEntry {
    pub dev_wallet: String,
    pub reason: String,
    pub mint: String,
    pub pnl_sol: f64,
    pub close_reason: String,
    pub created_at: i64,
    pub expires_at: i64,
}

#[derive(Clone, Debug)]
pub struct ActiveDevBlacklist {
    pub reason: String,
    pub mint: String,
    pub pnl_sol: f64,
    pub close_reason: String,
    pub created_at: i64,
    pub expires_at: i64,
}

#[async_trait::async_trait]
pub trait DevBlacklistRepository {
    async fn insert(&self, entry: DevBlacklistEntry) -> Result<(), Error>;

    /// Latest non-expired row for this dev, if any.
    async fn active_for_dev(&self, dev_wallet: &str, now_unix: i64) -> Result<Option<ActiveDevBlacklist>, Error>;
}
