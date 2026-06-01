use crate::persistence::error::Error;

pub struct BotTradeEntry {
    pub wallet_id: String,
    pub mint: String,
    pub entry_mcap_sol: f64,
    pub invested_sol: f64,
    pub realized_pnl_pct: f64,
    pub close_reason: String,
    pub entry_at: i64,
    pub closed_at: i64,
    pub exit_mcap_sol: f64,
    /// JSON of `V3TapeWire` at entry (empty string if unknown).
    pub entry_meta: String,
}

#[derive(serde::Serialize, sqlx::FromRow)]
pub struct BotTradeRow {
    pub id: i64,
    pub wallet_id: String,
    pub mint: String,
    pub entry_mcap_sol: f64,
    pub invested_sol: f64,
    pub realized_pnl_pct: f64,
    pub close_reason: String,
    pub entry_at: i64,
    pub closed_at: i64,
    pub exit_mcap_sol: f64,
    pub entry_meta: String,
    pub post_exit_mcap_10s: Option<f64>,
    pub post_exit_mcap_30s: Option<f64>,
    pub post_exit_mcap_50s: Option<f64>,
    pub post_exit_mcap_70s: Option<f64>,
    pub post_exit_mcap_100s: Option<f64>,
    pub post_exit_mcap_180s: Option<f64>,
    pub post_exit_mcap_240s: Option<f64>,
    pub post_exit_mcap_300s: Option<f64>,
    pub post_exit_mcap_5m: Option<f64>,
    pub post_exit_mcap_10m: Option<f64>,
    pub post_exit_mcap_15m: Option<f64>,
    pub post_exit_mcap_30m: Option<f64>,
    pub post_exit_max_mcap: Option<f64>,
    pub post_exit_min_mcap: Option<f64>,
    pub post_exit_time_to_max_secs: Option<i64>,
    pub post_exit_time_to_min_secs: Option<i64>,
    pub post_exit_pct_10s: Option<f64>,
    pub post_exit_pct_30s: Option<f64>,
    pub post_exit_pct_50s: Option<f64>,
    pub post_exit_pct_70s: Option<f64>,
    pub post_exit_pct_100s: Option<f64>,
    pub post_exit_pct_180s: Option<f64>,
    pub post_exit_pct_240s: Option<f64>,
    pub post_exit_pct_300s: Option<f64>,
    pub post_exit_pct_5m: Option<f64>,
    pub post_exit_pct_10m: Option<f64>,
    pub post_exit_pct_15m: Option<f64>,
    pub post_exit_pct_30m: Option<f64>,
    pub post_exit_max_pct: Option<f64>,
    pub post_exit_min_pct: Option<f64>,
    pub post_exit_tracking_done: bool,
}

#[async_trait::async_trait]
pub trait BotTradeRepository {
    async fn save_bot_trade(&self, entry: BotTradeEntry) -> Result<i64, Error>;
}
