use crate::persistence::error::Error;

pub struct BotTradeEntry {
    pub mint: String,
    pub entry_mcap_sol: f64,
    pub invested_sol: f64,
    pub realized_pnl_pct: f64,
    pub close_reason: String,
    pub closed_at: i64,
    pub exit_mcap_sol: f64,
}

#[derive(serde::Serialize, sqlx::FromRow)]
pub struct BotTradeRow {
    pub id: i64,
    pub mint: String,
    pub entry_mcap_sol: f64,
    pub invested_sol: f64,
    pub realized_pnl_pct: f64,
    pub close_reason: String,
    pub closed_at: i64,
    pub exit_mcap_sol: f64,
}

#[async_trait::async_trait]
pub trait BotTradeRepository {
    async fn save_bot_trade(&self, entry: BotTradeEntry) -> Result<(), Error>;
}
