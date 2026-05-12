use sqlx::{PgPool, Postgres};

use crate::persistence::{
    bot_trades::{BotTradeEntry, BotTradeRepository},
    error::Error,
};

pub struct BotTradesRepositoryPostgres {
    pool: sqlx::Pool<Postgres>,
}

impl BotTradesRepositoryPostgres {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl BotTradeRepository for BotTradesRepositoryPostgres {
    async fn save_bot_trade(&self, entry: BotTradeEntry) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO bot_trades
                (mint, entry_mcap_sol, invested_sol, realized_pnl_pct, close_reason, closed_at, exit_mcap_sol)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(&entry.mint)
        .bind(entry.entry_mcap_sol)
        .bind(entry.invested_sol)
        .bind(entry.realized_pnl_pct)
        .bind(&entry.close_reason)
        .bind(entry.closed_at)
        .bind(entry.exit_mcap_sol)
        .execute(&self.pool)
        .await
        .map_err(Error::from)?;

        Ok(())
    }
}
