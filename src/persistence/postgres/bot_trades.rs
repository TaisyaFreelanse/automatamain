use sqlx::{PgPool, Postgres};

use crate::persistence::{
    bot_trade_post_exit::{BotTradePostExitRepository, PostExitMetrics},
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
    async fn save_bot_trade(&self, entry: BotTradeEntry) -> Result<i64, Error> {
        let row: (i64,) = sqlx::query_as(
            r#"
            INSERT INTO bot_trades
                (mint, entry_mcap_sol, invested_sol, realized_pnl_pct, close_reason, entry_at, closed_at, exit_mcap_sol, entry_meta)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            RETURNING id
            "#,
        )
        .bind(&entry.mint)
        .bind(entry.entry_mcap_sol)
        .bind(entry.invested_sol)
        .bind(entry.realized_pnl_pct)
        .bind(&entry.close_reason)
        .bind(entry.entry_at)
        .bind(entry.closed_at)
        .bind(entry.exit_mcap_sol)
        .bind(&entry.entry_meta)
        .fetch_one(&self.pool)
        .await
        .map_err(Error::from)?;

        Ok(row.0)
    }
}

#[async_trait::async_trait]
impl BotTradePostExitRepository for BotTradesRepositoryPostgres {
    async fn update_post_exit_metrics(
        &self,
        trade_id: i64,
        metrics: &PostExitMetrics,
    ) -> Result<(), Error> {
        sqlx::query(
            r#"
            UPDATE bot_trades SET
                post_exit_mcap_10s = $2,
                post_exit_mcap_30s = $3,
                post_exit_mcap_50s = $4,
                post_exit_mcap_70s = $5,
                post_exit_mcap_100s = $6,
                post_exit_mcap_180s = $7,
                post_exit_mcap_240s = $8,
                post_exit_mcap_300s = $9,
                post_exit_mcap_5m = $10,
                post_exit_mcap_10m = $11,
                post_exit_mcap_15m = $12,
                post_exit_mcap_30m = $13,
                post_exit_max_mcap = $14,
                post_exit_min_mcap = $15,
                post_exit_time_to_max_secs = $16,
                post_exit_time_to_min_secs = $17,
                post_exit_pct_10s = $18,
                post_exit_pct_30s = $19,
                post_exit_pct_50s = $20,
                post_exit_pct_70s = $21,
                post_exit_pct_100s = $22,
                post_exit_pct_180s = $23,
                post_exit_pct_240s = $24,
                post_exit_pct_300s = $25,
                post_exit_pct_5m = $26,
                post_exit_pct_10m = $27,
                post_exit_pct_15m = $28,
                post_exit_pct_30m = $29,
                post_exit_max_pct = $30,
                post_exit_min_pct = $31,
                post_exit_tracking_done = true
            WHERE id = $1
            "#,
        )
        .bind(trade_id)
        .bind(metrics.post_exit_mcap_10s)
        .bind(metrics.post_exit_mcap_30s)
        .bind(metrics.post_exit_mcap_50s)
        .bind(metrics.post_exit_mcap_70s)
        .bind(metrics.post_exit_mcap_100s)
        .bind(metrics.post_exit_mcap_180s)
        .bind(metrics.post_exit_mcap_240s)
        .bind(metrics.post_exit_mcap_300s)
        .bind(metrics.post_exit_mcap_5m)
        .bind(metrics.post_exit_mcap_10m)
        .bind(metrics.post_exit_mcap_15m)
        .bind(metrics.post_exit_mcap_30m)
        .bind(metrics.post_exit_max_mcap)
        .bind(metrics.post_exit_min_mcap)
        .bind(metrics.post_exit_time_to_max_secs)
        .bind(metrics.post_exit_time_to_min_secs)
        .bind(metrics.post_exit_pct_10s)
        .bind(metrics.post_exit_pct_30s)
        .bind(metrics.post_exit_pct_50s)
        .bind(metrics.post_exit_pct_70s)
        .bind(metrics.post_exit_pct_100s)
        .bind(metrics.post_exit_pct_180s)
        .bind(metrics.post_exit_pct_240s)
        .bind(metrics.post_exit_pct_300s)
        .bind(metrics.post_exit_pct_5m)
        .bind(metrics.post_exit_pct_10m)
        .bind(metrics.post_exit_pct_15m)
        .bind(metrics.post_exit_pct_30m)
        .bind(metrics.post_exit_max_pct)
        .bind(metrics.post_exit_min_pct)
        .execute(&self.pool)
        .await
        .map_err(Error::from)?;
        Ok(())
    }
}
