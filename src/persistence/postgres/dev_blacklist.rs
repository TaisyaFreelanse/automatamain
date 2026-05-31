use sqlx::PgPool;

use crate::persistence::{
    dev_blacklist::{ActiveDevBlacklist, DevBlacklistEntry, DevBlacklistRepository},
    error::Error,
};

pub struct DevBlacklistRepositoryPostgres {
    pool: PgPool,
}

impl DevBlacklistRepositoryPostgres {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl DevBlacklistRepository for DevBlacklistRepositoryPostgres {
    async fn insert(&self, entry: DevBlacklistEntry) -> Result<(), Error> {
        sqlx::query(
            r#"
            INSERT INTO dev_blacklist
                (dev_wallet, reason, mint, pnl_sol, close_reason, created_at, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(&entry.dev_wallet)
        .bind(&entry.reason)
        .bind(&entry.mint)
        .bind(entry.pnl_sol)
        .bind(&entry.close_reason)
        .bind(entry.created_at)
        .bind(entry.expires_at)
        .execute(&self.pool)
        .await
        .map_err(Error::from)?;
        Ok(())
    }

    async fn active_for_dev(&self, dev_wallet: &str, now_unix: i64) -> Result<Option<ActiveDevBlacklist>, Error> {
        let row: Option<(
            String,
            String,
            f64,
            String,
            i64,
            i64,
        )> = sqlx::query_as(
            r#"
            SELECT reason, mint, pnl_sol, close_reason, created_at, expires_at
            FROM dev_blacklist
            WHERE dev_wallet = $1 AND (expires_at = 0 OR expires_at > $2)
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )
        .bind(dev_wallet)
        .bind(now_unix)
        .fetch_optional(&self.pool)
        .await
        .map_err(Error::from)?;

        Ok(row.map(
            |(reason, mint, pnl_sol, close_reason, created_at, expires_at)| ActiveDevBlacklist {
                reason,
                mint,
                pnl_sol,
                close_reason,
                created_at,
                expires_at,
            },
        ))
    }
}
