use sqlx::{PgPool, Postgres};

use crate::{
    generalize::general_commands::Currency,
    persistence::{
        error::Error,
        traders::{DbCurrency, DbTraderType, TraderEntry, TraderRepository, TraderStatistics},
    },
};

pub struct TraderRepositoryPostgres {
    pool: sqlx::Pool<Postgres>,
}

impl TraderRepositoryPostgres {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl TraderRepository for TraderRepositoryPostgres {
    async fn save_trade(&self, entry: TraderEntry) -> Result<(), Error> {
        let mut tx = self.pool.begin().await.map_err(Error::from)?;

        // 1. Ensure the trader exists
        sqlx::query(
            r#"
            INSERT INTO traders (trader_address, active_from_slot)
            VALUES ($1, $2)
            ON CONFLICT (trader_address) DO NOTHING
            "#,
        )
        .bind(&entry.trader_address)
        .bind(entry.slot as i64)
        .execute(&mut *tx)
        .await
        .map_err(Error::from)?;

        // 2. Destructure Domain Enum into flat DB primitives
        let (_, size) = match &entry.currency {
            Currency::Native(amt) => (DbCurrency::Sol, amt.to_float()),
            Currency::Dollar(amt) => (DbCurrency::Usd, amt.to_float()),
        };

        let (db_currency, market_cap) = match &entry.market_cap {
            Currency::Native(amt) => (DbCurrency::Sol, amt.to_float()),
            Currency::Dollar(amt) => (DbCurrency::Usd, amt.to_float()),
        };

        let db_role: DbTraderType = match entry.role {
            crate::trading::trader::TraderType::Creator => DbTraderType::Creator,
            crate::trading::trader::TraderType::Sniper => DbTraderType::Sniper,
            crate::trading::trader::TraderType::Regular => DbTraderType::Regular,
        };

        sqlx::query(
            r#"
            INSERT INTO trades (
                trader_address, coin_address, pnl, slot_time,
                is_buy, market_cap, currency, size, role
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(&entry.trader_address)
        .bind(&entry.coin_address)
        .bind(entry.realized_pnl)
        .bind(entry.slot as i64)
        .bind(entry.is_buy)
        .bind(market_cap)
        .bind(db_currency) // Binds to Postgres enum directly
        .bind(size)
        .bind(db_role)
        .execute(&mut *tx)
        .await
        .map_err(Error::from)?;

        tx.commit().await.map_err(Error::from)?;

        Ok(())
    }

    async fn get_trade(
        &self,
        trader_address: solana_address::Address,
        token_address: solana_address::Address,
    ) -> Option<TraderEntry> {
        let trader_addr_str = trader_address.to_string();
        let token_addr_str = token_address.to_string();

        sqlx::query_as::<_, TraderEntry>(
            r#"
            SELECT
                trader_address,
                coin_address,
                CAST(pnl AS FLOAT8) AS realized_pnl,
                CAST(slot_time AS BIGINT) AS slot,
                is_buy,
                market_cap,
                currency,
                size
            FROM trades
            WHERE trader_address = $1 AND coin_address = $2
            ORDER BY slot_time DESC
            LIMIT 1
            "#,
        )
        .bind(trader_addr_str)
        .bind(token_addr_str)
        .fetch_optional(&self.pool)
        .await
        .unwrap_or(None)
    }

    async fn get_trader_stats(
        &self,
        trader_address: solana_address::Address,
    ) -> Result<TraderStatistics, Error> {
        let addr_str = trader_address.to_string();

        #[derive(sqlx::FromRow)]
        struct StatsRow {
            winrate: Option<f64>,
            total_trades: Option<i64>,
            best_pnl: Option<f64>,
            worst_pnl: Option<f64>,
            active_from: Option<i64>,
        }

        let row = sqlx::query_as::<_, StatsRow>(
            r#"
            SELECT
                SUM((pnl > 0)::int)::float8 / NULLIF(COUNT(pnl), 0) AS winrate,
                COUNT(pnl) AS total_trades,
                MAX(pnl)::float8 AS best_pnl,
                MIN(pnl)::float8 AS worst_pnl,
                MIN(slot_time) AS active_from
            FROM trades
            WHERE trader_address = $1
              AND pnl IS NOT NULL
            "#,
        )
        .bind(&addr_str)
        .fetch_one(&self.pool)
        .await?;

        let total_trades = row.total_trades.unwrap_or_default();

        if total_trades == 0 {
            return Ok(TraderStatistics {
                winrate: 0.0,
                total_trades: 0,
                best_pnl: 0.0,
                worst_pnl: 0.0,
                active_from: 0,
            });
        }

        Ok(TraderStatistics {
            winrate: row.winrate.unwrap_or(0.0),
            total_trades: total_trades as u64,
            best_pnl: row.best_pnl.unwrap_or(0.0),
            worst_pnl: row.worst_pnl.unwrap_or(0.0),
            active_from: row.active_from.unwrap_or(0) as crate::general::Slot,
        })
    }
}
