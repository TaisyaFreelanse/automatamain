use async_trait::async_trait;
use sqlx::{PgPool, Postgres, Row};

use solana_address::Address;

use crate::{
    generalize::general_commands::Currency,
    persistence::{
        creators::{CreatorRepository, CreatorStatistics},
        error::Error,
    },
};

pub struct CreatorsRepositoryPostgres {
    pool: sqlx::Pool<Postgres>,
}

impl CreatorsRepositoryPostgres {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl CreatorRepository for CreatorsRepositoryPostgres {
    async fn get_creator_stats_in_sol(
        &self,
        dev_address: Address,
    ) -> Result<Option<CreatorStatistics>, Error> {
        let dev_address = dev_address.to_string();

        let row = sqlx::query(
            r#"
            WITH creator_coins AS (
                SELECT coin_address
                FROM coins
                WHERE developer = $1
            ),
            token_stats AS (
                SELECT
                    cc.coin_address,

                    MAX(t.market_cap::double precision) AS ath_market_cap,
                    SUM(t.size::double precision) AS volume,

                    COUNT(*) AS total_trades,

                    COUNT(DISTINCT t.trader_address) FILTER (WHERE t.is_buy) AS unique_buy_wallets,
                    COUNT(DISTINCT t.trader_address) FILTER (WHERE NOT t.is_buy) AS unique_sell_wallets,

                    AVG(t.size::double precision) FILTER (WHERE t.is_buy) AS avg_buy_size

                FROM creator_coins cc
                LEFT JOIN trades t
                  ON t.coin_address = cc.coin_address
                 AND t.currency = 'sol'
                 AND t.role = 'regular'

                GROUP BY cc.coin_address
            ),
            trader_last_trade AS (
                SELECT DISTINCT ON (t.trader_address)
                    t.trader_address,
                    t.pnl::double precision AS pnl
                FROM trades t
                JOIN creator_coins cc
                  ON cc.coin_address = t.coin_address
                WHERE t.role = 'regular'
                  AND t.currency = 'sol'
                ORDER BY t.trader_address, t.slot_time DESC, t.id DESC
            )
            SELECT
                COALESCE(
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY ath_market_cap),
                    0.0
                ) AS median_market_cap,

                COALESCE(
                    (SELECT AVG(pnl) FROM trader_last_trade),
                    0.0
                ) AS trader_pnl_average,

                COALESCE(
                    AVG(unique_buy_wallets::double precision),
                    0.0
                ) AS total_holders_average,

                COALESCE(
                    AVG(COALESCE(volume, 0.0)),
                    0.0
                ) AS average_volume,

                COALESCE(
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY total_trades),
                    0.0
                ) AS median_total_trades,

                COALESCE(
                    AVG(
                        unique_buy_wallets::double precision
                        / NULLIF(unique_sell_wallets::double precision, 0.0)
                    ),
                    0.0
                ) AS average_unique_buy_to_sell_ratio,

                COALESCE(
                    AVG(COALESCE(avg_buy_size, 0.0)),
                    0.0
                ) AS average_buy_trader_size,

                (SELECT COUNT(*) FROM creator_coins) AS total_coins

            FROM token_stats;
            "#,
        )
        .bind(&dev_address)
        .fetch_one(&self.pool)
        .await?;

        let total_coins: i64 = row.get("total_coins");

        if total_coins == 0 {
            return Ok(None);
        }

        Ok(Some(CreatorStatistics {
            median_market_cap: Currency::from_float_native(row.get::<f64, _>("median_market_cap")),
            trader_pnl_average: row.get("trader_pnl_average"),
            total_holders_average: row.get::<f64, _>("total_holders_average").round() as u64,
            average_volume: row.get("average_volume"),
            median_total_trades: row.get::<f64, _>("median_total_trades").round() as u64,
            average_unique_buy_to_sell_ratio: row.get("average_unique_buy_to_sell_ratio"),
            average_buy_trader_size: Currency::from_float_native(
                row.get::<f64, _>("average_buy_trader_size"),
            ),
            total_coins: total_coins as u64,
        }))
    }
}
