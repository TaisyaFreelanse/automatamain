use solana_address::Address;
use sqlx::{PgPool, Postgres, Row};
use std::str::FromStr;

use crate::{
    general::Slot,
    persistence::{
        error::Error,
        tokens::{TokenData, TokenRepository},
    },
};

pub struct TokenRepositoryPostgres {
    pool: sqlx::Pool<Postgres>,
}

impl TokenRepositoryPostgres {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

#[async_trait::async_trait]
impl TokenRepository for TokenRepositoryPostgres {
    async fn save_token(&self, coin: Address, developer: Address, slot: Slot) -> Result<(), Error> {
        let coin_str = coin.to_string();
        let dev_str = developer.to_string();
        let slot_i64 = slot as i64;

        sqlx::query(
            r#"
            WITH ins_dev AS (
                INSERT INTO developers (developer_address, active_from_slot)
                VALUES ($2, $3)
                ON CONFLICT (developer_address) DO NOTHING
            )
            INSERT INTO coins (coin_address, developer, created_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (coin_address) DO NOTHING
            "#,
        )
        .bind(&coin_str)
        .bind(&dev_str)
        .bind(slot_i64)
        .execute(&self.pool)
        .await
        .map_err(Error::from)?;

        Ok(())
    }

    async fn get_token(&self, coin: Address) -> Option<TokenData> {
        let coin_str = coin.to_string();

        // Query the database for the coin
        let row_result = sqlx::query(
            r#"
            SELECT coin_address, created_at
            FROM coins
            WHERE coin_address = $1
            "#,
        )
        .bind(coin_str)
        .fetch_optional(&self.pool)
        .await;

        // Extract the row if it exists and there was no DB error
        let row = match row_result {
            Ok(Some(r)) => r,
            _ => return None, // Return None if not found or if a database error occurred
        };

        // Get the values out of the Postgres Row
        let mint_str: String = row.try_get("coin_address").ok()?;
        let created_at_i64: i64 = row.try_get("created_at").ok()?;

        // Convert the String back into a solana_address::Address
        // Assuming Address implements std::str::FromStr
        let mint = Address::from_str(&mint_str).ok()?;

        Some(TokenData {
            mint,
            created_at: created_at_i64 as Slot,
        })
    }
}
