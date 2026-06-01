use crate::{
    general::Slot, generalize::general_commands::Currency,
    persistence::error::Error, trading::trader::TraderType,
};

#[derive(Debug)]
pub struct TraderEntry {
    pub trader_address: String,
    pub coin_address: String,
    pub realized_pnl: f64,
    pub slot: Slot,
    pub is_buy: bool,
    pub market_cap: Currency,
    pub currency: Currency,
    pub role: TraderType,
}

#[derive(Debug, Type)]
#[sqlx(type_name = "currency_enum", rename_all = "lowercase")]
pub enum DbCurrency {
    Sol,
    Usd,
}

#[derive(Debug, Type)]
#[sqlx(type_name = "trader_role_enum", rename_all = "lowercase")]
pub enum DbTraderType {
    Creator,
    Sniper,
    Regular,
}

use sqlx::{Row, postgres::PgRow, prelude::Type};

impl<'r> sqlx::FromRow<'r, PgRow> for TraderEntry {
    fn from_row(row: &'r PgRow) -> Result<Self, sqlx::Error> {
        let slot_i64: i64 = row.try_get("slot")?;

        let db_currency: DbCurrency = row.try_get("currency")?;
        let db_trader_type: DbTraderType = row.try_get("role")?;
        let size: f64 = row.try_get("size")?;
        let market_cap: f64 = row.try_get("size")?;

        let trader = match db_trader_type {
            DbTraderType::Creator => TraderType::Creator,
            DbTraderType::Sniper => TraderType::Sniper,
            DbTraderType::Regular => TraderType::Regular,
        };

        let currency = match db_currency {
            DbCurrency::Sol => Currency::from_float_native(size),
            DbCurrency::Usd => Currency::from_float_usd(size),
        };

        let mcap_currency = match db_currency {
            DbCurrency::Sol => Currency::from_float_native(market_cap),
            DbCurrency::Usd => Currency::from_float_usd(market_cap),
        };

        Ok(TraderEntry {
            trader_address: row.try_get("trader_address")?,
            coin_address: row.try_get("coin_address")?,
            realized_pnl: row.try_get("realized_pnl")?,
            slot: slot_i64 as Slot,
            market_cap: mcap_currency,
            is_buy: row.try_get("is_buy")?,
            currency,
            role: trader,
        })
    }
}

pub struct TraderStatistics {
    pub winrate: f64,
    pub total_trades: u64,
    pub best_pnl: f64,
    pub worst_pnl: f64,

    pub active_from: Slot,
}

#[async_trait::async_trait]
pub trait TraderRepository {
    async fn save_trade(&self, entry: TraderEntry) -> Result<(), Error>;

    async fn get_trade(
        &self,
        trader_address: solana_address::Address,
        token_address: solana_address::Address,
    ) -> Option<TraderEntry>;

    async fn get_trader_stats(
        &self,
        trader_address: solana_address::Address,
    ) -> Result<TraderStatistics, Error>;
}
