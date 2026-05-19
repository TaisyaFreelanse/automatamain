use std::str::FromStr;

use base64::{Engine, engine::general_purpose};
use borsh::BorshDeserialize;
use solana_rpc_client_types::config::RpcTransactionLogsFilter;

use crate::feed::logs::log::{Error, HasLogsFilter};
use solana_address::Address;

#[derive(serde::Serialize, BorshDeserialize, Debug, Clone)]
pub enum PumpEvent {
    Create(CreateEvent),
    TradeEvent(TradeEvent),
}

impl PumpEvent {
    pub fn mint(&self) -> Address {
        match self {
            PumpEvent::Create(create_event) => create_event.mint,
            PumpEvent::TradeEvent(trade_event) => trade_event.mint,
        }
    }
}

impl HasLogsFilter for PumpEvent {
    const PROGRAM: &'static str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

    fn logs_filter() -> RpcTransactionLogsFilter {
        RpcTransactionLogsFilter::Mentions(vec![Self::PROGRAM.into()])
    }
}

const CREATE_EVENT: &[u8] = &[27, 114, 169, 77, 222, 235, 99, 118];
const TRADE_EVENT: &[u8] = &[189, 219, 127, 211, 78, 230, 97, 238];

impl FromStr for PumpEvent {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = match s.strip_prefix("Program data: ") {
            Some(s) => s,
            None => return Err(Error::InvalidLogEvent),
        };

        let blob = general_purpose::STANDARD.decode(s)?;
        if blob.len() < 8 {
            return Err(Error::InvalidLogEvent);
        }

        // 1. Make `data` mutable so we can pass `&mut data` to deserialize
        let (discriminator, mut data) = blob.split_at(8);

        match discriminator {
            TRADE_EVENT => {
                // 2. Call `deserialize` instead of `borsh::from_slice`
                let event = TradeEvent::deserialize(&mut data).map_err(Error::from)?;
                Ok(PumpEvent::TradeEvent(event))
            }
            CREATE_EVENT => {
                let event = CreateEvent::deserialize(&mut data).map_err(Error::from)?;
                Ok(PumpEvent::Create(event))
            }
            _ => Err(Error::InvalidDiscriminator),
        }
    }
}

#[derive(serde::Serialize, BorshDeserialize, Debug, Clone)]
pub struct CreateEvent {
    pub name: String,
    pub symbol: String,
    pub uri: String,
    pub mint: Address,
    pub bonding_curve: Address,
    pub user: Address,
    pub creator: Address,
    pub timestamp: i64,
    pub virtual_token_reserves: u64,
    pub virtual_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub token_total_supply: u64,
    pub token_program: Address,
    pub is_mayhem_mode: bool,
    pub is_cashback_enabled: bool,
    pub quote_mint: Address,
    pub virtual_quote_reserves: u64,
}

#[derive(serde::Serialize, BorshDeserialize, Debug, Clone)]
pub struct TradeEvent {
    pub mint: Address,
    pub sol_amount: u64,
    pub token_amount: u64,
    pub is_buy: bool,
    pub user: Address,
    pub timestamp: i64,
    pub virtual_sol_reserves: u64,
    pub virtual_token_reserves: u64,
    pub real_sol_reserves: u64,
    pub real_token_reserves: u64,
    pub fee_recipient: Address,
    pub fee_basis_points: u64,
    pub fee: u64,
    pub creator: Address,
    pub creator_fee_basis_points: u64,
    pub creator_fee: u64,
    pub track_volume: bool,
    pub total_unclaimed_tokens: u64,
    pub total_claimed_tokens: u64,
    pub current_sol_volume: u64,
    pub last_update_timestamp: i64,
    pub ix_name: String,
    pub mayhem_mode: bool,
    pub cashback_fee_basis_points: u64,
    pub cashback: u64,
    pub buyback_fee_basis_points: u64,
    pub buyback_fee: u64,
    pub shareholders: Vec<Address>,
    pub quote_amount: u64,
    pub virtual_quote_reserves: u64,
    pub real_quote_reserves: u64,
}
