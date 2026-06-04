use std::str::FromStr;

use base64::{Engine, engine::general_purpose};
use borsh::BorshDeserialize;
use solana_rpc_client_types::config::RpcTransactionLogsFilter;

use crate::{
    feed::logs::log::{Error, HasLogsFilter},
    generalize::general_commands::{
        Action, Currency, GeneralBuy, GeneralCreate, GeneralSell, TradeAction,
    },
    helper::Amount,
    launchpads::pump::general::PRECISION,
};
use solana_address::Address;

#[derive(serde::Serialize, BorshDeserialize, Debug, Clone)]
pub enum PumpAmmEvent {
    Buy(BuyEvent),
    Sell(SellEvent),
    CreatePool(CreatePoolEvent),
}

impl PumpAmmEvent {
    pub fn pool(&self) -> Address {
        match self {
            PumpAmmEvent::Buy(buy_event) => buy_event.pool,
            PumpAmmEvent::Sell(sell_event) => sell_event.pool,
            PumpAmmEvent::CreatePool(create_pool_event) => create_pool_event.pool,
        }
    }

    pub fn into_general(self, mint: Address) -> Action {
        match self {
            PumpAmmEvent::Buy(buy_event) => Action::Trade(TradeAction::Buy(GeneralBuy {
                mint,
                user: buy_event.user,
                bought: Amount::from_raw(buy_event.pool_quote_token_reserves, PRECISION),
                spent: Currency::Native(Amount::from_raw_native(buy_event.quote_amount_in)),
            })),

            PumpAmmEvent::Sell(sell_event) => Action::Trade(TradeAction::Sell(GeneralSell {
                mint,
                user: sell_event.user,
                sold: Amount::from_raw(sell_event.base_amount_in, PRECISION),
                received: Currency::Native(Amount::from_raw_native(sell_event.base_amount_in)),
            })),
            PumpAmmEvent::CreatePool(create_pool_event) => Action::Create(GeneralCreate {
                mint,
                user: create_pool_event.creator,
                metadata: None,
                quote_mint: create_pool_event.quote_mint,
            }),
        }
    }
}

impl HasLogsFilter for PumpAmmEvent {
    const PROGRAM: &'static str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

    fn logs_filter() -> RpcTransactionLogsFilter {
        RpcTransactionLogsFilter::Mentions(vec![Self::PROGRAM.into()])
    }
}

const BUY_EVENT: &[u8] = &[103, 244, 82, 31, 44, 245, 119, 119];
const SELL_EVENT: &[u8] = &[62, 47, 55, 10, 165, 3, 220, 42];
const CREATE_POOL_EVENT: &[u8] = &[177, 49, 12, 210, 160, 118, 167, 116];

impl FromStr for PumpAmmEvent {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s
            .strip_prefix("Program data: ")
            .ok_or(Error::InvalidLogEvent)?;

        let blob = general_purpose::STANDARD.decode(s)?;
        if blob.len() < 8 {
            return Err(Error::InvalidLogEvent);
        }
        let (disc, data) = blob.split_at(8);

        match disc {
            BUY_EVENT => Ok(Self::Buy(borsh::from_slice(data)?)),
            SELL_EVENT => Ok(Self::Sell(borsh::from_slice(data)?)),
            CREATE_POOL_EVENT => Ok(Self::CreatePool(borsh::from_slice(data)?)),
            _ => Err(Error::InvalidDiscriminator),
        }
    }
}

#[derive(serde::Serialize, BorshDeserialize, Debug, Clone)]
pub struct CreatePoolEvent {
    pub pool: Address,
    pub creator: Address,
    pub base_mint: Address,
    pub quote_mint: Address,
    pub base_amount: u64,
    pub quote_amount: u64,
    pub lp_mint: Address,
    pub lp_supply: u64,
    pub timestamp: i64,
}

#[derive(serde::Serialize, BorshDeserialize, Debug, Clone)]
pub struct BuyEvent {
    pub timestamp: i64,
    pub base_amount_out: u64,
    pub max_quote_amount_in: u64,
    pub user_base_token_reserves: u64,
    pub user_quote_token_reserves: u64,
    pub pool_base_token_reserves: u64,
    pub pool_quote_token_reserves: u64,
    pub quote_amount_in: u64,
    pub lp_fee_basis_points: u64,
    pub lp_fee: u64,
    pub protocol_fee_basis_points: u64,
    pub protocol_fee: u64,
    pub quote_amount_in_with_lp_fee: u64,
    pub user_quote_amount_in: u64,
    pub pool: Address,
    pub user: Address,
    pub user_base_token_account: Address,
    pub user_quote_token_account: Address,
    pub protocol_fee_recipient: Address,
    pub protocol_fee_recipient_token_account: Address,
    pub coin_creator: Address,
    pub coin_creator_fee_basis_points: u64,
    pub coin_creator_fee: u64,
    pub track_volume: bool,
    pub total_unclaimed_tokens: u64,
    pub total_claimed_tokens: u64,
    pub current_sol_volume: u64,
    pub last_update_timestamp: i64,
    pub min_base_amount_out: u64,
    pub ix_name: String,
    pub cashback_fee_basis_points: u64,
    pub cashback: u64,
}

#[derive(serde::Serialize, BorshDeserialize, Debug, Clone)]
pub struct SellEvent {
    pub timestamp: i64,
    pub base_amount_in: u64,
    pub min_quote_amount_out: u64,

    pub user_base_token_reserves: u64,
    pub user_quote_token_reserves: u64,
    pub pool_base_token_reserves: u64,
    pub pool_quote_token_reserves: u64,

    pub quote_amount_out: u64,
    pub quote_amount_out_without_lp_fee: u64,

    pub lp_fee_basis_points: u64,
    pub lp_fee: u64,

    pub protocol_fee_basis_points: u64,
    pub protocol_fee: u64,

    pub user_quote_amount_out: u64,

    pub pool: Address,
    pub user: Address,

    pub user_base_token_account: Address,
    pub user_quote_token_account: Address,

    pub protocol_fee_recipient: Address,
    pub protocol_fee_recipient_token_account: Address,

    pub coin_creator: Address,
    pub coin_creator_fee_basis_points: u64,
    pub coin_creator_fee: u64,
}
