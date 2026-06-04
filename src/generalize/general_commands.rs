use std::fmt::Display;

use crate::{helper::Amount, trading::offer::Offer};
use serde::{Deserialize, Serialize};
use solana_address::Address;

#[derive(Debug)]
pub enum Action {
    Create(GeneralCreate),
    Trade(TradeAction),
}

impl Action {
    pub fn mint(&self) -> Address {
        match self {
            Action::Create(general_create) => general_create.mint,
            Action::Trade(trade_action) => match trade_action {
                TradeAction::Buy(general_buy) => general_buy.mint,
                TradeAction::Sell(general_sell) => general_sell.mint,
            },
        }
    }
}

#[derive(Debug)]
pub enum TradeAction {
    Buy(GeneralBuy),
    Sell(GeneralSell),
}

impl TradeAction {
    pub fn mint(&self) -> Address {
        match self {
            TradeAction::Buy(general_buy) => general_buy.mint,
            TradeAction::Sell(general_sell) => general_sell.mint,
        }
    }

    pub fn trader(&self) -> Address {
        match self {
            TradeAction::Buy(general_buy) => general_buy.user,
            TradeAction::Sell(general_sell) => general_sell.user,
        }
    }

    pub fn size(&self) -> Currency {
        match self {
            TradeAction::Buy(general_buy) => general_buy.spent,
            TradeAction::Sell(general_sell) => general_sell.received,
        }
    }

    pub fn is_buy(&self) -> bool {
        match self {
            TradeAction::Buy(_general_buy) => true,
            TradeAction::Sell(_general_sell) => false,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum Currency {
    Native(Amount),
    Dollar(Amount),
}

impl Currency {
    pub fn collumn_name(&self) -> &'static str {
        match self {
            Currency::Native(_) => "sol",
            Currency::Dollar(_) => "usd",
        }
    }

    pub fn amount(&self) -> Amount {
        match self {
            Currency::Native(amount) => *amount,
            Currency::Dollar(amount) => *amount,
        }
    }

    pub fn from_float_native(amount: f64) -> Currency {
        Currency::Native(Amount::from_float_native(amount))
    }

    pub fn from_float_usd(amount: f64) -> Currency {
        Currency::Dollar(Amount::from_float_native(amount))
    }
}

impl Display for Currency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Currency::Native(amount) => write!(f, "{} SOL", amount),
            Currency::Dollar(amount) => write!(f, "{}$", amount),
        }
    }
}

impl From<Currency> for u64 {
    fn from(value: Currency) -> Self {
        match value {
            Currency::Native(amount) => amount.raw(),
            Currency::Dollar(amount) => amount.raw(),
        }
    }
}

#[derive(Debug)]
pub struct GeneralMetadata {
    pub name: String,
    pub ticker: String,
    pub uri: String,
}

/// Pump bonding-curve / pool quote mint the bot supports for live entry (SOL only).
pub const NATIVE_SOL_QUOTE_MINT: Address =
    Address::from_str_const("So11111111111111111111111111111111111111112");

/// Legacy pump `Create` events leave `quote_mint` unset (defaults to system program).
pub const LEGACY_SOL_QUOTE_MINT: Address =
    Address::from_str_const("11111111111111111111111111111111");

/// USDC-quote bonding curves (bot uses `buy_exact_in` with SOL only).
pub const USDC_QUOTE_MINT: Address =
    Address::from_str_const("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v");

#[derive(Debug)]
pub struct GeneralCreate {
    pub mint: Address,
    pub user: Address,
    pub metadata: Option<GeneralMetadata>,
    /// Quote asset for the launch (native SOL vs USDC, etc.). Non-SOL quotes are
    /// skipped before scoring — `buy_exact_in` and mcap tape assume SOL reserves.
    pub quote_mint: Address,
}

impl GeneralCreate {
    /// True when the launch is SOL-denominated (or legacy unset → treat as SOL).
    pub fn is_native_sol_quote(&self) -> bool {
        self.quote_mint == NATIVE_SOL_QUOTE_MINT || self.quote_mint == LEGACY_SOL_QUOTE_MINT
    }

    /// Non-SOL quotes we must not score/buy (e.g. USDC bonding curves).
    pub fn is_unsupported_quote_mint(&self) -> bool {
        !self.is_native_sol_quote()
    }
}

#[derive(Debug)]
pub struct GeneralBuy {
    pub mint: Address,
    pub user: Address,
    pub bought: Amount,
    pub spent: Currency,
}

#[derive(Debug)]
pub struct GeneralSell {
    pub mint: Address,
    pub user: Address,
    pub sold: Amount,
    pub received: Currency,
}

impl From<TradeAction> for Offer {
    fn from(action: TradeAction) -> Self {
        match action {
            TradeAction::Buy(buy) => Offer::Buy {
                buy_for: buy.spent.into(),
                received: buy.bought,
            },
            TradeAction::Sell(sell) => Offer::Sell {
                sell_amount: sell.sold,
                received: sell.received.into(),
            },
        }
    }
}
