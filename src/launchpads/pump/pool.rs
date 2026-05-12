use std::any::Any;

use crate::feed::logs::pump::PumpEvent;
use crate::generalize::general_commands::{Action, Currency};
use crate::generalize::general_pool::DynClonePool;
use crate::launchpads::pump::general::{PRECISION, bounding_curve, pool_pda};
use crate::{generalize::general_pool::Pool, helper::Amount};
use solana_address::Address;

#[derive(Debug, Clone)]
pub struct Bonding {
    virtual_sol_reserves: Amount,
    virtual_token_reserves: Amount,

    real_sol_reserves: Amount,
    real_token_reserves: Amount,

    mint: Address,
    pool: Address,

    creator: [Address; 1],
}

#[derive(Debug, Clone)]
pub struct Migrated {
    sol_reserves: Amount,
    token_reserves: Amount,

    mint: Address,
    pool: Address,

    creator: [Address; 1],
}

impl DynClonePool for Migrated {
    fn clone_box(&self) -> Box<dyn Pool> {
        Box::new(self.clone())
    }
}

impl Pool for Migrated {
    fn price(&self) -> Currency {
        Currency::Native(Amount::from_raw_native(
            self.sol_reserves.raw() / (self.token_reserves.raw()) / 1000,
        ))
    }

    fn mint(&self) -> Address {
        self.mint
    }

    fn pool(&self) -> Address {
        self.pool
    }

    fn update(&mut self, event: &dyn Any) {}

    fn token_decimals(&self) -> u8 {
        6
    }

    fn quote_decimals(&self) -> u8 {
        9
    }

    fn creators(&self) -> &[Address] {
        &self.creator
    }

    fn market_cap(&self) -> Currency {
        let market_cap = (self.sol_reserves.raw() as u128) * 1_000_000_000u128
            / (self.token_reserves.raw() as u128);

        Currency::Native(Amount::from_raw_native(market_cap as u64))
    }

    fn instruction(&self, action: Action) -> solana_instruction::Instruction {
        todo!()
    }
}

impl DynClonePool for Bonding {
    fn clone_box(&self) -> Box<dyn Pool> {
        Box::new(self.clone())
    }
}

impl Pool for Bonding {
    fn price(&self) -> crate::generalize::general_commands::Currency {
        let price = (self.virtual_sol_reserves.raw() as u128) * 1_000_000_000u128
            / (self.virtual_token_reserves.raw() as u128);

        Currency::Native(Amount::from_raw_native(price as u64))
    }

    fn mint(&self) -> solana_address::Address {
        self.mint
    }

    fn pool(&self) -> solana_address::Address {
        self.pool
    }

    fn update(&mut self, event: &dyn Any) {
        if let Some(event) = event.downcast_ref::<PumpEvent>() {
            match event {
                PumpEvent::Create(_) => (),
                PumpEvent::TradeEvent(trade_event) => {
                    self.real_sol_reserves = Amount::from_raw_native(trade_event.real_sol_reserves);
                    self.real_token_reserves =
                        Amount::from_raw(trade_event.real_token_reserves, PRECISION);
                    self.virtual_sol_reserves =
                        Amount::from_raw_native(trade_event.virtual_sol_reserves);
                    self.virtual_token_reserves =
                        Amount::from_raw(trade_event.virtual_token_reserves, PRECISION);
                }
            }
        }
    }

    fn token_decimals(&self) -> u8 {
        6
    }

    fn quote_decimals(&self) -> u8 {
        9
    }

    fn creators(&self) -> &[Address] {
        &self.creator
    }

    fn market_cap(&self) -> Currency {
        let market_cap = (self.virtual_sol_reserves.raw() as u128) * 1_000_000_000_000_000u128
            / (self.virtual_token_reserves.raw() as u128);

        Currency::Native(Amount::from_raw_native(market_cap as u64))
    }

    fn instruction(&self, action: Action) -> solana_instruction::Instruction {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct PumpPool<T: Pool> {
    state: T,
}

impl PumpPool<Bonding> {
    pub fn new(mint: Address, creator: Address) -> Self {
        Self {
            state: Bonding {
                virtual_sol_reserves: Amount::from_raw_native(30000000000),
                virtual_token_reserves: Amount::from_raw(1073000000000000, PRECISION),
                real_sol_reserves: Amount::from_raw_native(0),
                real_token_reserves: Amount::from_raw(793100000000000, PRECISION),
                mint,
                pool: bounding_curve(&mint).0,
                creator: [creator],
            },
        }
    }

    pub fn migrate(self) -> PumpPool<Migrated> {
        PumpPool {
            state: Migrated {
                sol_reserves: self.state.real_sol_reserves,
                token_reserves: self.state.real_token_reserves,
                mint: self.state.mint,
                pool: pool_pda(&self.mint()).0,
                creator: self.state.creator,
            },
        }
    }
}

impl DynClonePool for PumpPool<Bonding> {
    fn clone_box(&self) -> Box<dyn Pool> {
        Box::new(self.clone())
    }
}

impl Pool for PumpPool<Bonding> {
    fn price(&self) -> crate::generalize::general_commands::Currency {
        self.state.price()
    }

    fn mint(&self) -> solana_address::Address {
        self.state.mint()
    }

    fn pool(&self) -> solana_address::Address {
        self.state.pool()
    }

    fn update(&mut self, event: &dyn Any) {
        self.state.update(event);
    }

    fn token_decimals(&self) -> u8 {
        6
    }

    fn quote_decimals(&self) -> u8 {
        9
    }

    fn creators(&self) -> &[Address] {
        &self.state.creator
    }

    fn market_cap(&self) -> Currency {
        self.state.market_cap()
    }
    fn instruction(&self, action: Action) -> solana_instruction::Instruction {
        todo!()
    }
}

impl DynClonePool for PumpPool<Migrated> {
    fn clone_box(&self) -> Box<dyn Pool> {
        Box::new(self.clone())
    }
}

impl Pool for PumpPool<Migrated> {
    fn price(&self) -> crate::generalize::general_commands::Currency {
        self.state.price()
    }

    fn mint(&self) -> solana_address::Address {
        self.state.mint()
    }

    fn pool(&self) -> solana_address::Address {
        self.state.pool()
    }

    fn update(&mut self, event: &dyn Any) {
        self.state.update(event);
    }

    fn token_decimals(&self) -> u8 {
        6
    }

    fn quote_decimals(&self) -> u8 {
        9
    }

    fn creators(&self) -> &[Address] {
        self.state.creators()
    }

    fn market_cap(&self) -> Currency {
        self.state.market_cap()
    }

    fn instruction(&self, action: Action) -> solana_instruction::Instruction {
        todo!()
    }
}
