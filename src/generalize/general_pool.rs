use std::any::Any;

use crate::generalize::general_commands::{Action, Currency};
use solana_address::Address;

pub trait Pool: Send + Sync + DynClonePool {
    fn update(&mut self, event: &dyn Any);
    fn token_decimals(&self) -> u8;
    fn quote_decimals(&self) -> u8;
    fn instruction(&self, action: Action) -> solana_instruction::Instruction;
    fn price(&self) -> Currency;
    fn market_cap(&self) -> Currency;
    fn mint(&self) -> Address;
    fn pool(&self) -> Address;
    fn creators(&self) -> &[Address];
}

pub trait DynClonePool {
    fn clone_box(&self) -> Box<dyn Pool>;
}
