use crate::instructions::raw_instruction::RawInstruction;
use crate::{
    address::Address,
    helper::{Amount, NATIVE_SOL_PRECISION},
    instructions::raw_instruction::{Instruction, InstructionArgs, ProgramAddress},
};
use borsh::{BorshDeserialize, BorshSerialize};
use ix_macros::{Instruction, Instructions};

#[derive(Instruction, BorshSerialize, BorshDeserialize, Debug)]
#[ix_data(discriminator = [2])]
pub struct ComputeUnitLimit {
    pub limit: u32,
}

#[derive(Instruction, BorshSerialize, BorshDeserialize, Debug)]
#[ix_data(discriminator = [3])]
pub struct ComputeUnitPrice {
    pub price: u128,
}

impl ComputeUnitPrice {
    pub fn from_sol(amount: Amount<NATIVE_SOL_PRECISION>) -> Self {
        Self {
            price: amount.raw() as u128 * 1_000_000,
        }
    }
}

#[derive(Instructions, Debug)]
#[program("ComputeBudget111111111111111111111111111111")]
pub enum ComputeBudgetInstruction {
    SetUnitPrice(Instruction<ComputeUnitPrice, ()>),
    SetComputeLimit(Instruction<ComputeUnitLimit, ()>),
}

impl ComputeBudgetInstruction {
    pub fn priority_fee(
        compute_limit: u32,
        fee: Amount<NATIVE_SOL_PRECISION>,
    ) -> (
        Instruction<ComputeUnitPrice, ()>,
        Instruction<ComputeUnitLimit, ()>,
    ) {
        let unit_price = fee.raw() as u128 * 1_000_000u128 / compute_limit as u128;
        (
            Instruction {
                data: ComputeUnitPrice { price: unit_price },
                accounts: (),
            },
            Instruction {
                data: ComputeUnitLimit {
                    limit: compute_limit,
                },

                accounts: (),
            },
        )
    }
}
