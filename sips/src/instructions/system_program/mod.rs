use crate::address::Address;

use crate::{
    helper::{Amount, NATIVE_SOL_PRECISION},
    instructions::raw_instruction::InstructionArgs,
};
use borsh::{BorshDeserialize, BorshSerialize};
use ix_macros::{Accounts, Instruction};

use crate::instructions::account::{AccountMeta, IntoAccountMetaArray};

#[derive(Instruction, BorshSerialize, BorshDeserialize, Debug)]
#[ix_data(
    discriminator = [2, 0, 0, 0]
)]
pub struct Transfer {
    pub sol: Amount<NATIVE_SOL_PRECISION>,
}

#[derive(Accounts)]
pub struct TransferAccounts {
    #[signer]
    #[writable]
    pub sender: Address,
    #[writable]
    pub receiver: Address,
}
