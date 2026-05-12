use crate::address::Address;
use crate::instructions::account::{AccountMeta, IntoAccountMetaArray};
use crate::instructions::raw_instruction::{
    Instruction, InstructionArgs, ProgramAddress, RawInstruction,
};
use borsh::{BorshDeserialize, BorshSerialize};
use ix_macros::{Accounts, Instruction, Instructions};

#[derive(Instructions, Debug)]
#[program("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")]
pub enum TokenProgram2022 {
    TransferChecked(Instruction<TransferCheckedInstruction, TransferAccounts>),
}

#[derive(Instruction, BorshSerialize, BorshDeserialize, Debug)]
#[ix_data(discriminator = [12])]
pub struct TransferCheckedInstruction {
    amount: u64,
    decimals: u8,
}

#[derive(Accounts, Debug)]
pub struct TransferAccounts {
    #[signer]
    #[writable]
    source: Address,
    mint: Address,
    #[writable]
    destination: Address,
    authority: Address,
}
