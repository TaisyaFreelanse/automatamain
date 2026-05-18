//! Legacy SPL Token program (`Tokenkeg…`). Pump.fun bonding-curve mints use
//! this program; Token-2022 (`Tokenz…`) is a different program id — mixing
//! them surfaces as `IncorrectProgramId` on ATA / transfer / close ixs.

use crate::address::Address;
use crate::instructions::account::{AccountMeta, IntoAccountMetaArray};
use crate::instructions::raw_instruction::{
    Instruction, InstructionArgs, ProgramAddress, RawInstruction,
};
use borsh::{BorshDeserialize, BorshSerialize};
use ix_macros::{Accounts, Instruction, Instructions};

#[derive(Instructions, Debug)]
#[program("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")]
pub enum SplTokenProgram {
    TransferChecked(Instruction<TransferCheckedInstruction, TransferAccounts>),
    /// Close an empty token account; rent returns to `destination`.
    CloseAccount(Instruction<CloseAccountInstruction, CloseAccountAccounts>),
}

impl SplTokenProgram {
    pub fn close_account(
        account: Address,
        destination: Address,
        authority: Address,
    ) -> Instruction<CloseAccountInstruction, CloseAccountAccounts> {
        Instruction {
            data: CloseAccountInstruction,
            accounts: CloseAccountAccounts {
                account,
                destination,
                authority,
            },
        }
    }
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

#[derive(Instruction, BorshSerialize, BorshDeserialize, Debug)]
#[ix_data(discriminator = [9])]
pub struct CloseAccountInstruction;

#[derive(Accounts, Debug)]
pub struct CloseAccountAccounts {
    #[writable]
    account: Address,

    #[writable]
    destination: Address,

    #[signer]
    authority: Address,
}
