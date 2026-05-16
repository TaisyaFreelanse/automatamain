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
    /// Drains rent-exempt SOL from a token account back to `destination` and
    /// permanently closes the account. Only valid when the token account's
    /// balance is exactly 0 — issue a full-balance Transfer/Sell first, then
    /// CloseAccount in the same transaction for atomic rent recovery.
    CloseAccount(Instruction<CloseAccountInstruction, CloseAccountAccounts>),
}

impl TokenProgram2022 {
    /// Build a `CloseAccount` instruction for `(account, destination,
    /// authority)`. For the typical bot case: `account` = our ATA,
    /// `destination` = our wallet (rent SOL refunded to us),
    /// `authority` = our wallet (signer).
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
