use crate::address::Address;
use crate::helper::ata;
use crate::instructions::account::{AccountMeta, IntoAccountMetaArray};
use crate::instructions::raw_instruction::{
    Instruction, InstructionArgs, ProgramAddress, RawInstruction,
};
use borsh::{BorshDeserialize, BorshSerialize};
use ix_macros::{Accounts, Instruction, Instructions};

#[derive(Instructions, Debug)]
#[program("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL")]
pub enum AssociatedTokenProgram {
    CreateIdempotent(Instruction<CreateIdempotentInstruction, CreateIdempotentAccounts>),
}

impl AssociatedTokenProgram {
    pub fn create_idempotent(
        mint: Address,
        source: Address,
        address: Address,
        token_program: Address,
    ) -> Instruction<CreateIdempotentInstruction, CreateIdempotentAccounts> {
        Instruction {
            data: CreateIdempotentInstruction,
            accounts: CreateIdempotentAccounts {
                source,
                account: ata(&address, &token_program, &mint).0,
                token_program,
                system_program: Address::from_str_const("11111111111111111111111111111111"),
                mint,
                address,
            },
        }
    }
}

#[derive(Instruction, BorshSerialize, BorshDeserialize, Debug)]
#[ix_data(discriminator = [1])]
pub struct CreateIdempotentInstruction;

#[derive(Accounts, Debug)]
pub struct CreateIdempotentAccounts {
    #[signer]
    #[writable]
    source: Address,

    #[writable]
    account: Address,

    #[writable]
    address: Address,

    mint: Address,

    system_program: Address,
    token_program: Address,
}
