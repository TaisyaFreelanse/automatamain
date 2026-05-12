use crate::{
    address::Address,
    instructions::{
        account::{AccountMeta, IntoAccountMetaArray},
        raw_instruction::{Instruction, InstructionArgs, ProgramAddress},
    },
};

impl<Args, Accounts> From<Instruction<Args, Accounts>> for solana_instruction::Instruction
where
    Args: InstructionArgs,
    Accounts: IntoAccountMetaArray,
    Instruction<Args, Accounts>: ProgramAddress,
{
    fn from(value: Instruction<Args, Accounts>) -> Self {
        Self {
            program_id: value.program().clone().into(),
            accounts: convert_accounts(value.accounts.accounts_meta()),
            data: value.data.to_le_bytes(),
        }
    }
}

impl From<AccountMeta> for solana_instruction::AccountMeta {
    fn from(value: AccountMeta) -> Self {
        Self {
            pubkey: value.pubkey.into(),
            is_signer: value.is_signer,
            is_writable: value.writable,
        }
    }
}

impl From<Address> for solana_address::Address {
    fn from(value: Address) -> Self {
        solana_address::Address::new_from_array(value.to_bytes())
    }
}

impl From<solana_address::Address> for Address {
    fn from(value: solana_address::Address) -> Self {
        Self(value.to_bytes())
    }
}

fn convert_accounts(
    accounts: alloc::vec::Vec<AccountMeta>,
) -> alloc::vec::Vec<solana_instruction::AccountMeta> {
    accounts.into_iter().map(Into::into).collect()
}
