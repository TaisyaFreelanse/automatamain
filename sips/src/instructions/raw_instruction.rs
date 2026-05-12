use crate::{
    address::Address,
    instructions::{
        account::{AccountMeta, IntoAccountMetaArray},
        error::Error,
    },
};

use alloc::vec::Vec;
use borsh::{BorshDeserialize, BorshSerialize, from_slice};

#[derive(Debug)]
pub struct Instruction<Args: InstructionArgs, Accounts: IntoAccountMetaArray> {
    pub data: Args,
    pub accounts: Accounts,
}

impl<Args, Accounts> Instruction<Args, Accounts>
where
    Args: InstructionArgs,
    Accounts: IntoAccountMetaArray,
{
    pub fn into_raw(self, program: Address) -> RawInstruction {
        let data = self.data.to_le_bytes();
        let accounts = self.accounts.accounts_meta();

        RawInstruction {
            program,
            data,
            accounts,
        }
    }
}

//probably not very good?
impl IntoAccountMetaArray for () {
    fn accounts_meta(self) -> alloc::vec::Vec<AccountMeta> {
        alloc::vec![]
    }
}

#[derive(Debug)]
pub struct RawInstruction {
    pub program: Address,
    pub data: Vec<u8>,
    pub accounts: Vec<AccountMeta>,
}

pub trait InstructionArgs: Sized + BorshSerialize + BorshDeserialize {
    const DISCRIMINATOR: &'static [u8];

    fn from_bytes(data: &[u8]) -> Result<Self, Error> {
        let discriminator_size = Self::DISCRIMINATOR.len();

        let discriminator = data
            .get(..discriminator_size)
            .ok_or(Error::InstructionDataIsTooSmall)?;

        if Self::DISCRIMINATOR != discriminator {
            return Err(Error::InvalidDiscriminator);
        }

        let data: &[u8] = data
            .get(discriminator_size..)
            .ok_or(Error::InvalidInstructionSize)?;

        let instruction = from_slice::<Self>(&data).map_err(|_| Error::InvalidInstructionData)?;

        Ok(instruction)
    }

    fn to_le_bytes(&self) -> alloc::vec::Vec<u8> {
        let mut data = alloc::vec::Vec::new();
        data.extend_from_slice(Self::DISCRIMINATOR);

        //how the fuck can it panic
        self.serialize(&mut data)
            .expect("Borsh serialization failed");

        data
    }
}

pub trait ProgramAddress {
    fn program(&self) -> &'static Address;
}
