use borsh::{BorshDeserialize, BorshSerialize};

use crate::address::Address;

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct Link(pub alloc::string::String);

#[derive(BorshDeserialize)]
pub struct Time(pub u64);

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct Amount<const P: u8>(pub u64);

impl<const P: u8> Amount<P> {
    pub const SCALE: u64 = 10u64.pow(P as u32);

    pub fn from_float(v: f64) -> Self {
        Self((v * Self::SCALE as f64).round() as u64)
    }

    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    pub fn raw(&self) -> u64 {
        self.0
    }

    pub fn to_float(&self) -> f64 {
        self.0 as f64 / Self::SCALE as f64
    }
}

pub const NATIVE_SOL_PRECISION: u8 = 9;
pub const LAMPORT_PRECISION: u8 = 6;

// move later in ata folder
const ATA_PROGRAM: Address =
    Address::from_str_const("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");

pub fn ata(address: &Address, token_program: &Address, mint: &Address) -> (Address, u8) {
    Address::pda(
        &ATA_PROGRAM,
        &[address.as_ref(), token_program.as_ref(), mint.as_ref()],
    )
}
