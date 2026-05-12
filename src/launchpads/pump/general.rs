use std::sync::Arc;

use solana_address::Address;
use tokio::sync::mpsc::Sender;

use crate::{
    feed::{feed::Feed, logs::pump::PumpEvent},
    generalize::{general_commands::Action, generalizer::generalize},
    launchpads::pump::launchpad::{PumpLaunchpadCommand, PumpLaunchpadStorageActor},
};

pub const PRECISION: u8 = 6;
pub const PUMP_FUN_ADDRESS: Address =
    Address::from_str_const("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");

pub fn pool_pda(base_mint: &Address) -> (Address, u8) {
    Address::derive_program_address(
        &[
            b"pool",
            &0u16.to_le_bytes(),
            pool_authority(base_mint).0.as_ref(),
            base_mint.as_ref(),
            Address::from_str_const("So11111111111111111111111111111111111111112").as_ref(),
        ],
        &Address::from_str_const("pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"),
    )
    .expect("very fucking unlikely pda fail")
}

pub fn pool_authority(mint: &Address) -> (Address, u8) {
    Address::derive_program_address(
        &[
            &[
                112, 111, 111, 108, 45, 97, 117, 116, 104, 111, 114, 105, 116, 121,
            ],
            mint.as_ref(),
        ],
        &Address::from_str_const("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"),
    )
    .expect("very fucking unlikely pda fail")
}

pub fn bounding_curve(mint: &Address) -> (Address, u8) {
    let seeds = &[b"bonding-curve", mint.as_ref()];
    Address::derive_program_address(seeds, &PUMP_FUN_ADDRESS)
        .expect("if it crashes i will fucking kill myself")
}
