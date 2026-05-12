use borsh::{BorshDeserialize, BorshSerialize};
use sha2::{Digest, Sha256};

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct Address(pub [u8; 32]);
impl Address {
    pub const fn from_str_const(data: &'static str) -> Self {
        Address(five8_const::decode_32_const(data))
    }

    pub fn to_bytes(self) -> [u8; 32] {
        self.0
    }

    #[allow(clippy::should_implement_trait)]
    pub fn as_ref(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn pda(program: &Address, seeds: &[&[u8]]) -> (Address, u8) {
        let mut bump_seed = [u8::MAX];
        for _ in 0..u8::MAX {
            {
                let mut seeds_with_bump = seeds.to_vec();
                seeds_with_bump.push(&bump_seed);
                match create_program_address(&seeds_with_bump, program) {
                    Ok(address) => return (address, bump_seed[0]),
                    Err(AddressError::InvalidSeeds) => (),
                    _ => break,
                }
            }
            bump_seed[0] -= 1;
        }

        panic!("No valid PDA was found");
    }
}

pub fn bytes_are_curve_point<T: AsRef<[u8]>>(_bytes: T) -> bool {
    let Ok(compressed_edwards_y) =
        curve25519_dalek::edwards::CompressedEdwardsY::from_slice(_bytes.as_ref())
    else {
        return false;
    };
    compressed_edwards_y.decompress().is_some()
}

pub fn create_program_address(
    seeds: &[&[u8]],
    program_id: &Address,
) -> Result<Address, AddressError> {
    if seeds.len() > 16 {
        return Err(AddressError::MaxSeedLengthExceeded);
    }
    if seeds.iter().any(|seed| seed.len() > 32) {
        return Err(AddressError::MaxSeedLengthExceeded);
    }

    let mut hasher = Sha256::new();

    for seed in seeds {
        sha2::digest::DynDigest::update(&mut hasher, seed);
    }

    sha2::digest::DynDigest::update(&mut hasher, program_id.as_ref());
    sha2::digest::DynDigest::update(&mut hasher, b"ProgramDerivedAddress");

    let hash: [u8; 32] = hasher.finalize().into();

    if bytes_are_curve_point(hash.as_ref()) {
        return Err(AddressError::InvalidSeeds);
    }

    Ok(Address(hash))
}

pub enum AddressError {
    MaxSeedLengthExceeded,
    InvalidSeeds,
}
