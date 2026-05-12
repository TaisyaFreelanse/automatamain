use solana_address::Address;

use crate::{general::Slot, persistence::error::Error};

pub struct TokenData {
    pub mint: Address,
    pub created_at: Slot,
}

#[async_trait::async_trait]
pub trait TokenRepository {
    async fn save_token(&self, coin: Address, developer: Address, slot: Slot) -> Result<(), Error>;
    async fn get_token(&self, coin: Address) -> Option<TokenData>;
}
