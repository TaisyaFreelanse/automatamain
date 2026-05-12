use crate::address::Address;

pub trait IntoAccountMetaArray {
    fn accounts_meta(self) -> alloc::vec::Vec<AccountMeta>;
}

#[derive(Debug)]
pub struct AccountMeta {
    pub pubkey: Address,
    pub is_signer: bool,
    pub writable: bool,
}
