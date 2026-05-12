use borsh::BorshDeserialize;
use serde::{Deserialize, Serialize};
const ADDRESS_LENGTH: usize = 32;

#[derive(BorshDeserialize, Clone, Copy, Debug, Hash, PartialEq)]
pub struct Address(pub [u8; ADDRESS_LENGTH]);

impl Serialize for Address {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = bs58::encode(self.0).into_string();
        serializer.serialize_str(&s)
    }
}

impl<'de> Deserialize<'de> for Address {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let base58_string = <String as serde::Deserialize>::deserialize(deserializer)?;

        let blob = bs58::decode(base58_string)
            .into_vec()
            .map_err(|_| serde::de::Error::custom("Base58 deserialization failed"))?;

        if blob.len() != ADDRESS_LENGTH {
            return Err(serde::de::Error::custom(
                "Deserialized address is invalid, bytes not equal 32",
            ));
        }

        let mut arr = [0u8; 32];
        arr.copy_from_slice(&blob);
        Ok(Address(arr))
    }
}
