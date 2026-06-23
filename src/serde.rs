use miden_client::account::AccountId;
use serde::{Deserialize, Deserializer, Serializer};

pub fn serialize_account_id<S: Serializer>(id: &AccountId, s: S) -> Result<S::Ok, S::Error> {
    s.serialize_str(&id.to_hex())
}

pub fn deserialize_account_id<'de, D: Deserializer<'de>>(d: D) -> Result<AccountId, D::Error> {
    let hex_str: &str = Deserialize::deserialize(d)?;
    AccountId::from_hex(hex_str)
        .map_err(|e| serde::de::Error::custom(format!("invalid AccountId: {}", e)))
}
