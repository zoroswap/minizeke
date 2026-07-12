use std::ops::Add;

use anyhow::Result;
use miden_client::{Felt, Word, asset::FungibleAsset};
use miden_protocol::asset::AssetCallbackFlag;

pub fn asset_to_word(asset: FungibleAsset) -> Word {
    let faucet_id = asset.faucet_id();
    let value = asset.to_value_word();
    let callbacks = asset.callbacks();
    [
        Felt::new_unchecked(faucet_id.suffix().as_canonical_u64()),
        faucet_id.prefix().as_felt(),
        callbacks.as_u8().into(),
        value[0],
    ]
    .into()
}

pub fn word_to_asset(word: Word) -> Result<FungibleAsset> {
    // AssetVaultKey metadata packs fungible composition in bits 0-1 and the callback flag
    // in bit 2 of the faucet suffix.
    let metadata = Felt::new(1 + (word[2].as_canonical_u64() << 2))?;
    let asset = FungibleAsset::from_key_value_words(
        [Felt::ZERO, Felt::ZERO, word[0].add(metadata), word[1]].into(),
        [word[3], Felt::ZERO, Felt::ZERO, Felt::ZERO].into(),
    )?;
    if word[2].as_canonical_u64().ne(&0) {
        Ok(asset.with_callbacks(AssetCallbackFlag::Enabled))
    } else {
        Ok(asset)
    }
}

#[cfg(test)]
mod tests {
    use miden_client::account::AccountId;
    use miden_client::testing::account_id::ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1;
    use miden_protocol::asset::AssetCallbackFlag;

    use super::*;

    fn faucet_id() -> Result<AccountId> {
        Ok(AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?)
    }

    #[test]
    fn parse_asset_to_word() -> Result<()> {
        let faucet_id = faucet_id()?;
        let asset = FungibleAsset::new(faucet_id, 10000)?;
        let word = asset_to_word(asset);
        assert_eq!(
            word[0].as_canonical_u64(),
            asset.faucet_id().suffix().as_canonical_u64()
        );
        assert_eq!(
            word[1].as_canonical_u64(),
            asset.faucet_id().prefix().as_u64()
        );
        assert_eq!(word[2].as_canonical_u64() as u8, asset.callbacks().as_u8());
        assert_eq!(word[3].as_canonical_u64(), asset.amount().as_u64());
        Ok(())
    }

    #[test]
    fn parse_word_to_asset() -> Result<()> {
        let expected = FungibleAsset::new(faucet_id()?, 10000)?;
        let asset = word_to_asset(asset_to_word(expected))?;
        assert_eq!(asset.callbacks().as_u8(), 0);
        assert_eq!(asset, expected);
        Ok(())
    }

    #[test]
    fn parse_asset_with_callback_to_word() -> Result<()> {
        let faucet_id = faucet_id()?;
        let asset =
            FungibleAsset::new(faucet_id, 10000)?.with_callbacks(AssetCallbackFlag::Enabled);
        let word = asset_to_word(asset);
        assert_eq!(
            word[0].as_canonical_u64(),
            asset.faucet_id().suffix().as_canonical_u64()
        );
        assert_eq!(
            word[1].as_canonical_u64(),
            asset.faucet_id().prefix().as_u64()
        );
        assert_eq!(1, asset.callbacks().as_u8());
        assert_eq!(word[3].as_canonical_u64(), asset.amount().as_u64());
        Ok(())
    }

    #[test]
    fn parse_word_to_asset_with_callback() -> Result<()> {
        let expected =
            FungibleAsset::new(faucet_id()?, 10000)?.with_callbacks(AssetCallbackFlag::Enabled);
        let asset = word_to_asset(asset_to_word(expected))?;
        assert_eq!(asset.callbacks().as_u8(), 1);
        assert_eq!(asset, expected);
        Ok(())
    }
}
