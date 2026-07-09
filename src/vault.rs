use anyhow::{Result, anyhow};
use miden_client::{
    Client,
    account::{
        Account, AccountBuilder, AccountComponent, AccountId, AccountStorage, AccountType,
        StorageSlot, component::BasicWallet,
    },
    assembly::CodeBuilder,
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig},
    keystore::{FilesystemKeyStore, Keystore},
};
use miden_core::Word;
use miden_protocol::account::AccountComponentMetadata;
use rand::RngCore;

use crate::{
    assembly_utils::{storage_slot_name, vault_component_code},
    miden_env::MidenNetwork,
    test_utils::touch_account,
};

pub async fn deploy_vault(client: &mut Client<FilesystemKeyStore>) -> Result<Account> {
    let mut init_seed = [0_u8; 32];
    client.rng().fill_bytes(&mut init_seed);

    let key_pair = AuthSecretKey::new_ecdsa_k256_keccak();
    let vault_component = build_vault_component(client.code_builder())?;

    let vault_contract = AccountBuilder::new(init_seed)
        .account_type(AccountType::Public)
        .with_component(vault_component.clone())
        .with_auth_component(AuthSingleSig::new(
            key_pair.public_key().to_commitment(),
            AuthScheme::EcdsaK256Keccak,
        ))
        .with_component(BasicWallet)
        .build()?;

    let keystore = FilesystemKeyStore::new("keystore".into())?;
    keystore
        .add_key(&key_pair, vault_contract.id())
        .await
        .map_err(|e| anyhow!("Failed to add key: {e:?}"))?;

    println!(
        "pool contract commitment hash: {:?}",
        vault_contract.to_commitment().to_hex()
    );
    println!(
        "contract id: {:?}, hex: {:?}",
        vault_contract
            .id()
            .to_bech32(MidenNetwork::from_env().endpoint().to_network_id()),
        vault_contract.id().to_hex()
    );

    // Add the account to the client
    client.add_account(&vault_contract, true).await?;

    client.sync_state().await?;

    touch_account(client, &vault_contract.id()).await?;

    let vault_contract = client.try_get_account(vault_contract.id()).await?;

    Ok(vault_contract)
}

pub fn build_vault_component(_cb: CodeBuilder) -> Result<AccountComponent> {
    let lib = vault_component_code().clone();
    let slot_user_assets_total_funding =
        StorageSlot::with_empty_map(storage_slot_name("zorovault::user_asset_total_funding"));
    let slot_user_total_redeems =
        StorageSlot::with_empty_map(storage_slot_name("zorovault::user_asset_total_redeems"));
    let slot_user_asset_total_initiated_redeems = StorageSlot::with_empty_map(storage_slot_name(
        "zorovault::user_asset_total_initiated_redeems",
    ));

    let component = AccountComponent::new(
        lib,
        vec![
            slot_user_assets_total_funding,
            slot_user_total_redeems,
            slot_user_asset_total_initiated_redeems,
        ],
        AccountComponentMetadata::new("zoro_miden::vault"),
    )?;

    Ok(component)
}

/// Returns the vault's current on-chain [`AccountStorage`], as known by the client's local
/// (synced) store.
pub async fn get_vault_storage(
    client: &Client<FilesystemKeyStore>,
    vault_id: AccountId,
) -> Result<AccountStorage> {
    let account = client
        .try_get_account(vault_id)
        .await
        .map_err(|e| anyhow!("failed to fetch vault account {}: {e:?}", vault_id.to_hex()))?;
    Ok(account.storage().clone())
}

/// Builds the vault's per-user-per-asset storage map key: `[asset_suffix, asset_prefix,
/// user_suffix, user_prefix]` (see the KEY CONVENTION docs in `masm/accounts/vault.masm`).
pub fn vault_user_asset_key(asset_id: AccountId, user_id: AccountId) -> Word {
    Word::from([
        asset_id.suffix(),
        asset_id.prefix().as_felt(),
        user_id.suffix(),
        user_id.prefix().as_felt(),
    ])
}

/// A user's cumulative funding/redeem accounting for a single asset within the vault.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VaultUserAssetInfo {
    pub total_funding: u64,
    pub total_initiated_redeems: u64,
    pub total_redeems: u64,
}

impl VaultUserAssetInfo {
    /// Redeems that have been initiated but not yet completed.
    pub fn pending_redeem(&self) -> u64 {
        self.total_initiated_redeems.saturating_sub(self.total_redeems)
    }
}

/// Extracts a user's vault accounting for `asset_id` from an already-fetched [`AccountStorage`].
pub fn vault_user_asset_info_from_storage(
    storage: &AccountStorage,
    asset_id: AccountId,
    user_id: AccountId,
) -> Result<VaultUserAssetInfo> {
    let key = vault_user_asset_key(asset_id, user_id);
    let read = |slot: &str| -> Result<u64> {
        Ok(storage
            .get_map_item(&storage_slot_name(slot), key)
            .map_err(|e| anyhow!("failed to read {slot}: {e:?}"))?[0]
            .as_canonical_u64())
    };

    Ok(VaultUserAssetInfo {
        total_funding: read("zorovault::user_asset_total_funding")?,
        total_initiated_redeems: read("zorovault::user_asset_total_initiated_redeems")?,
        total_redeems: read("zorovault::user_asset_total_redeems")?,
    })
}

/// Fetches the vault's storage and extracts `user_id`'s funding, initiated-redeem and redeem
/// totals for `asset_id`.
pub async fn get_vault_user_asset_info(
    client: &Client<FilesystemKeyStore>,
    vault_id: AccountId,
    asset_id: AccountId,
    user_id: AccountId,
) -> Result<VaultUserAssetInfo> {
    let storage = get_vault_storage(client, vault_id).await?;
    vault_user_asset_info_from_storage(&storage, asset_id, user_id)
}
