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
    transaction::TransactionRequestBuilder,
};
use miden_core::{Felt, Word};
use miden_protocol::account::AccountComponentMetadata;
use rand::RngCore;

use crate::{
    assembly_utils::{
        compile_vault_code, link_all_note_libraries, pool_balance_details_proc_root,
        print_library_exports, storage_slot_name,
    },
    miden_env::MidenNetwork,
    test_utils::{submit_tx_resilient, touch_account},
};

pub const USER_ASSET_TOTAL_FUNDING_SLOT: &str = "zorovault::user_asset_total_funding";
pub const USER_ASSET_TOTAL_REDEEMS_SLOT: &str = "zorovault::user_asset_total_redeems";
pub const USER_ASSET_TOTAL_INITIATED_REDEEMS_SLOT: &str =
    "zorovault::user_asset_total_initiated_redeems";
pub const USER_PUBKEYS_SLOT: &str = "zorovault::user_pubkeys";
pub const USER_INDICES_SLOT: &str = "zorovault::user_indices";
pub const NEXT_USER_INDEX_SLOT: &str = "zorovault::next_user_index";
pub const POOL_ACCOUNT_ID_SLOT: &str = "zorovault::pool_account_id";
pub const USER_POOL_BALANCE_DETAILS_PROC_ROOT_SLOT: &str =
    "zorovault::user_pool_balance_details_proc_root";
pub const LP_ENTITLEMENTS_SLOT: &str = "zorovault::lp_entitlements";
pub const LP_WITHDRAWN_SLOT: &str = "zorovault::lp_withdrawn";

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

pub fn build_vault_component(cb: CodeBuilder) -> Result<AccountComponent> {
    // storage-less pool build just to extract the FPI proc root (breaks the circular
    // vault <-> pool dependency; MAST roots do not depend on storage)
    let pool_proc_root = pool_balance_details_proc_root(cb.clone())?;

    let lib = compile_vault_code(cb)?;
    print_library_exports(lib.as_library());

    let zero_word = Word::new([Felt::ZERO; 4]);

    let component = AccountComponent::new(
        lib,
        vec![
            StorageSlot::with_empty_map(storage_slot_name(USER_ASSET_TOTAL_FUNDING_SLOT)),
            StorageSlot::with_empty_map(storage_slot_name(USER_ASSET_TOTAL_REDEEMS_SLOT)),
            StorageSlot::with_empty_map(storage_slot_name(USER_ASSET_TOTAL_INITIATED_REDEEMS_SLOT)),
            StorageSlot::with_empty_map(storage_slot_name(USER_PUBKEYS_SLOT)),
            StorageSlot::with_empty_map(storage_slot_name(USER_INDICES_SLOT)),
            StorageSlot::with_value(storage_slot_name(NEXT_USER_INDEX_SLOT), zero_word),
            // set after the pool is deployed via `set_pool_account_id_on_vault`
            StorageSlot::with_value(storage_slot_name(POOL_ACCOUNT_ID_SLOT), zero_word),
            StorageSlot::with_value(
                storage_slot_name(USER_POOL_BALANCE_DETAILS_PROC_ROOT_SLOT),
                pool_proc_root,
            ),
            StorageSlot::with_empty_map(storage_slot_name(LP_ENTITLEMENTS_SLOT)),
            StorageSlot::with_empty_map(storage_slot_name(LP_WITHDRAWN_SLOT)),
        ],
        AccountComponentMetadata::new("zoro_miden::vault"),
    )?;

    Ok(component)
}

/// Stores the pool's account id on the vault (one tx script call). Must run after the pool
/// is deployed, before any FPI-dependent flow (swap / init_redeem / redeem).
pub async fn set_pool_account_id_on_vault(
    client: &mut Client<FilesystemKeyStore>,
    vault_id: AccountId,
    pool_id: AccountId,
) -> Result<()> {
    let script_code = format!(
        "
        use miden::core::sys
        use zoro_miden::vault

        begin
            push.{prefix}.{suffix}
            # => [pool_id_suffix, pool_id_prefix]
            call.vault::set_pool_account_id
            exec.sys::truncate_stack
        end
        ",
        prefix = pool_id.prefix().as_u64(),
        suffix = pool_id.suffix().as_canonical_u64(),
    );

    let cb = link_all_note_libraries(client.code_builder())?;
    let tx_script = cb.compile_tx_script(&script_code)?;
    let tx_request = TransactionRequestBuilder::new()
        .custom_script(tx_script)
        .build()?;
    submit_tx_resilient(client, vault_id, tx_request).await?;
    client.sync_state().await?;
    Ok(())
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

/// Builds the vault's registration map key for a user: `[user_suffix, user_prefix, 0, 0]`.
pub fn vault_user_key(user_id: AccountId) -> Word {
    Word::from([
        user_id.suffix(),
        user_id.prefix().as_felt(),
        Felt::ZERO,
        Felt::ZERO,
    ])
}

/// Reads a user's registration from an already-fetched vault [`AccountStorage`].
/// Returns `None` when the user is not registered, otherwise `(index, pubkey_commitment)`.
pub fn vault_user_registration(
    storage: &AccountStorage,
    user_id: AccountId,
) -> Result<Option<(u64, Word)>> {
    let key = vault_user_key(user_id);
    let pubkey = storage
        .get_map_item(&storage_slot_name(USER_PUBKEYS_SLOT), key)
        .map_err(|e| anyhow!("failed to read {USER_PUBKEYS_SLOT}: {e:?}"))?;
    if pubkey == Word::new([Felt::ZERO; 4]) {
        return Ok(None);
    }
    let index = storage
        .get_map_item(&storage_slot_name(USER_INDICES_SLOT), key)
        .map_err(|e| anyhow!("failed to read {USER_INDICES_SLOT}: {e:?}"))?[0]
        .as_canonical_u64();
    Ok(Some((index, pubkey)))
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
        self.total_initiated_redeems
            .saturating_sub(self.total_redeems)
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

/// An LP's cumulative entitlement/withdrawn counters for a single asset within the vault.
/// LP shares themselves live on the server only; these on-chain counters guarantee the LP
/// can self-custodially withdraw up to `withdrawable()` even without the operator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct VaultLpInfo {
    /// Cumulative amount ever owed: principal + operator-checkpointed fees.
    pub entitlement: u64,
    /// Cumulative amount paid out.
    pub withdrawn: u64,
}

impl VaultLpInfo {
    pub fn withdrawable(&self) -> u64 {
        self.entitlement.saturating_sub(self.withdrawn)
    }
}

/// Extracts an LP's entitlement/withdrawn counters for `asset_id` from an already-fetched
/// vault [`AccountStorage`]. Uses the same `[asset, lp]` key convention as the user maps.
pub fn vault_lp_info_from_storage(
    storage: &AccountStorage,
    asset_id: AccountId,
    lp_id: AccountId,
) -> Result<VaultLpInfo> {
    let key = vault_user_asset_key(asset_id, lp_id);
    let read = |slot: &str| -> Result<u64> {
        Ok(storage
            .get_map_item(&storage_slot_name(slot), key)
            .map_err(|e| anyhow!("failed to read {slot}: {e:?}"))?[0]
            .as_canonical_u64())
    };

    Ok(VaultLpInfo {
        entitlement: read(LP_ENTITLEMENTS_SLOT)?,
        withdrawn: read(LP_WITHDRAWN_SLOT)?,
    })
}

/// Fetches the vault's storage and extracts `lp_id`'s LP counters for `asset_id`.
pub async fn get_vault_lp_info(
    client: &Client<FilesystemKeyStore>,
    vault_id: AccountId,
    asset_id: AccountId,
    lp_id: AccountId,
) -> Result<VaultLpInfo> {
    let storage = get_vault_storage(client, vault_id).await?;
    vault_lp_info_from_storage(&storage, asset_id, lp_id)
}

/// Operator-signed maintenance tx: raises `lp_id`'s entitlement for `asset_id` to
/// `new_entitlement` (principal + accrued fees, computed by the server from LP shares).
/// The vault panics if `new_entitlement` is below the current entitlement, so this can
/// only ever raise the counter.
pub async fn checkpoint_lp_entitlement_on_vault(
    client: &mut Client<FilesystemKeyStore>,
    vault_id: AccountId,
    asset_id: AccountId,
    lp_id: AccountId,
    new_entitlement: u64,
) -> Result<()> {
    let script_code = format!(
        "
        use miden::core::sys
        use zoro_miden::vault

        begin
            push.{lp_prefix}.{lp_suffix}.{asset_prefix}.{asset_suffix}.{new_entitlement}
            # => [new_entitlement, asset_suffix, asset_prefix, lp_suffix, lp_prefix]
            call.vault::checkpoint_lp_entitlement
            exec.sys::truncate_stack
        end
        ",
        lp_prefix = lp_id.prefix().as_u64(),
        lp_suffix = lp_id.suffix().as_canonical_u64(),
        asset_prefix = asset_id.prefix().as_u64(),
        asset_suffix = asset_id.suffix().as_canonical_u64(),
    );

    let cb = link_all_note_libraries(client.code_builder())?;
    let tx_script = cb.compile_tx_script(&script_code)?;
    let tx_request = TransactionRequestBuilder::new()
        .custom_script(tx_script)
        .build()?;
    submit_tx_resilient(client, vault_id, tx_request).await?;
    client.sync_state().await?;
    Ok(())
}
