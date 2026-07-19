use std::collections::BTreeSet;

use anyhow::{Result, anyhow};
use miden_client::{
    Client,
    account::{
        Account, AccountBuilder, AccountComponent, AccountId, AccountStorage, AccountType,
        StorageSlot, component::BasicWallet,
    },
    assembly::CodeBuilder,
    keystore::FilesystemKeyStore,
};
use miden_core::{Felt, Word};
use miden_protocol::{account::AccountComponentMetadata, note::NoteScriptRoot};
use miden_standards::account::auth::AuthNetworkAccount;
use rand::RngCore;

use crate::{
    assembly_utils::{
        compile_vault_code, pool_balance_details_proc_root, print_library_exports,
        storage_slot_name,
    },
    miden_env::MidenNetwork,
    note::{AddPoolInstructions, CheckpointInstructions, NoteKind, ZekeNote, ZekeNoteInstructions},
    test_utils::{send_note_to_network, touch_account},
};

pub const USER_ASSET_TOTAL_FUNDING_SLOT: &str = "zorovault::user_asset_total_funding";
pub const USER_ASSET_TOTAL_REDEEMS_SLOT: &str = "zorovault::user_asset_total_redeems";
pub const USER_ASSET_TOTAL_INITIATED_REDEEMS_SLOT: &str =
    "zorovault::user_asset_total_initiated_redeems";
pub const USER_PUBKEYS_SLOT: &str = "zorovault::user_pubkeys";
pub const AUTHORIZED_POOLS_SLOT: &str = "zorovault::authorized_pools";
pub const USER_POOL_SLOT: &str = "zorovault::user_pool";
pub const ACTIVE_POOL_SLOT: &str = "zorovault::active_pool";
pub const POOL_USER_CAPACITY_SLOT: &str = "zorovault::pool_user_capacity";
pub const POOL_USER_COUNTS_SLOT: &str = "zorovault::pool_user_counts";
pub const OPERATOR_ACCOUNT_ID_SLOT: &str = "zorovault::operator_account_id";
pub const USER_POOL_BALANCE_DETAILS_PROC_ROOT_SLOT: &str =
    "zorovault::user_pool_balance_details_proc_root";
pub const LP_ENTITLEMENTS_SLOT: &str = "zorovault::lp_entitlements";
pub const LP_WITHDRAWN_SLOT: &str = "zorovault::lp_withdrawn";

/// Default registered-users-per-shard budget. Paired with [`DEFAULT_ASSET_CAPACITY`] so
/// worst-case cells (`users * assets`) stay under the pool's 247-cell limit.
pub const DEFAULT_POOL_USER_CAPACITY: u32 = 16;
/// Default asset budget used when sizing `pool_user_capacity`.
pub const DEFAULT_ASSET_CAPACITY: u32 = 15;

pub async fn deploy_vault(
    client: &mut Client<FilesystemKeyStore>,
    operator_id: AccountId,
    pool_user_capacity: u32,
) -> Result<Account> {
    let mut init_seed = [0_u8; 32];
    client.rng().fill_bytes(&mut init_seed);

    let vault_component =
        build_vault_component(client.code_builder(), operator_id, pool_user_capacity)?;
    let allowed_note_roots: BTreeSet<NoteScriptRoot> = NoteKind::NETWORK_KINDS
        .iter()
        .map(|kind| ZekeNote::get_note_script(client.code_builder(), kind.masm_name()))
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .map(|script| script.root())
        .collect();
    let network_auth = AuthNetworkAccount::with_allowed_notes(allowed_note_roots)
        .map_err(|e| anyhow!("failed to build network-account auth component: {e}"))?;

    let vault_contract = AccountBuilder::new(init_seed)
        .account_type(AccountType::Public)
        .with_component(vault_component.clone())
        .with_auth_component(network_auth)
        .with_component(BasicWallet)
        .build()?;

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

pub fn build_vault_component(
    cb: CodeBuilder,
    operator_id: AccountId,
    pool_user_capacity: u32,
) -> Result<AccountComponent> {
    // storage-less pool build just to extract the FPI proc root (breaks the circular
    // vault <-> pool dependency; MAST roots do not depend on storage)
    let pool_proc_root = pool_balance_details_proc_root(cb.clone())?;

    let lib = compile_vault_code(cb)?;
    print_library_exports(lib.as_library());

    let zero_word = Word::new([Felt::ZERO; 4]);
    let capacity_felt = Felt::new(u64::from(pool_user_capacity))
        .map_err(|error| anyhow!("invalid pool_user_capacity {pool_user_capacity}: {error:?}"))?;

    let component = AccountComponent::new(
        lib,
        vec![
            StorageSlot::with_empty_map(storage_slot_name(USER_ASSET_TOTAL_FUNDING_SLOT)),
            StorageSlot::with_empty_map(storage_slot_name(USER_ASSET_TOTAL_REDEEMS_SLOT)),
            StorageSlot::with_empty_map(storage_slot_name(USER_ASSET_TOTAL_INITIATED_REDEEMS_SLOT)),
            StorageSlot::with_empty_map(storage_slot_name(USER_PUBKEYS_SLOT)),
            StorageSlot::with_empty_map(storage_slot_name(AUTHORIZED_POOLS_SLOT)),
            StorageSlot::with_empty_map(storage_slot_name(USER_POOL_SLOT)),
            StorageSlot::with_value(storage_slot_name(ACTIVE_POOL_SLOT), zero_word),
            StorageSlot::with_value(
                storage_slot_name(POOL_USER_CAPACITY_SLOT),
                Word::from([capacity_felt, Felt::ZERO, Felt::ZERO, Felt::ZERO]),
            ),
            StorageSlot::with_empty_map(storage_slot_name(POOL_USER_COUNTS_SLOT)),
            StorageSlot::with_value(
                storage_slot_name(OPERATOR_ACCOUNT_ID_SLOT),
                Word::from([
                    operator_id.suffix(),
                    operator_id.prefix().as_felt(),
                    Felt::ZERO,
                    Felt::ZERO,
                ]),
            ),
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

/// Authorizes a pool on the vault and makes it active for subsequent user registrations.
pub async fn add_pool_to_vault(
    client: &mut Client<FilesystemKeyStore>,
    operator_id: AccountId,
    vault_id: AccountId,
    pool_id: AccountId,
) -> Result<()> {
    let note = ZekeNote::new(
        ZekeNoteInstructions::AddPool(AddPoolInstructions {
            operator_id,
            vault_id,
            pool_id,
        }),
        client.code_builder(),
    )?;
    send_note_to_network(client, &note, operator_id).await
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
/// Returns `None` when the user is not registered, otherwise the pubkey commitment.
pub fn vault_user_registration(
    storage: &AccountStorage,
    user_id: AccountId,
) -> Result<Option<Word>> {
    let key = vault_user_key(user_id);
    let pubkey = storage
        .get_map_item(&storage_slot_name(USER_PUBKEYS_SLOT), key)
        .map_err(|e| anyhow!("failed to read {USER_PUBKEYS_SLOT}: {e:?}"))?;
    if pubkey == Word::new([Felt::ZERO; 4]) {
        return Ok(None);
    }
    Ok(Some(pubkey))
}

/// Pool key word for map lookups: `[pool_suffix, pool_prefix, 0, 0]`.
pub fn vault_pool_key(pool_id: AccountId) -> Word {
    Word::from([
        pool_id.suffix(),
        pool_id.prefix().as_felt(),
        Felt::ZERO,
        Felt::ZERO,
    ])
}

/// Reads the vault's configured per-pool registration capacity from a storage snapshot.
pub fn pool_user_capacity_from_storage(storage: &AccountStorage) -> Result<u32> {
    let word = storage
        .get_item(&storage_slot_name(POOL_USER_CAPACITY_SLOT))
        .map_err(|e| anyhow!("failed to read {POOL_USER_CAPACITY_SLOT}: {e:?}"))?;
    let capacity = word[0].as_canonical_u64();
    u32::try_from(capacity)
        .map_err(|_| anyhow!("{POOL_USER_CAPACITY_SLOT} value {capacity} does not fit in u32"))
}

/// Reads how many users are registered against `pool_id`.
pub fn pool_user_count_from_storage(storage: &AccountStorage, pool_id: AccountId) -> Result<u32> {
    let word = storage
        .get_map_item(
            &storage_slot_name(POOL_USER_COUNTS_SLOT),
            vault_pool_key(pool_id),
        )
        .map_err(|e| anyhow!("failed to read {POOL_USER_COUNTS_SLOT}: {e:?}"))?;
    let count = word[0].as_canonical_u64();
    u32::try_from(count)
        .map_err(|_| anyhow!("{POOL_USER_COUNTS_SLOT} value {count} does not fit in u32"))
}

/// Reads the vault's current active pool for new registrations, if set.
pub fn active_pool_from_storage(storage: &AccountStorage) -> Result<Option<AccountId>> {
    let pool = storage
        .get_item(&storage_slot_name(ACTIVE_POOL_SLOT))
        .map_err(|e| anyhow!("failed to read {ACTIVE_POOL_SLOT}: {e:?}"))?;
    if pool == Word::new([Felt::ZERO; 4]) {
        return Ok(None);
    }
    Ok(Some(
        AccountId::try_from_elements(pool[0], pool[1])
            .map_err(|e| anyhow!("invalid pool account id in {ACTIVE_POOL_SLOT}: {e:?}"))?,
    ))
}

/// Reads the authorized pool bound to a user from an already-fetched vault storage snapshot.
pub fn user_pool_from_storage(
    storage: &AccountStorage,
    user_id: AccountId,
) -> Result<Option<AccountId>> {
    let pool = storage
        .get_map_item(&storage_slot_name(USER_POOL_SLOT), vault_user_key(user_id))
        .map_err(|e| anyhow!("failed to read {USER_POOL_SLOT}: {e:?}"))?;
    if pool == Word::new([Felt::ZERO; 4]) {
        return Ok(None);
    }

    let authorized = storage
        .get_map_item(&storage_slot_name(AUTHORIZED_POOLS_SLOT), pool)
        .map_err(|e| anyhow!("failed to read {AUTHORIZED_POOLS_SLOT}: {e:?}"))?[0]
        .as_canonical_u64();
    if authorized != 1 {
        return Err(anyhow!(
            "user {} is bound to unauthorized pool",
            user_id.to_hex()
        ));
    }

    Ok(Some(
        AccountId::try_from_elements(pool[0], pool[1])
            .map_err(|e| anyhow!("invalid pool account id in {USER_POOL_SLOT}: {e:?}"))?,
    ))
}

/// Resolves a registered user's assigned pool shard from public vault storage.
///
/// Returns `None` only when the user is not registered. A registered user without a pool
/// assignment is treated as inconsistent vault state.
pub fn user_placement_from_storage(
    storage: &AccountStorage,
    user_id: AccountId,
) -> Result<Option<AccountId>> {
    if vault_user_registration(storage, user_id)?.is_none() {
        return Ok(None);
    }
    user_pool_from_storage(storage, user_id)?
        .map(Some)
        .ok_or_else(|| {
            anyhow!(
                "registered user {} has no assigned pool shard",
                user_id.to_hex()
            )
        })
}

/// Fetches the vault storage and returns the authorized pool bound to `user_id`.
pub async fn get_user_pool(
    client: &Client<FilesystemKeyStore>,
    vault_id: AccountId,
    user_id: AccountId,
) -> Result<Option<AccountId>> {
    let storage = get_vault_storage(client, vault_id).await?;
    user_pool_from_storage(&storage, user_id)
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

/// Operator-authored maintenance note: raises `lp_id`'s entitlement for `asset_id` to
/// `new_entitlement` (principal + accrued fees, computed by the server from LP shares).
/// The vault panics if `new_entitlement` is below the current entitlement, so this can
/// only ever raise the counter.
pub async fn checkpoint_lp_entitlement_on_vault(
    client: &mut Client<FilesystemKeyStore>,
    operator_id: AccountId,
    vault_id: AccountId,
    asset_id: AccountId,
    lp_id: AccountId,
    new_entitlement: u64,
) -> Result<()> {
    let note = ZekeNote::new(
        ZekeNoteInstructions::Checkpoint(CheckpointInstructions {
            operator_id,
            vault_id,
            asset_id,
            lp_id,
            new_entitlement,
        }),
        client.code_builder(),
    )?;
    send_note_to_network(client, &note, operator_id).await
}

#[cfg(test)]
mod tests {
    use miden_client::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE;
    use miden_standards::account::auth::NetworkAccount;

    use super::*;

    #[test]
    fn vault_builds_as_network_account_with_all_note_scripts_allowed() -> Result<()> {
        let operator_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE)?;
        let code_builder = CodeBuilder::new();
        let roots = NoteKind::NETWORK_KINDS
            .iter()
            .map(|kind| ZekeNote::get_note_script(code_builder.clone(), kind.masm_name()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .map(|script| script.root())
            .collect::<BTreeSet<_>>();

        let account = AccountBuilder::new([7; 32])
            .account_type(AccountType::Public)
            .with_component(build_vault_component(
                code_builder,
                operator_id,
                DEFAULT_POOL_USER_CAPACITY,
            )?)
            .with_auth_component(AuthNetworkAccount::with_allowed_notes(roots.clone())?)
            .with_component(BasicWallet)
            .build()?;
        let network_account = NetworkAccount::new(account)?;

        assert_eq!(
            network_account.allowed_notes().allowed_script_roots(),
            &roots
        );
        Ok(())
    }
}
