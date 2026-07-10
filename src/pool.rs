use std::sync::{Arc, OnceLock};

use anyhow::{Result, anyhow};
use miden_client::{
    Client,
    account::{
        Account, AccountBuilder, AccountComponent, AccountId, AccountStorage, AccountType,
        StorageMap, StorageMapKey, StorageSlot, StorageSlotName, component::BasicWallet,
    },
    assembly::CodeBuilder,
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig},
    keystore::{FilesystemKeyStore, Keystore},
    rpc::{GrpcClient, NodeRpcClient},
};
use miden_core::{Felt, Word};
use miden_protocol::account::AccountComponentMetadata;
use rand::RngCore;
use serde::Serialize;

use crate::{
    assembly_utils::{compile_pool_code, storage_slot_name, vault_trading_details_proc_root},
    miden_env::MidenNetwork,
    test_utils::touch_account,
    vault::{VaultUserAssetInfo, vault_user_asset_info_from_storage, vault_user_registration},
};

/// Maximum number of users a pool supports (one trades value slot per user).
pub const MAX_POOL_USERS: usize = 128;

/// Amount of each asset a user funds into the vault before trading (service flow).
pub const USER_INITIAL_ON_CHAIN_BALANCE: u64 = 1_000;

pub const USER_SLOT_IDS_SLOT: &str = "zoropool::user_slot_ids";
pub const ASSETS_SLOT: &str = "zoropool::assets";
pub const VAULT_ACCOUNT_ID_SLOT: &str = "zoropool::vault_account_id";
pub const USER_TRADING_DETAILS_PROC_ROOT_SLOT: &str = "zoropool::user_trading_details_proc_root";

static FETCH_RPC: OnceLock<Arc<GrpcClient>> = OnceLock::new();

fn get_fetch_rpc() -> &'static Arc<GrpcClient> {
    FETCH_RPC.get_or_init(|| {
        let endpoint = MidenNetwork::from_env().endpoint();
        Arc::new(GrpcClient::new(&endpoint, 30_000))
    })
}

pub async fn fetch_account_storage_from_rpc(account_id: AccountId) -> Result<AccountStorage> {
    let account = get_fetch_rpc()
        .get_account_details(account_id)
        .await
        .map_err(|e| anyhow!("failed to fetch account from RPC: {e:?}"))?
        .ok_or_else(|| anyhow!("account {} not found or is private", account_id.to_hex()))?;

    Ok(account.storage().clone())
}

#[derive(Debug, Copy, Clone, Serialize)]
pub struct PoolState {
    pub balance: u64,
}

/// A user's cumulative trade counters for both pool assets:
/// `[bought0, sold0, bought1, sold1]`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UserTrades {
    pub bought: [u64; 2],
    pub sold: [u64; 2],
}

impl UserTrades {
    pub fn from_word(word: Word) -> Self {
        let e = word.as_elements();
        Self {
            bought: [e[0].as_canonical_u64(), e[2].as_canonical_u64()],
            sold: [e[1].as_canonical_u64(), e[3].as_canonical_u64()],
        }
    }
}

/// Derives (balance, available) for one asset from the vault's totals and the pool's
/// trade counters — the same formula the MASM uses:
///
///   balance   = total_funding - total_redeems + bought - sold
///   available = balance - pending_redeems
pub fn derive_balance_details(
    vault_info: &VaultUserAssetInfo,
    bought: u64,
    sold: u64,
) -> (u64, u64) {
    let balance = vault_info.total_funding + bought - vault_info.total_redeems - sold;
    let available = balance.saturating_sub(vault_info.pending_redeem());
    (balance, available)
}

pub fn get_user_trades_slot_name(index: u16) -> StorageSlotName {
    storage_slot_name(format!("pool::user_{index}_trades").as_str())
}

/// Reads a user's trade counters from an already-fetched pool [`AccountStorage`].
pub fn user_trades_from_storage(storage: &AccountStorage, user_index: u16) -> Result<UserTrades> {
    let slot_name = get_user_trades_slot_name(user_index);
    let word = storage
        .get_item(&slot_name)
        .map_err(|e| anyhow!("failed to read storage slot {}: {e:?}", slot_name.as_str()))?;
    Ok(UserTrades::from_word(word))
}

/// Fetches a user's derived pool balance for one asset over RPC: reads the vault totals and
/// the pool trade counters and combines them.
pub async fn get_user_balance_from_pool(
    pool_id: AccountId,
    vault_id: AccountId,
    asset_id: AccountId,
    asset_index: u8,
    user_id: AccountId,
) -> Result<u64> {
    if asset_index > 1 {
        return Err(anyhow!("asset_index must be 0 or 1, got {asset_index}"));
    }

    let vault_storage = fetch_account_storage_from_rpc(vault_id).await?;
    let vault_info = vault_user_asset_info_from_storage(&vault_storage, asset_id, user_id)?;
    let (user_index, _) = vault_user_registration(&vault_storage, user_id)?
        .ok_or_else(|| anyhow!("user {} is not registered on the vault", user_id.to_hex()))?;

    let pool_storage = fetch_account_storage_from_rpc(pool_id).await?;
    let trades = user_trades_from_storage(&pool_storage, user_index as u16)?;

    let (balance, _) = derive_balance_details(
        &vault_info,
        trades.bought[asset_index as usize],
        trades.sold[asset_index as usize],
    );
    Ok(balance)
}

pub async fn deploy_pool(
    client: &mut Client<FilesystemKeyStore>,
    vault_id: AccountId,
    asset0: AccountId,
    asset1: AccountId,
) -> Result<Account> {
    let mut init_seed = [0_u8; 32];
    client.rng().fill_bytes(&mut init_seed);

    let key_pair = AuthSecretKey::new_ecdsa_k256_keccak();
    let pool_component = build_pool_component(client.code_builder(), vault_id, asset0, asset1)?;

    let pool_contract = AccountBuilder::new(init_seed)
        .account_type(AccountType::Public)
        .with_component(pool_component)
        .with_auth_component(AuthSingleSig::new(
            key_pair.public_key().to_commitment(),
            AuthScheme::EcdsaK256Keccak,
        ))
        .with_component(BasicWallet)
        .build()?;

    let keystore = FilesystemKeyStore::new("keystore".into())?;
    keystore
        .add_key(&key_pair, pool_contract.id())
        .await
        .map_err(|e| anyhow!("Failed to add key: {e:?}"))?;

    println!(
        "pool contract commitment hash: {:?}",
        pool_contract.to_commitment().to_hex()
    );
    println!(
        "pool contract id: {:?}",
        pool_contract
            .id()
            .to_bech32(MidenNetwork::from_env().endpoint().to_network_id())
    );

    client.add_account(&pool_contract, true).await?;
    client.sync_state().await?;
    touch_account(client, &pool_contract.id()).await?;
    let pool_contract = client.try_get_account(pool_contract.id()).await?;

    Ok(pool_contract)
}

pub fn build_pool_component(
    cb: CodeBuilder,
    vault_id: AccountId,
    asset0: AccountId,
    asset1: AccountId,
) -> Result<AccountComponent> {
    let vault_proc_root = vault_trading_details_proc_root(cb.clone())?;
    let lib = compile_pool_code(cb)?;

    let zero_word = Word::new([Felt::ZERO; 4]);

    let mut slots: Vec<StorageSlot> = Vec::with_capacity(MAX_POOL_USERS + 4);

    // one trades slot per user: [bought0, sold0, bought1, sold1]
    for i in 0..MAX_POOL_USERS {
        slots.push(StorageSlot::with_value(
            get_user_trades_slot_name(i as u16),
            zero_word,
        ));
    }

    // index -> hashed trades-slot id lookup (slot ids are hashed names, underivable in MASM)
    let slot_ids_map = StorageMap::with_entries((0..MAX_POOL_USERS).map(|i| {
        let slot_id = get_user_trades_slot_name(i as u16).id();
        (
            StorageMapKey::new(Word::new([
                Felt::new(i as u64).unwrap(),
                Felt::ZERO,
                Felt::ZERO,
                Felt::ZERO,
            ])),
            Word::new([slot_id.suffix(), slot_id.prefix(), Felt::ZERO, Felt::ZERO]),
        )
    }))
    .map_err(|e| anyhow!("failed to build user slot ids map: {e:?}"))?;
    slots.push(StorageSlot::with_map(
        storage_slot_name(USER_SLOT_IDS_SLOT),
        slot_ids_map,
    ));

    // pool assets: [asset0_suffix, asset0_prefix, asset1_suffix, asset1_prefix]
    slots.push(StorageSlot::with_value(
        storage_slot_name(ASSETS_SLOT),
        Word::new([
            asset0.suffix(),
            asset0.prefix().as_felt(),
            asset1.suffix(),
            asset1.prefix().as_felt(),
        ]),
    ));

    slots.push(StorageSlot::with_value(
        storage_slot_name(VAULT_ACCOUNT_ID_SLOT),
        Word::new([
            vault_id.suffix(),
            vault_id.prefix().as_felt(),
            Felt::ZERO,
            Felt::ZERO,
        ]),
    ));

    slots.push(StorageSlot::with_value(
        storage_slot_name(USER_TRADING_DETAILS_PROC_ROOT_SLOT),
        vault_proc_root,
    ));

    let component = AccountComponent::new(lib, slots, AccountComponentMetadata::new("zoro_miden::pool"))?;

    Ok(component)
}

#[cfg(test)]
mod tests {
    use miden_client::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
    };

    use super::*;
    use crate::assembly_utils::pool_balance_details_proc_root;

    /// Compiles both components + extracts both FPI proc roots: validates all the MASM and
    /// the storage layout without needing a running node.
    #[test]
    fn test_build_components_and_roots() -> Result<()> {
        let vault_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE)?;
        let asset0 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
        let asset1 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2)?;

        let cb = CodeBuilder::new();
        build_pool_component(cb.clone(), vault_id, asset0, asset1)?;
        crate::vault::build_vault_component(cb.clone())?;

        let vault_root = vault_trading_details_proc_root(cb.clone())?;
        let pool_root = pool_balance_details_proc_root(cb)?;
        assert_ne!(vault_root, Word::new([Felt::ZERO; 4]));
        assert_ne!(pool_root, Word::new([Felt::ZERO; 4]));
        Ok(())
    }
}
