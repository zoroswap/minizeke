use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, OnceLock},
};

use alloy_primitives::{I256, U256};
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
    curve::ZoroCurve,
    miden_env::MidenNetwork,
    test_utils::touch_account,
    vault::{VaultUserAssetInfo, vault_user_asset_info_from_storage},
};

/// Maximum number of lazily allocated `(asset, user)` accounting cells.
///
/// The pool component uses five additional slots, `AuthSingleSig` uses two, and
/// `BasicWallet` uses none: `248 + 5 + 2 = 255`, the account storage limit.
pub const MAX_POOL_CELLS: usize = 248;

/// Amount of each asset a user funds into the vault before trading (service flow).
pub const USER_INITIAL_ON_CHAIN_BALANCE: u64 = 1_000;

pub const CELL_SLOT_IDS_SLOT: &str = "zoropool::cell_slot_ids";
pub const CELL_INDEX_SLOT: &str = "zoropool::cell_index";
pub const NEXT_CELL_SLOT: &str = "zoropool::next_cell";
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

fn serialize_u256<S>(value: &U256, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn serialize_i256<S>(value: &I256, serializer: S) -> std::result::Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&value.to_string())
}

#[derive(Clone, Debug, Copy, Serialize, Eq, PartialEq, Default)]
pub struct PoolBalances {
    #[serde(serialize_with = "serialize_u256")]
    pub reserve: U256,
    #[serde(serialize_with = "serialize_u256")]
    pub reserve_with_slippage: U256,
    #[serde(serialize_with = "serialize_u256")]
    pub total_liabilities: U256,
}

#[derive(Clone, Debug, Copy, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FeeSource {
    None,
    Automatic,
    Manual,
}

#[derive(Clone, Debug, Copy, Serialize)]
pub struct PoolSettings {
    #[serde(serialize_with = "serialize_i256")]
    pub beta: I256,
    #[serde(serialize_with = "serialize_i256")]
    pub c: I256,
    #[serde(serialize_with = "serialize_u256")]
    pub swap_fee: U256,
    #[serde(serialize_with = "serialize_u256")]
    pub backstop_fee: U256,
    #[serde(serialize_with = "serialize_u256")]
    pub protocol_fee: U256,
    /// Dynamic volatility surcharge when this asset is the swap input/sold asset.
    #[serde(serialize_with = "serialize_u256")]
    pub volatility_fee_in: U256,
    /// Dynamic volatility surcharge when this asset is the swap output/bought asset.
    #[serde(serialize_with = "serialize_u256")]
    pub volatility_fee_out: U256,
    pub volatility_fee_valid_until: u64,
    pub volatility_fee_version: u64,
    pub volatility_fee_source: FeeSource,
}

impl Default for PoolSettings {
    fn default() -> Self {
        Self {
            beta: I256::from_str("10000000000000000").unwrap(),
            c: I256::from_str("16000000000000000000").unwrap(),
            swap_fee: U256::from(200),
            backstop_fee: U256::from(300),
            protocol_fee: U256::from(0),
            volatility_fee_in: U256::ZERO,
            volatility_fee_out: U256::ZERO,
            volatility_fee_valid_until: 0,
            volatility_fee_version: 0,
            volatility_fee_source: FeeSource::None,
        }
    }
}

#[derive(Clone, Debug, Copy, Serialize)]
pub struct PoolMetadata {
    pub name: &'static str,
    pub asset_decimals: u8,
}

impl Default for PoolMetadata {
    fn default() -> Self {
        PoolMetadata {
            name: "Default pool",
            asset_decimals: POOL_ASSET_DECIMALS,
        }
    }
}

/// Decimals of the pool faucets deployed by the server (see `get_faucet`).
pub const POOL_ASSET_DECIMALS: u8 = 8;

/// Server-side liquidity pool state for one asset. Custody stays in the vault; this
/// struct only holds the curve accounting (reserves, liabilities, LP supply).
#[derive(Clone, Debug, Copy, Serialize, Default)]
pub struct PoolState {
    settings: PoolSettings,
    balances: PoolBalances,
    lp_total_supply: u64,
    metadata: PoolMetadata,
}

impl PoolState {
    pub fn default_with_settings(settings: PoolSettings) -> Self {
        Self {
            settings,
            balances: PoolBalances::default(),
            lp_total_supply: 0,
            metadata: PoolMetadata::default(),
        }
    }

    pub fn new(
        settings: PoolSettings,
        balances: PoolBalances,
        lp_total_supply: u64,
        metadata: PoolMetadata,
    ) -> Self {
        Self {
            settings,
            balances,
            lp_total_supply,
            metadata,
        }
    }

    pub fn update_state(&mut self, balances: PoolBalances, lp_total_supply: u64) {
        self.balances = balances;
        self.lp_total_supply = lp_total_supply;
    }

    pub fn update_balances(&mut self, balances: PoolBalances) {
        self.balances = balances;
    }

    pub fn balances(&self) -> &PoolBalances {
        &self.balances
    }

    pub fn settings(&self) -> &PoolSettings {
        &self.settings
    }

    pub fn metadata(&self) -> &PoolMetadata {
        &self.metadata
    }

    pub fn update_settings(&mut self, settings: PoolSettings) {
        self.settings = settings;
    }

    pub fn lp_total_supply(&self) -> u64 {
        self.lp_total_supply
    }

    /// # Returns
    ///
    /// new_lp_amount, new_lp_total_supply, new_pool_balances
    pub fn get_deposit_lp_amount_out(
        &self,
        deposit_amount: U256,
    ) -> Result<(U256, u64, PoolBalances)> {
        let lp_total_supply = U256::from(self.lp_total_supply);
        let old_total_liabilities = self.balances.total_liabilities;
        let old_reserve = self.balances.reserve;
        let old_reserve_with_slippage = self.balances.reserve_with_slippage;

        let curve = ZoroCurve::new(self.settings.beta.into_raw(), self.settings.c.into_raw());

        let new_reserve_with_slippage = old_reserve_with_slippage + deposit_amount;
        let mut reserve_increment = curve.inverse_diagonal(
            old_reserve,
            old_total_liabilities,
            new_reserve_with_slippage,
            U256::from(self.metadata.asset_decimals),
        );

        // fix potential numerical imprecission
        if reserve_increment < deposit_amount {
            reserve_increment = deposit_amount;
        }
        let new_lp_amount = if old_total_liabilities > U256::ZERO {
            reserve_increment * lp_total_supply / old_total_liabilities
        } else {
            reserve_increment
        };

        let new_pool_balances = PoolBalances {
            reserve: old_reserve + reserve_increment,
            reserve_with_slippage: new_reserve_with_slippage,
            total_liabilities: old_total_liabilities + reserve_increment,
        };

        let new_lp_total_supply = lp_total_supply
            .saturating_add(new_lp_amount)
            .saturating_to::<u64>();

        Ok((new_lp_amount, new_lp_total_supply, new_pool_balances))
    }

    /// # Returns
    ///
    /// payout_amount, new_lp_total_supply, new_pool_balances
    pub fn get_withdraw_asset_amount_out(
        &self,
        withdraw_amount: U256,
    ) -> Result<(U256, u64, PoolBalances)> {
        let lp_total_supply = U256::from(self.lp_total_supply);
        let old_total_liabilities = self.balances.total_liabilities;
        let old_reserve = self.balances.reserve;
        let old_reserve_with_slippage = self.balances.reserve_with_slippage;

        let reserve_decrement = if lp_total_supply == U256::ZERO {
            U256::ZERO
        } else {
            (withdraw_amount * old_total_liabilities) / lp_total_supply
        };

        let curve = ZoroCurve::new(self.settings.beta.into_raw(), self.settings.c.into_raw());

        let mut new_reserve_with_slippage = curve.psi(
            old_reserve - reserve_decrement,
            old_total_liabilities - reserve_decrement,
            U256::from(self.metadata.asset_decimals),
        );

        if new_reserve_with_slippage > old_reserve_with_slippage {
            new_reserve_with_slippage = old_reserve_with_slippage;
        }

        let mut payout_amount = old_reserve_with_slippage - new_reserve_with_slippage;

        // fix potential numerical imprecission
        if payout_amount > reserve_decrement {
            payout_amount = reserve_decrement;
        }

        let new_total_liabilities = old_total_liabilities - reserve_decrement;
        let new_reserve = old_reserve - reserve_decrement;
        let new_reserve_with_slippage = old_reserve_with_slippage - payout_amount;

        let new_pool_balances = PoolBalances {
            reserve: new_reserve,
            reserve_with_slippage: new_reserve_with_slippage,
            total_liabilities: new_total_liabilities,
        };

        // `withdraw_amount` is the LP tokens being redeemed
        let new_lp_total_supply = lp_total_supply
            .saturating_sub(withdraw_amount)
            .saturating_to::<u64>();

        Ok((payout_amount, new_lp_total_supply, new_pool_balances))
    }
}

/// Per-depositor LP share ledger, kept on the server only. Outer key is the pool's
/// faucet id, inner key is the depositor's account id.
#[derive(Clone, Debug, Default)]
pub struct LpLedger {
    shares: HashMap<AccountId, HashMap<AccountId, u64>>,
}

impl LpLedger {
    pub fn mint(&mut self, faucet_id: AccountId, depositor: AccountId, lp_amount: u64) {
        let entry = self
            .shares
            .entry(faucet_id)
            .or_default()
            .entry(depositor)
            .or_insert(0);
        *entry = entry.saturating_add(lp_amount);
    }

    /// Burns up to `lp_amount` shares; errors if the depositor doesn't own enough.
    pub fn burn(
        &mut self,
        faucet_id: AccountId,
        depositor: AccountId,
        lp_amount: u64,
    ) -> Result<()> {
        let entry = self
            .shares
            .get_mut(&faucet_id)
            .and_then(|m| m.get_mut(&depositor))
            .ok_or_else(|| anyhow!("no LP position for {} in pool {}", depositor, faucet_id))?;
        if *entry < lp_amount {
            return Err(anyhow!(
                "insufficient LP shares: has {}, burning {}",
                entry,
                lp_amount
            ));
        }
        *entry -= lp_amount;
        Ok(())
    }

    pub fn shares_of(&self, faucet_id: AccountId, depositor: AccountId) -> u64 {
        self.shares
            .get(&faucet_id)
            .and_then(|m| m.get(&depositor))
            .copied()
            .unwrap_or(0)
    }

    pub fn depositors(&self, faucet_id: AccountId) -> Vec<(AccountId, u64)> {
        self.shares
            .get(&faucet_id)
            .map(|m| m.iter().map(|(k, v)| (*k, *v)).collect())
            .unwrap_or_default()
    }
}

/// One asset-user shard: `[bought, sold, 0, 0]`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PoolCell {
    pub bought: u64,
    pub sold: u64,
}

/// An allocated `(asset, user)` cell and its concrete account-storage slot id.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PoolCellAllocation {
    pub slot_id: String,
    pub bought: u64,
    pub sold: u64,
}

impl PoolCell {
    pub fn from_word(word: Word) -> Self {
        let e = word.as_elements();
        Self {
            bought: e[0].as_canonical_u64(),
            sold: e[1].as_canonical_u64(),
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

pub fn get_pool_cell_slot_name(index: u16) -> StorageSlotName {
    storage_slot_name(format!("zoropool::cell_{index}").as_str())
}

fn asset_user_key(asset_id: AccountId, user_id: AccountId) -> Word {
    Word::new([
        asset_id.suffix(),
        asset_id.prefix().as_felt(),
        user_id.suffix(),
        user_id.prefix().as_felt(),
    ])
}

/// Reads one `(asset, user)` allocation from fetched pool storage.
///
/// `None` means no cell has been allocated. An allocated zero-valued cell is returned as
/// `Some` with both counters set to zero.
pub fn pool_cell_allocation_from_storage(
    storage: &AccountStorage,
    asset_id: AccountId,
    user_id: AccountId,
) -> Result<Option<PoolCellAllocation>> {
    let slot_id_word = storage
        .get_map_item(
            &storage_slot_name(CELL_INDEX_SLOT),
            asset_user_key(asset_id, user_id),
        )
        .map_err(|e| anyhow!("failed to read pool cell index: {e:?}"))?;

    if slot_id_word == Word::new([Felt::ZERO; 4]) {
        return Ok(None);
    }

    let id = slot_id_word.as_elements();
    let slot = storage
        .slots()
        .iter()
        .find(|slot| {
            let candidate = slot.name().id();
            candidate.suffix() == id[0] && candidate.prefix() == id[1]
        })
        .ok_or_else(|| {
            anyhow!(
                "pool cell index for asset {} and user {} references an unknown slot",
                asset_id.to_hex(),
                user_id.to_hex()
            )
        })?;

    let cell = PoolCell::from_word(slot.content().value());
    Ok(Some(PoolCellAllocation {
        slot_id: slot.name().id().to_string(),
        bought: cell.bought,
        sold: cell.sold,
    }))
}

/// Reads one `(asset, user)` cell, treating an unallocated key as an empty cell.
pub fn pool_cell_from_storage(
    storage: &AccountStorage,
    asset_id: AccountId,
    user_id: AccountId,
) -> Result<PoolCell> {
    Ok(
        match pool_cell_allocation_from_storage(storage, asset_id, user_id)? {
            Some(allocation) => PoolCell {
                bought: allocation.bought,
                sold: allocation.sold,
            },
            None => PoolCell::default(),
        },
    )
}

/// Fetches a user's derived pool balance for one asset over RPC: reads the vault totals and
/// the pool trade counters and combines them.
pub async fn get_user_balance_from_pool(
    pool_id: AccountId,
    vault_id: AccountId,
    asset_id: AccountId,
    user_id: AccountId,
) -> Result<u64> {
    Ok(
        get_user_balance_details_from_pool(pool_id, vault_id, asset_id, user_id)
            .await?
            .0,
    )
}

/// Fetches the amount the user can spend after pending redeems are reserved.
pub async fn get_user_available_balance_from_pool(
    pool_id: AccountId,
    vault_id: AccountId,
    asset_id: AccountId,
    user_id: AccountId,
) -> Result<u64> {
    Ok(
        get_user_balance_details_from_pool(pool_id, vault_id, asset_id, user_id)
            .await?
            .1,
    )
}

async fn get_user_balance_details_from_pool(
    pool_id: AccountId,
    vault_id: AccountId,
    asset_id: AccountId,
    user_id: AccountId,
) -> Result<(u64, u64)> {
    let vault_storage = fetch_account_storage_from_rpc(vault_id).await?;
    let vault_info = vault_user_asset_info_from_storage(&vault_storage, asset_id, user_id)?;

    let pool_storage = fetch_account_storage_from_rpc(pool_id).await?;
    let cell = pool_cell_from_storage(&pool_storage, asset_id, user_id)?;

    Ok(derive_balance_details(&vault_info, cell.bought, cell.sold))
}

pub async fn deploy_pool(
    client: &mut Client<FilesystemKeyStore>,
    vault_id: AccountId,
) -> Result<Account> {
    let mut init_seed = [0_u8; 32];
    client.rng().fill_bytes(&mut init_seed);

    let key_pair = AuthSecretKey::new_ecdsa_k256_keccak();
    let pool_component = build_pool_component(client.code_builder(), vault_id)?;

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

pub fn build_pool_component(cb: CodeBuilder, vault_id: AccountId) -> Result<AccountComponent> {
    let vault_proc_root = vault_trading_details_proc_root(cb.clone())?;
    let lib = compile_pool_code(cb)?;

    let zero_word = Word::new([Felt::ZERO; 4]);

    let mut slots: Vec<StorageSlot> = Vec::with_capacity(MAX_POOL_CELLS + 5);

    // Generic cells are assigned lazily to full (asset, user) keys.
    for i in 0..MAX_POOL_CELLS {
        slots.push(StorageSlot::with_value(
            get_pool_cell_slot_name(i as u16),
            zero_word,
        ));
    }

    // Dense index -> hashed cell-slot id (slot ids are underivable in MASM).
    let slot_ids_map = StorageMap::with_entries((0..MAX_POOL_CELLS).map(|i| {
        let slot_id = get_pool_cell_slot_name(i as u16).id();
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
    .map_err(|e| anyhow!("failed to build pool cell slot ids map: {e:?}"))?;
    slots.push(StorageSlot::with_map(
        storage_slot_name(CELL_SLOT_IDS_SLOT),
        slot_ids_map,
    ));

    slots.push(StorageSlot::with_map(
        storage_slot_name(CELL_INDEX_SLOT),
        StorageMap::new(),
    ));

    slots.push(StorageSlot::with_value(
        storage_slot_name(NEXT_CELL_SLOT),
        zero_word,
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

    let component = AccountComponent::new(
        lib,
        slots,
        AccountComponentMetadata::new("zoro_miden::pool"),
    )?;

    Ok(component)
}

#[cfg(test)]
mod tests {
    use miden_client::testing::account_id::ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE;

    use super::*;
    use crate::assembly_utils::pool_balance_details_proc_root;

    /// Compiles both components + extracts both FPI proc roots: validates all the MASM and
    /// the storage layout without needing a running node.
    #[test]
    fn test_build_components_and_roots() -> Result<()> {
        let vault_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE)?;
        let cb = CodeBuilder::new();
        build_pool_component(cb.clone(), vault_id)?;
        crate::vault::build_vault_component(cb.clone(), vault_id)?;

        let vault_root = vault_trading_details_proc_root(cb.clone())?;
        let pool_root = pool_balance_details_proc_root(cb)?;
        assert_ne!(vault_root, Word::new([Felt::ZERO; 4]));
        assert_ne!(pool_root, Word::new([Felt::ZERO; 4]));
        Ok(())
    }

    #[test]
    fn pending_redeems_reduce_available_but_not_gross_balance() {
        let vault_info = VaultUserAssetInfo {
            total_funding: 1_000,
            total_initiated_redeems: 400,
            total_redeems: 100,
        };

        let (balance, available) = derive_balance_details(&vault_info, 200, 50);
        assert_eq!(balance, 1_050);
        assert_eq!(available, 750);
    }
}
