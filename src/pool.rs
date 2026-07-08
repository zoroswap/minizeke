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
use tracing::info;

use crate::{
    assembly_utils::{link_operator, link_storage_utils, read_masm_file, storage_slot_name},
    miden_env::MidenNetwork,
    miden_execution::user_id_word,
    user::User,
};

pub const USER_INITIAL_ON_CHAIN_BALANCE: u64 = 1_000;

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

pub async fn get_user_balance_from_pool(
    pool_id: AccountId,
    user_index: u16,
    asset_index: u8,
) -> Result<u64> {
    if asset_index > 1 {
        return Err(anyhow!("asset_index must be 0 or 1, got {asset_index}"));
    }

    let storage = fetch_account_storage_from_rpc(pool_id).await?;
    let slot_name = get_user_balance_storage_slot_name(user_index);

    let word = storage
        .get_item(&slot_name)
        .map_err(|e| anyhow!("failed to read storage slot {}: {e:?}", slot_name.as_str()))?;

    Ok(word.as_elements()[asset_index as usize].as_canonical_u64())
}

#[derive(Debug, Copy, Clone, Serialize)]
pub struct PoolState {
    pub balance: u64,
}

pub async fn deploy_pool(
    client: &mut Client<FilesystemKeyStore>,
    users: Vec<User>,
) -> Result<(Account, AccountComponent)> {
    let mut init_seed = [0_u8; 32];
    client.rng().fill_bytes(&mut init_seed);

    let key_pair = AuthSecretKey::new_ecdsa_k256_keccak();
    let user_ids: Vec<AccountId> = users.iter().map(|u| u.id()).collect();
    let users_keys: Vec<(Word, Word)> = users
        .iter()
        .map(|user| {
            let pubkey: Word = user.pubkey().to_commitment().into();
            let user = user_id_word(user.id());
            (user, pubkey)
        })
        .collect();

    let operator_component = build_operator_component(client.code_builder(), &users_keys)?;
    let pool_component = build_pool_component(user_ids, client.code_builder())?;

    let pool_contract = AccountBuilder::new(init_seed)
        .account_type(AccountType::Public)
        .with_component(operator_component.clone())
        .with_component(pool_component.clone())
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
        "contract id: {:?}",
        pool_contract
            .id()
            .to_bech32(MidenNetwork::from_env().endpoint().to_network_id())
    );

    Ok((pool_contract, pool_component))
}

pub fn get_user_balance_storage_slot_names(n_users: usize) -> Vec<StorageSlotName> {
    let mut slot_names: Vec<StorageSlotName> = Vec::with_capacity(100);
    for i in 0..n_users {
        slot_names.push(storage_slot_name(
            format!("pool::user_{i}_balance").as_str(),
        ));
    }
    slot_names
}

pub fn get_user_balance_storage_slot_name(index: u16) -> StorageSlotName {
    storage_slot_name(format!("pool::user_{index}_balance").as_str())
}

pub fn build_pool_component(users: Vec<AccountId>, cb: CodeBuilder) -> Result<AccountComponent> {
    let code = read_masm_file(&["accounts", "pool.masm"])?;
    let cb = link_storage_utils(cb)?;
    let cb = link_operator(cb)?;
    let lib = cb.compile_component_code("zoro_miden::pool", &code)?;

    let user_amount = USER_INITIAL_ON_CHAIN_BALANCE;

    let user_balance: Word = [
        Felt::new(user_amount).unwrap(),
        Felt::new(user_amount).unwrap(),
        Felt::ZERO,
        Felt::ZERO,
    ]
    .into();

    let slot_names = get_user_balance_storage_slot_names(users.len());

    let component = AccountComponent::new(
        lib,
        slot_names[..users.len()]
            .iter()
            .map(|name| StorageSlot::with_value(name.clone(), user_balance))
            .collect(),
        AccountComponentMetadata::new("zoro_miden::pool"),
    )?;

    Ok(component)
}

pub fn build_operator_component(
    code_builder: CodeBuilder,
    depositors: &[(Word, Word)],
) -> Result<AccountComponent> {
    let code = read_masm_file(&["accounts", "operator.masm"])?;
    let library = code_builder
        .compile_component_code("zoro_miden::operator", code)
        .expect("operator.masm must assemble");

    let keys_slot = StorageSlotName::new("operator::depositor_keys").expect("slot name must parse");
    // let nonce_slot = StorageSlotName::new(LAST_NONCE_SLOT).expect("slot name must parse");
    // let auth_slot = StorageSlotName::new(LAST_AUTH_SLOT).expect("slot name must parse");

    let map = StorageMap::with_entries(depositors.iter().map(|(uid, comm)| {
        info!("depositor {uid:?}, commitment {comm:?}");
        (StorageMapKey::new(*uid), *comm)
    }))
    .expect("depositor map must build");

    let component = AccountComponent::new(
        library,
        vec![
            StorageSlot::with_map(keys_slot, map),
            // StorageSlot::with_value(nonce_slot, Word::from([0u32, 0, 0, 0])),
            // StorageSlot::with_value(auth_slot, Word::from([0u32, 0, 0, 0])),
        ],
        AccountComponentMetadata::mock("zoro_miden::operator"),
    )
    .expect("operator component must build");

    Ok(component)
}
