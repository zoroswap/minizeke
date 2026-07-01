use std::{fs::read_to_string, path::PathBuf};

use anyhow::{Result, anyhow};
use miden_client::{
    Client,
    account::{
        Account, AccountBuilder, AccountComponent, AccountId, AccountType, StorageMap,
        StorageMapKey, StorageSlot, StorageSlotName, component::BasicWallet,
    },
    assembly::CodeBuilder,
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig, PublicKeyCommitment},
    keystore::{FilesystemKeyStore, Keystore},
    rpc::Endpoint,
};
use miden_core::{Felt, Word, ZERO};
use miden_protocol::account::AccountComponentMetadata;
use rand::RngCore;
use serde::Serialize;
use tracing::info;

use crate::{miden_execution::user_id_word, user::User};

#[derive(Debug, Copy, Clone, Serialize)]
pub struct PoolState {
    pub balance: u64,
}

pub async fn deploy_pool(
    client: &mut Client<FilesystemKeyStore>,
    users: Vec<User>,
    pool_0_balance: u64,
    pool_1_balance: u64,
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
    let pool_component = build_pool_component(
        pool_0_balance,
        pool_1_balance,
        user_ids,
        client.code_builder(),
    )?;

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
            .to_bech32(Endpoint::devnet().to_network_id())
    );

    Ok((pool_contract, pool_component))
}

pub fn get_user_balance_storage_slot_names() -> Vec<StorageSlotName> {
    let mut slot_names: Vec<StorageSlotName> = Vec::with_capacity(100);
    for i in 0..100 {
        slot_names.push(n(format!("pool::user_{i}_balance").as_str()));
    }
    slot_names
}

pub fn build_pool_component(
    pool_0_balance: u64,
    pool_1_balance: u64,
    users: Vec<AccountId>,
    cb: CodeBuilder,
) -> Result<AccountComponent> {
    let code = read_masm_file(&["accounts", "pool.masm"])?;
    let cb = link_storage_utils(cb)?;
    let cb = link_operator(cb)?;
    let lib = cb.compile_component_code("zoro_miden::pool", &code)?;

    let user_amount = 1_000;

    let pool_balance_0: Word = [
        Felt::new(pool_0_balance).unwrap(),
        Felt::ZERO,
        Felt::ZERO,
        Felt::ZERO,
    ]
    .into();

    let pool_balance_1: Word = [
        Felt::new(pool_1_balance).unwrap(),
        Felt::ZERO,
        Felt::ZERO,
        Felt::ZERO,
    ]
    .into();

    let user_balance: Word = [
        Felt::new(user_amount).unwrap(),
        Felt::new(user_amount).unwrap(),
        Felt::ZERO,
        Felt::ZERO,
    ]
    .into();

    let slot_names = get_user_balance_storage_slot_names();

    let component = AccountComponent::new(
        lib,
        slot_names[..users.len()]
            .iter()
            .map(|name| StorageSlot::with_value(name.clone(), user_balance.into()))
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

pub fn read_masm_file(path_steps: &[&str]) -> Result<String> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from_iter(
        [manifest_dir, "masm"]
            .into_iter()
            .chain(path_steps.iter().copied()),
    );
    read_to_string(&path).map_err(|e| anyhow!("Error reading MASM file at path {path:?}: {e:?}"))
}

fn n(name: &str) -> StorageSlotName {
    let name = StorageSlotName::new(name).expect("valid slot name");
    // println!("Slot name: {:?}, id: {:?}", name, name.id());
    name
}

pub fn link_pool(mut code_builder: CodeBuilder) -> Result<CodeBuilder> {
    //let mut code_builder = link_storage_utils(code_builder)?;
    let pool_code = read_masm_file(&["accounts", "pool.masm"])?;
    code_builder.link_module("zoro_miden::pool", &pool_code)?;
    Ok(code_builder)
}

pub fn link_storage_utils(code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let mut code_builder = link_math(code_builder)?;
    let storage_utils_code = read_masm_file(&["lib", "storage_utils.masm"])?;
    code_builder.link_module("zoro_miden::lib::storage_utils", &storage_utils_code)?;
    Ok(code_builder)
}

pub fn link_math(mut code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let math_code = read_masm_file(&["lib", "math.masm"])?;
    code_builder.link_module("zoro_miden::lib::math", &math_code)?;
    Ok(code_builder)
}

pub fn link_operator(mut code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let math_code = read_masm_file(&["accounts", "operator.masm"])?;
    code_builder.link_module("zoro_miden::operator", &math_code)?;
    Ok(code_builder)
}

fn map_from(entries: &[(Word, u64)]) -> StorageMap {
    let mut map = StorageMap::new();
    for (k, v) in entries {
        map.insert(
            StorageMapKey::new(*k),
            [Felt::new(*v).unwrap(), ZERO, ZERO, ZERO].into(),
        )
        .expect("insert into map");
    }
    map
}

pub fn print_contract_procedures(pool_contract: &Account) {
    println!("+++++Pool contract procedures");
    pool_contract.code().procedures().iter().for_each(|proc| {
        println!("Proc root: {:?} ", proc.mast_root().to_hex());
    });
}

pub fn print_library_exports(masm_lib: &miden_assembly::Library) {
    println!("+++++Masm lib exports:");
    masm_lib.exports().for_each(|export| {
        let path = export.path();
        if let Some(root) = masm_lib.get_procedure_root_by_path(&path) {
            println!("Export: {:?} {:?} {:?}", path, root, root.to_hex());
        } else {
            println!("Export: {:?} (no procedure root)", path);
        }
    });
}
