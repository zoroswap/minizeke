use std::{fs::read_to_string, path::PathBuf};

use anyhow::{Result, anyhow};
use miden_client::{
    Client,
    account::{
        Account, AccountBuilder, AccountComponent, AccountId, AccountStorageMode, AccountType,
        StorageSlot, StorageSlotName, component::BasicWallet,
    },
    assembly::CodeBuilder,
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig},
    keystore::FilesystemKeyStore,
    rpc::Endpoint,
};
use miden_protocol::account::AccountComponentMetadata;
use rand::RngCore;

pub fn deploy_pool(
    client: &mut Client<FilesystemKeyStore>,
    users: Vec<AccountId>,
) -> Result<Account> {
    let mut init_seed = [0_u8; 32];
    client.rng().fill_bytes(&mut init_seed);

    let key_pair = AuthSecretKey::new_falcon512_poseidon2_with_rng(client.rng());
    let pool_component = build_pool_component()?;

    let pool_contract = AccountBuilder::new(init_seed)
        .account_type(AccountType::RegularAccountUpdatableCode)
        .storage_mode(AccountStorageMode::Public)
        .with_component(pool_component.clone())
        .with_auth_component(AuthSingleSig::new(
            key_pair.public_key().to_commitment(),
            AuthScheme::Falcon512Poseidon2,
        ))
        .with_component(BasicWallet)
        .build()?;

    println!(
        "pool contract commitment hash: {:?}",
        pool_contract.to_commitment().to_hex()
    );
    println!(
        "contract id: {:?}",
        pool_contract
            .id()
            .to_bech32(Endpoint::testnet().to_network_id())
    );
    Ok(pool_contract)
}

pub fn build_pool_component() -> Result<AccountComponent> {
    let code = read_masm_file(&["masm", "pool.masm"])?;
    let cb = link_storage_utils(link_math(CodeBuilder::new())?)?;
    let lib = cb.compile_component_code("zoroswap::vault", &code)?;
    let component = AccountComponent::new(
        lib,
        vec![
            StorageSlot::with_empty_map(n("pool::pool_assets")),
            StorageSlot::with_empty_map(n("pool::user_asset_balances")),
            StorageSlot::with_empty_map(n("pool::pool_state")),
        ],
        AccountComponentMetadata::new("minizeke::pool", AccountType::all()),
    )?;

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
    StorageSlotName::new(name).expect("valid slot name")
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
