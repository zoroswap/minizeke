use std::{fs::read_to_string, path::PathBuf};

use anyhow::{Result, anyhow};
use miden_client::{
    Client,
    account::{
        Account, AccountBuilder, AccountComponent, AccountId, AccountStorageMode, AccountType,
        StorageMap, StorageMapKey, StorageSlot, StorageSlotName, component::BasicWallet,
    },
    assembly::CodeBuilder,
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig},
    keystore::{FilesystemKeyStore, Keystore},
    rpc::Endpoint,
    testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    },
};
use miden_core::{Felt, Word, ZERO};
use miden_protocol::account::AccountComponentMetadata;
use rand::RngCore;

pub async fn deploy_pool(
    client: &mut Client<FilesystemKeyStore>,
    users: Vec<AccountId>,
) -> Result<(Account, AccountComponent)> {
    let mut init_seed = [0_u8; 32];
    client.rng().fill_bytes(&mut init_seed);

    let key_pair = AuthSecretKey::new_falcon512_poseidon2_with_rng(client.rng());
    let pool_component = build_pool_component(users, client.code_builder())?;

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
            .to_bech32(Endpoint::testnet().to_network_id())
    );
    Ok((pool_contract, pool_component))
}

pub fn build_pool_component(users: Vec<AccountId>, cb: CodeBuilder) -> Result<AccountComponent> {
    let code = read_masm_file(&["accounts", "pool.masm"])?;
    let cb = link_storage_utils(cb)?;
    let lib = cb.compile_component_code("zoro_miden::pool", &code)?;

    let asset0 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
    let asset1 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2)?;
    let faucets = [asset0, asset1];

    let amount_0 = 10_000_000;
    let amount_1 = 10_000_000;
    let user_amount = 1_000;

    let pool_balances_key_0: Word = [
        Felt::new(0),
        Felt::new(0),
        asset0.suffix(),
        asset0.prefix().into(),
    ]
    .into();
    let pool_balances_key_1: Word = [
        Felt::new(0),
        Felt::new(0),
        asset1.suffix(),
        asset1.prefix().into(),
    ]
    .into();
    let pool_balances = [
        (pool_balances_key_0, amount_0),
        (pool_balances_key_1, amount_1),
    ];

    let mut user_balances: Vec<(Word, u64)> = Vec::with_capacity(users.len());
    for user in users {
        for faucet in faucets {
            user_balances.push((
                [
                    user.suffix(),
                    user.prefix().into(),
                    faucet.suffix(),
                    faucet.prefix().into(),
                ]
                .into(),
                user_amount,
            ));
        }
    }

    let component = AccountComponent::new(
        lib,
        vec![
            StorageSlot::with_map(n("pool::pool_assets"), map_from(&pool_balances)),
            StorageSlot::with_map(n("pool::user_asset_balances"), map_from(&user_balances)),
            StorageSlot::with_map(n("pool::pool_state"), map_from(&pool_balances)),
        ],
        AccountComponentMetadata::new("zoro_miden::pool", AccountType::all()),
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

fn map_from(entries: &[(Word, u64)]) -> StorageMap {
    let mut map = StorageMap::new();
    for (k, v) in entries {
        map.insert(
            StorageMapKey::new(*k),
            [Felt::new(*v), ZERO, ZERO, ZERO].into(),
        )
        .expect("insert into map");
    }
    map
}
