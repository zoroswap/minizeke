use anyhow::{Result, anyhow};
use miden_client::{
    Client,
    account::{
        Account, AccountBuilder, AccountComponent, AccountType, StorageSlot, component::BasicWallet,
    },
    assembly::CodeBuilder,
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig},
    keystore::{FilesystemKeyStore, Keystore},
};
use miden_protocol::account::AccountComponentMetadata;
use rand::RngCore;

use crate::{
    assembly_utils::{
        link_all_libraries_for_vault, link_storage_utils, link_vault, read_masm_file,
        storage_slot_name,
    },
    miden_env::MidenNetwork,
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
        "contract id: {:?}",
        vault_contract
            .id()
            .to_bech32(MidenNetwork::from_env().endpoint().to_network_id())
    );

    Ok(vault_contract)
}

pub fn build_vault_component(cb: CodeBuilder) -> Result<AccountComponent> {
    let code = read_masm_file(&["accounts", "vault.masm"])?;
    let cb = link_all_libraries_for_vault(cb)?;
    let lib = cb.compile_component_code("zoro_miden::vault", &code)?;
    let slot_user_assets_total_funding =
        StorageSlot::with_empty_map(storage_slot_name("zorovault::user_asset_total_funding"));
    let slot_user_total_redeems =
        StorageSlot::with_empty_map(storage_slot_name("zorovault::user_total_redeems"));
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
