use anyhow::Result;
use miden_client::{
    Client,
    account::{Account, AccountBuilder, AccountId, AccountType, component::BasicWallet},
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig},
    keystore::FilesystemKeyStore,
};
use miden_core::Word;
use rand::RngCore;

use crate::{miden_execution::user_id_word, user::User};

pub async fn deploy_vault(
    client: &mut Client<FilesystemKeyStore>,
    users: Vec<User>,
    pool_0_balance: u64,
    pool_1_balance: u64,
) -> Result<Account> {
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
            .to_bech32(MidenNetwork::from_env().endpoint().to_network_id())
    );

    Ok((pool_contract, pool_component))
}
