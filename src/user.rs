use anyhow::Result;
use miden_client::{
    Client,
    account::{AccountBuilder, AccountId, AccountStorageMode, AccountType, component::BasicWallet},
    keystore::FilesystemKeyStore,
};
use rand::RngCore;

pub async fn get_users(n: u32, client: &mut Client<FilesystemKeyStore>) -> Result<Vec<AccountId>> {
    let mut init_seed = [0_u8; 32];
    client.rng().fill_bytes(&mut init_seed);
    let mut users = Vec::with_capacity(n as usize);

    for _ in 0..n {
        println!("Deploying user {n}");
        let builder = AccountBuilder::new(init_seed)
            .account_type(AccountType::RegularAccountUpdatableCode)
            .storage_mode(AccountStorageMode::Public)
            .with_component(BasicWallet);
        let account = builder.build()?;
        users.push(account.id());
    }
    Ok(users)
}
