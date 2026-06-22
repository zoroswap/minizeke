use anyhow::Result;
use miden_client::{
    Felt, Word,
    account::{
        Account, AccountBuilder, AccountComponent, AccountStorageMode, AccountType, StorageMap,
        StorageMapKey, StorageSlot, StorageSlotName, component::AccountComponentMetadata,
    },
    assembly::CodeBuilder,
    transaction::TransactionScript,
};

pub fn make_exec_script() -> String {
    let user: AccountId;
    let sell_asset: AccountId;
    let buy_asset: AccountId;

    let buy_key: Word = [
        user.suffix(),
        user.prefix().into(),
        buy_asset.suffix(),
        buy_asset.prefix().into(),
    ]
    .into();

    let sell_key: Word = [
        user.suffix(),
        user.prefix().into(),
        sell_asset.suffix(),
        sell_asset.prefix().into(),
    ]
    .into();

    format!(
        r#"
        use miden::protocol::active_account
        use miden::core::sys
        use zoro_miden::pool::execute_swap
        use zoro_miden::pool::update_pool_state

        begin
            push.{0}.{1}.{2}.{3}.{4}.{5}.{6}.{7}.{8}.{9} exec.execute_swap
            push.{10}.{11}.{12}.{13} exec.update_pool_state
        end
        "#,
        buy_key[3],
        buy_key[2],
        buy_key[1],
        buy_key[0],
        sell_key[3],
        sell_key[2],
        sell_key[1],
        sell_key[0],
        buy_amount,
        sell_amount,
        pool_sub,
        pull_add,
        pool_prefix,
        pool_suffix
    )
}
