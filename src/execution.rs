use miden_client::account::AccountId;
use miden_core::{Word, field::PrimeField64};

pub struct Trade {
    pub user: AccountId,
    pub sell_asset: AccountId,
    pub buy_asset: AccountId,
    pub sell_amount: u64,
    pub buy_amount: u64,
}

pub struct PoolStateDelta {
    pub asset: AccountId,
    pub add_amount: u64,
    pub sub_amount: u64,
}

pub fn make_exec_script(trades: Vec<Trade>, pool_state_deltas: Vec<PoolStateDelta>) -> String {
    let mut script = r#"
        use miden::protocol::active_account
        use miden::core::sys
        use zoro_miden::pool::execute_swap
        use zoro_miden::pool::update_pool_state
        "#
    .to_string();

    for trade in trades {
        let Trade {
            user,
            sell_asset,
            buy_asset,
            sell_amount,
            buy_amount,
        } = trade;

        let user_suffix: u64 = user.suffix().as_canonical_u64();
        let user_prefix: u64 = user.prefix().into();
        let buy_asset_suffix: u64 = buy_asset.suffix().as_canonical_u64();
        let buy_asset_prefix: u64 = buy_asset.prefix().into();
        let sell_asset_suffix: u64 = sell_asset.suffix().as_canonical_u64();
        let sell_asset_prefix: u64 = sell_asset.prefix().into();
        let trade_string = format!(
            "push.{buy_asset_prefix}.{buy_asset_suffix}.{user_prefix}.{user_suffix}.{sell_asset_prefix}.{sell_asset_suffix}.{user_prefix}.{user_suffix}.{buy_amount}.{sell_amount} exec.execute_swap\n",
        );

        script.push_str(&trade_string);
    }

    for pool_state_delta in pool_state_deltas {
        let PoolStateDelta {
            asset,
            add_amount,
            sub_amount,
        } = pool_state_delta;
        let suffix: u64 = asset.suffix().as_canonical_u64();
        let prefix: u64 = asset.prefix().into();
        let pool_state_delta_str =
            format!("push.{prefix}.{suffix}.{add_amount}.{sub_amount} exec.update_pool_state\n");
        script.push_str(&pool_state_delta_str);
    }

    script.push_str("\nend");
    script
}
