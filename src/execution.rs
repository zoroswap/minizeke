use miden_client::account::{AccountId, StorageSlotId};
use miden_core::{Word, field::PrimeField64};

pub struct Trade {
    pub user: StorageSlotId,
    pub sell_asset_index: u64,
    pub buy_asset_index: u64,
    pub sell_amount: u64,
    pub buy_amount: u64,
}

pub struct PoolStateDelta {
    pub pool_index: u64,
    pub set_amount: u64,
}

pub fn make_exec_script(trades: Vec<Trade>, pool_state_deltas: Vec<PoolStateDelta>) -> String {
    let mut script = r#"
use zoro_miden::pool::execute_swap
use miden::core::sys

begin
"#
    .to_string();

    for trade in trades {
        let Trade {
            user,
            sell_asset_index,
            buy_asset_index,
            sell_amount,
            buy_amount,
        } = trade;

        let user_suffix: u64 = user.suffix().as_canonical_u64();
        let user_prefix: u64 = user.prefix().as_canonical_u64();

        let trade_string = format!(
            // "push.{buy_amount}.{buy_asset_index}.{user_suffix}.{user_prefix}.{sell_amount}.{sell_asset_index}.{user_suffix}.{user_prefix} call.execute_swap\n",
            "push.{buy_amount}.{buy_asset_index}.{user_prefix}.{user_suffix}.{sell_amount}.{sell_asset_index}.{user_prefix}.{user_suffix} call.execute_swap\n",
        );

        script.push_str(&trade_string);
    }

    // for pool_state_delta in pool_state_deltas {
    //     let PoolStateDelta {
    //         pool_index,
    //         set_amount,
    //     } = pool_state_delta;

    //     let pool_state_delta_str =
    //         format!("push.{set_amount} call.set_pool_{pool_index}_balance\n");
    //     script.push_str(&pool_state_delta_str);
    // }

    script.push_str("\nexec.sys::truncate_stack\nend");
    script
}
