pub struct Trade {
    pub user_index: u16,
    pub sell_asset_index: u64,
    pub buy_asset_index: u64,
    pub sell_amount: u64,
    pub buy_amount: u64,
}

pub struct PoolStateDelta {
    pub pool_index: u64,
    pub set_amount: u64,
}

pub fn make_exec_script(trades: Vec<Trade>) -> String {
    let mut script = r#"
use zoro_miden::pool::execute_swap
use zoro_miden::pool::set_pool_0_balance
use zoro_miden::pool::set_pool_1_balance
use miden::core::sys

begin
"#
    .to_string();

    for trade in trades {
        let Trade {
            user_index,
            sell_asset_index,
            buy_asset_index,
            sell_amount,
            buy_amount,
        } = trade;

        let sell_index = user_index as u64 * 10 + sell_asset_index;
        let buy_index = user_index as u64 * 10 + buy_asset_index;
        let trade_string = format!(
            "push.{buy_amount}.{buy_index}.{sell_amount}.{sell_index} call.execute_swap\n",
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
