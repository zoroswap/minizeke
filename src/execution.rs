use miden_client::account::AccountId;
use miden_core::{Word, field::PrimeField64};

pub struct Trade {
    pub user_index: u64,
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
use zoro_miden::pool
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

        let sell_index = user_index * 10 + sell_asset_index;
        let buy_index = user_index * 10 + buy_asset_index;
        let sell_string: String =
            format!("push.{sell_amount} call.pool::sub_from_user{sell_index}_balance\n",);
        let buy_string: String =
            format!("push.{buy_amount} call.pool::add_to_user{buy_index}_balance\n",);

        script.push_str(&sell_string);
        script.push_str(&buy_string);
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
