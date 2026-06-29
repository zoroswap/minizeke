use miden_client::account::{AccountId, StorageSlotId};

use crate::intent::Intent;

pub struct Trade {
    pub user: StorageSlotId,
    pub sell_asset: AccountId,
    pub buy_asset: AccountId,
    pub sell_asset_index: u64,
    pub buy_asset_index: u64,
    pub sell_amount: u64,
    pub buy_amount: u64,
    pub intent: Intent,
}

pub struct PoolStateDelta {
    pub pool_index: u64,
    pub set_amount: u64,
}

const TX_SCRIPT_START: &str = r#"
use zoro_miden::pool::execute_swap
use operator::verify_signature
use miden::core::sys

  begin"#;

const TX_SCRIPT_END: &str = r#"
    exec.sys::truncate_stack
end"#;

pub fn make_exec_script(intents: Vec<Intent>) -> String {
    let mut script = TX_SCRIPT_START.to_string();

    for intent in intents {
        let Intent {
            user_suffix,
            user_prefix,
            sell_idx,
            sell_amount,
            buy_idx,
            buy_amount,
        } = intent;
        let trade_string = format!(
            // "push.{buy_amount}.{buy_asset_index}.{user_suffix}.{user_prefix}.{sell_amount}.{sell_asset_index}.{user_suffix}.{user_prefix} call.execute_swap\n",

            // sell_amount, buy_amount, user_suf, user_pre, sell_suf, sell_pre, buy_suf, buy_pre
            //
            r#"
    push.{buy_amount}.{buy_idx}.{user_prefix}.{user_suffix}.{sell_amount}.{sell_idx}.{user_prefix}.{user_suffix} call.execute_swap"#,
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

    script.push_str(TX_SCRIPT_END);
    script
}
