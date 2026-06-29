use miden_client::account::StorageSlotId;

use crate::intent::Intent;

pub struct Trade {
    pub balance_slot: StorageSlotId,
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
use miden::core::sys

  begin"#;

const TX_SCRIPT_END: &str = r#"
    exec.sys::truncate_stack
end"#;

pub fn make_exec_script(swaps: Vec<(Intent, StorageSlotId)>) -> String {
    let mut script = TX_SCRIPT_START.to_string();

    for (i, (intent, _balance_slot)) in swaps.into_iter().enumerate() {
        let Intent {
            user_suffix,
            user_prefix,
            sell_idx,
            sell_amount,
            buy_idx,
            buy_amount,
        } = intent;
        let advice_key_idx = i as u64 + 1;
        let trade_string = format!(
            r#"
    push.0.0.0.{advice_key_idx} adv.push_mapval dropw
    push.{buy_amount}.{buy_idx}.{user_prefix}.{user_suffix}.{sell_amount}.{sell_idx}.{user_prefix}.{user_suffix}
    call.execute_swap"#,
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
