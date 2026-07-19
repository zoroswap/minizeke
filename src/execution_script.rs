use miden_client::account::{AccountId, StorageSlotId};

use crate::intent::Intent;

pub struct Trade {
    pub user: StorageSlotId,
    pub sell_asset: AccountId,
    pub buy_asset: AccountId,
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
    const ADV_MAP_KEY = [0, 0, 0, 1]

  begin
    push.ADV_MAP_KEY adv.push_mapval dropw

"#;

const TX_SCRIPT_END: &str = r#"
    exec.sys::truncate_stack
end"#;

pub fn make_exec_script(intents: Vec<Intent>) -> String {
    let mut script = TX_SCRIPT_START.to_string();

    for intent in intents {
        let Intent {
            purpose,
            domain,
            network,
            user_suffix,
            user_prefix,
            sell_asset_suffix,
            sell_asset_prefix,
            sell_amount,
            buy_asset_suffix,
            buy_asset_prefix,
            buy_amount,
            client_order_id,
            expires_at,
        } = intent;
        let [uuid0, uuid1, uuid2, uuid3] = client_order_id;

        let trade_string = format!(
            r#"
       push.{expires_at}.{uuid3}.{uuid2}.{uuid1}.{uuid0}.{buy_amount}.{buy_asset_prefix}.{buy_asset_suffix}.{sell_amount}.{sell_asset_prefix}.{sell_asset_suffix}.{user_prefix}.{user_suffix}.{network}.{domain}.{purpose}
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
