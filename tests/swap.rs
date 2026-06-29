use anyhow::Result;
use minizeke::{
    intent::Intent,
    order::{Order, OrderDetails, OrderExecutionResult},
    test_utils::{get_asset0, get_asset1, get_miden_execution},
};

#[tokio::test]
async fn test_swap() -> Result<()> {
    tracing_subscriber::fmt().init();

    let mut miden_execution = get_miden_execution().await?;
    let users = miden_execution.users();
    let users_by_id = users.by_account_id();
    let mut orders = Vec::with_capacity(users_by_id.len());
    let asset0 = get_asset0();
    let asset1 = get_asset1();

    for (user_id, user) in users.by_account_id() {
        let user_suffix: u64 = user_id.suffix().as_canonical_u64();
        let user_prefix: u64 = user_id.prefix().as_u64();

        let intent = Intent {
            user_suffix,
            user_prefix,
            sell_idx: 0,
            buy_idx: 1,
            sell_amount: 10,
            buy_amount: 10,
        };

        let msg_word = intent.message_word();
        let signature = user.sign(msg_word);

        let order = Order::new(
            signature,
            user_id,
            OrderDetails {
                asset_in: asset0,
                amount_in: 10,
                asset_out: asset1,
                min_amount_out: 10,
            },
            user.pubkey(),
        );

        let order = order.start_processing();
        let order = order.processed(OrderExecutionResult { amount_out: 10 });
        orders.push(order);
    }

    miden_execution.handle_batch(orders).await;

    Ok(())
}
