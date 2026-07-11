use alloy_primitives::U256;
use anyhow::Result;
use miden_client::{
    Client,
    account::AccountId,
    asset::FungibleAsset,
    auth::AuthSecretKey,
    keystore::FilesystemKeyStore,
    testing::common::wait_for_blocks,
    transaction::TransactionRequestBuilder,
};
use miden_core::{Felt, Word};
use minizeke::{
    assembly_utils::{link_math, link_operator, link_pool},
    curve::get_curve_amount_out,
    execution_script::make_exec_script,
    intent::Intent,
    note::{InitRedeemInstructions, RedeemInstructions, ZekeNote, ZekeNoteInstructions},
    pool::{
        PoolState, deploy_pool, derive_balance_details, get_user_trades_slot_name,
        user_trades_from_storage,
    },
    test_utils::{
        consume_all_notes_for, deposit_liquidity_on_vault, fund_user_on_vault, get_client,
        get_faucet, get_funded_user, get_pool_client, get_user, get_vault, mint_asset_to_user,
        pool_foreign_account, register_user_on_vault, vault_foreign_account,
        withdraw_liquidity_from_vault,
    },
    vault::{
        checkpoint_lp_entitlement_on_vault, get_vault_lp_info, get_vault_storage,
        get_vault_user_asset_info, set_pool_account_id_on_vault, vault_user_registration,
    },
};
use tracing::info;

const FUND_AMOUNT: u64 = 1_000;

/// Builds and submits a pool-native swap transaction for the given signed intents,
/// declaring the vault as a foreign account for the per-trader FPI.
///
/// Must be called on the pool client (see [`get_pool_client`]): the vault must NOT be
/// tracked in the submitting client's store, otherwise the foreign-account vault fetch
/// takes the `IfChangedFrom` fast path and reconstructs an empty vault, tripping the
/// kernel's commitment check once the vault holds assets.
async fn submit_swap(
    client: &mut Client<FilesystemKeyStore>,
    pool_id: AccountId,
    vault_id: AccountId,
    intents: Vec<Intent>,
    advice_data: Vec<Felt>,
    fpi_asset_user_pairs: &[(AccountId, AccountId)],
) -> Result<()> {
    // refresh the anchor block so the foreign-account proof covers the latest vault state
    client.sync_state().await?;

    let script = make_exec_script(intents);
    let cb = link_math(client.code_builder())?;
    let cb = link_operator(cb)?;
    let cb = link_pool(cb)?;
    let tx_script = cb.compile_tx_script(script)?;

    let advice_map_key = Word::from([Felt::ZERO, Felt::ZERO, Felt::ZERO, Felt::ONE]);
    let tx_req = TransactionRequestBuilder::new()
        .custom_script(tx_script)
        .extend_advice_map([(advice_map_key, advice_data)])
        .foreign_accounts(vec![vault_foreign_account(vault_id, fpi_asset_user_pairs)?])
        .build()?;

    client.submit_new_transaction(pool_id, tx_req).await?;
    client.sync_state().await?;
    wait_for_blocks(client, 1).await;
    Ok(())
}

fn signed_intent(
    user_id: AccountId,
    trading_key: &AuthSecretKey,
    user_index: u16,
    sell_idx: u64,
    sell_amount: u64,
    buy_idx: u64,
    buy_amount: u64,
) -> (Intent, Vec<Felt>) {
    let user_key_slot = get_user_trades_slot_name(user_index);
    let intent = Intent {
        user_suffix: user_id.suffix().as_canonical_u64(),
        user_prefix: user_id.prefix().as_u64(),
        user_key_prefix: user_key_slot.id().prefix().as_canonical_u64(),
        user_key_suffix: user_key_slot.id().suffix().as_canonical_u64(),
        sell_idx,
        buy_idx,
        sell_amount,
        buy_amount,
    };
    let msg = intent.message_word();
    let signature = trading_key.sign(msg);
    let prepared = signature.to_prepared_signature(msg); // [PK[9], SIG[17]]
    (intent, prepared)
}

/// Probe: for every tracked account with a non-empty vault, compare the node's response
/// for `VaultFetch::Always` vs `VaultFetch::IfChangedFrom(local_root)` — checking whether
/// the "unchanged" fast path returns an empty asset list that breaks the foreign-account
/// commitment reconstruction.
#[tokio::test]
#[ignore]
async fn probe_vault_fetch() -> Result<()> {
    use miden_client::rpc::{
        AccountStateAt,
        domain::account::{GetAccountRequest, StorageMapFetch, VaultFetch},
    };

    tracing_subscriber::fmt().init();
    let mut client = get_client().await?;
    client.sync_state().await?;
    let sync_height = client.get_sync_height().await?;

    let headers = client.get_account_headers().await?;
    for (header, _) in headers {
        let id = header.id();
        if !id.is_public() {
            continue;
        }
        let local_vault_root = header.vault_root();

        let (block_a, proof_always) = client
            .test_rpc_api()
            .get_account(
                id,
                GetAccountRequest::new()
                    .with_storage(StorageMapFetch::All)
                    .at(AccountStateAt::Block(sync_height))
                    .with_vault(VaultFetch::Always),
            )
            .await?;
        let (block_b, proof_ifchanged) = client
            .test_rpc_api()
            .get_account(
                id,
                GetAccountRequest::new()
                    .with_storage(StorageMapFetch::All)
                    .at(AccountStateAt::Block(sync_height))
                    .with_vault(VaultFetch::IfChangedFrom(local_vault_root)),
            )
            .await?;

        let always_assets = proof_always.vault_details().map(|v| v.assets.len());
        let ifchanged_assets = proof_ifchanged.vault_details().map(|v| v.assets.len());
        let remote_vault_root = proof_always
            .account_header()
            .map(|h| h.vault_root().to_hex())
            .unwrap_or_else(|| "n/a".into());
        let local_commitment = header.to_commitment().to_hex();
        let witness_commitment = proof_always.account_commitment().to_hex();
        info!(
            account = id.to_hex(),
            local_vault_root = local_vault_root.to_hex(),
            remote_vault_root,
            local_commitment,
            witness_commitment,
            ?block_a,
            ?block_b,
            ?always_assets,
            ?ifchanged_assets,
            "vault fetch comparison"
        );
    }
    Ok(())
}

/// Minimal repro: deploy, register, fund, swap. No negative cases in between.
/// Used to bisect the foreign-account commitment mismatch seen in the full E2E.
#[tokio::test]
#[ignore]
async fn test_swap_minimal() -> Result<()> {
    tracing_subscriber::fmt().init();
    let mut client = get_client().await?;
    let mut pool_client = get_pool_client().await?;

    let vault_id = get_vault(&mut client).await?;
    let (user_id, asset0) = get_funded_user(&mut client).await?;
    let asset1 = get_faucet(&mut client, "TSTB").await?;
    let pool = deploy_pool(&mut pool_client, vault_id, asset0, asset1).await?;
    let pool_id = pool.id();
    set_pool_account_id_on_vault(&mut client, vault_id, pool_id).await?;

    let trading_key = AuthSecretKey::new_ecdsa_k256_keccak();
    let pk_comm: Word = trading_key.public_key().to_commitment().into();
    register_user_on_vault(&mut client, vault_id, user_id, pk_comm).await?;

    fund_user_on_vault(
        &mut client,
        vault_id,
        user_id,
        FungibleAsset::new(asset0, FUND_AMOUNT)?,
    )
    .await?;

    let (intent, advice) = signed_intent(user_id, &trading_key, 0, 0, 10, 1, 7);
    submit_swap(
        &mut pool_client,
        pool_id,
        vault_id,
        vec![intent],
        advice,
        &[(asset0, user_id)],
    )
    .await?;

    let pool_account = pool_client.try_get_account(pool_id).await?;
    let trades = user_trades_from_storage(pool_account.storage(), 0)?;
    assert_eq!(trades.sold, [10, 0]);
    assert_eq!(trades.bought, [0, 7]);
    Ok(())
}

#[tokio::test]
async fn test_vault_pool_e2e() -> Result<()> {
    tracing_subscriber::fmt().init();
    let mut client = get_client().await?;
    // swaps are submitted from a separate client that does not track the vault
    // (see `submit_swap` / `get_pool_client` docs)
    let mut pool_client = get_pool_client().await?;

    // ------------------------------------------------------------------------------------------
    // SETUP: vault, assets, pool, cross-wiring
    // ------------------------------------------------------------------------------------------
    info!("[TEST] deploying vault");
    let vault_id = get_vault(&mut client).await?;

    info!("[TEST] creating funded user (asset0 faucet)");
    let (user_id, asset0) = get_funded_user(&mut client).await?;
    let asset1 = get_faucet(&mut client, "TSTB").await?;

    info!("[TEST] deploying pool");
    let pool = deploy_pool(&mut pool_client, vault_id, asset0, asset1).await?;
    let pool_id = pool.id();
    set_pool_account_id_on_vault(&mut client, vault_id, pool_id).await?;

    // ------------------------------------------------------------------------------------------
    // REGISTER
    // ------------------------------------------------------------------------------------------
    info!("[TEST] registering user");
    let trading_key = AuthSecretKey::new_ecdsa_k256_keccak();
    let pk_comm: Word = trading_key.public_key().to_commitment().into();
    register_user_on_vault(&mut client, vault_id, user_id, pk_comm).await?;

    let vault_storage = get_vault_storage(&client, vault_id).await?;
    let (user_index, registered_pk) =
        vault_user_registration(&vault_storage, user_id)?.expect("user should be registered");
    assert_eq!(user_index, 0);
    assert_eq!(registered_pk, pk_comm);

    // double registration must fail
    info!("[TEST] negative: double registration");
    let result = register_user_on_vault(&mut client, vault_id, user_id, pk_comm).await;
    assert!(result.is_err(), "double registration should fail");

    // ------------------------------------------------------------------------------------------
    // FUND
    // ------------------------------------------------------------------------------------------
    info!("[TEST] funding user with {FUND_AMOUNT} of asset0");
    let user_wallet_before_fund = client.account_reader(user_id).get_balance(asset0).await?;
    fund_user_on_vault(
        &mut client,
        vault_id,
        user_id,
        FungibleAsset::new(asset0, FUND_AMOUNT)?,
    )
    .await?;

    let vault_info = get_vault_user_asset_info(&client, vault_id, asset0, user_id).await?;
    assert_eq!(vault_info.total_funding, FUND_AMOUNT);

    // ------------------------------------------------------------------------------------------
    // SWAP: sell 10 asset0 for 7 asset1
    // ------------------------------------------------------------------------------------------
    info!("[TEST] executing swap batch");
    let (intent, advice) = signed_intent(user_id, &trading_key, 0, 0, 10, 1, 7);
    submit_swap(
        &mut pool_client,
        pool_id,
        vault_id,
        vec![intent],
        advice,
        &[(asset0, user_id)],
    )
    .await?;

    let pool_account = pool_client.try_get_account(pool_id).await?;
    let trades = user_trades_from_storage(pool_account.storage(), 0)?;
    assert_eq!(trades.sold, [10, 0]);
    assert_eq!(trades.bought, [0, 7]);

    let vault_info = get_vault_user_asset_info(&client, vault_id, asset0, user_id).await?;
    let (balance0, available0) = derive_balance_details(&vault_info, trades.bought[0], trades.sold[0]);
    assert_eq!(balance0, FUND_AMOUNT - 10);
    assert_eq!(available0, FUND_AMOUNT - 10);

    // ------------------------------------------------------------------------------------------
    // NEGATIVE: swap above available
    // ------------------------------------------------------------------------------------------
    info!("[TEST] negative: swap above available");
    let (intent, advice) = signed_intent(user_id, &trading_key, 0, 0, FUND_AMOUNT * 10, 1, 1);
    let result = submit_swap(
        &mut pool_client,
        pool_id,
        vault_id,
        vec![intent],
        advice,
        &[(asset0, user_id)],
    )
    .await;
    assert!(result.is_err(), "swap above available should fail");

    // ------------------------------------------------------------------------------------------
    // NEGATIVE: unregistered user swap
    // ------------------------------------------------------------------------------------------
    info!("[TEST] negative: unregistered user swap");
    let stranger_id = get_user(&mut client).await?;
    let stranger_key = AuthSecretKey::new_ecdsa_k256_keccak();
    let (intent, advice) = signed_intent(stranger_id, &stranger_key, 1, 0, 1, 1, 1);
    let result = submit_swap(
        &mut pool_client,
        pool_id,
        vault_id,
        vec![intent],
        advice,
        &[(asset0, stranger_id)],
    )
    .await;
    assert!(result.is_err(), "unregistered user swap should fail");

    // ------------------------------------------------------------------------------------------
    // NEGATIVE: init_redeem above available
    // ------------------------------------------------------------------------------------------
    info!("[TEST] negative: init_redeem above available");
    let over_redeem_note = ZekeNote::new(
        ZekeNoteInstructions::InitRedeem(InitRedeemInstructions {
            user_id,
            vault_id,
            min_expected_asset: FungibleAsset::new(asset0, FUND_AMOUNT * 10)?,
        }),
        client.code_builder(),
    )?;
    let tx_req = TransactionRequestBuilder::new()
        .own_output_notes(vec![over_redeem_note.note().clone()])
        .build()?;
    client.submit_new_transaction(user_id, tx_req).await?;
    client.sync_state().await?;
    wait_for_blocks(&mut client, 2).await;

    let tx_req = TransactionRequestBuilder::new()
        .input_notes(vec![(over_redeem_note.note().clone(), None)])
        .foreign_accounts(vec![pool_foreign_account(pool_id, 0)?])
        .build()?;
    let result = client.submit_new_transaction(vault_id, tx_req).await;
    assert!(result.is_err(), "init_redeem above available should fail");

    // ------------------------------------------------------------------------------------------
    // INIT_REDEEM the full available amount
    // ------------------------------------------------------------------------------------------
    let redeem_amount = FUND_AMOUNT - 10;
    info!("[TEST] initiating redeem of {redeem_amount}");
    let init_redeem_note = ZekeNote::new(
        ZekeNoteInstructions::InitRedeem(InitRedeemInstructions {
            user_id,
            vault_id,
            min_expected_asset: FungibleAsset::new(asset0, redeem_amount)?,
        }),
        client.code_builder(),
    )?;
    let tx_req = TransactionRequestBuilder::new()
        .own_output_notes(vec![init_redeem_note.note().clone()])
        .build()?;
    client.submit_new_transaction(user_id, tx_req).await?;
    client.sync_state().await?;
    wait_for_blocks(&mut client, 2).await;

    let tx_req = TransactionRequestBuilder::new()
        .input_notes(vec![(init_redeem_note.note().clone(), None)])
        .foreign_accounts(vec![pool_foreign_account(pool_id, 0)?])
        .build()?;
    client.submit_new_transaction(vault_id, tx_req).await?;
    client.sync_state().await?;
    wait_for_blocks(&mut client, 1).await;

    let vault_info = get_vault_user_asset_info(&client, vault_id, asset0, user_id).await?;
    assert_eq!(vault_info.total_initiated_redeems, redeem_amount);
    assert_eq!(vault_info.pending_redeem(), redeem_amount);

    // pending funds are locked: available is now 0, so even a 1-token swap must fail
    info!("[TEST] negative: swap with pending redeem locking the balance");
    let (intent, advice) = signed_intent(user_id, &trading_key, 0, 0, 1, 1, 1);
    let result = submit_swap(
        &mut pool_client,
        pool_id,
        vault_id,
        vec![intent],
        advice,
        &[(asset0, user_id)],
    )
    .await;
    assert!(result.is_err(), "swap while balance is pending-locked should fail");

    // ------------------------------------------------------------------------------------------
    // REDEEM
    // ------------------------------------------------------------------------------------------
    info!("[TEST] redeeming {redeem_amount}");
    let redeem_note = ZekeNote::new(
        ZekeNoteInstructions::Redeem(RedeemInstructions {
            user_id,
            vault_id,
            min_expected_asset: FungibleAsset::new(asset0, redeem_amount)?,
        }),
        client.code_builder(),
    )?;
    let tx_req = TransactionRequestBuilder::new()
        .own_output_notes(vec![redeem_note.note().clone()])
        .build()?;
    client.submit_new_transaction(user_id, tx_req).await?;
    client.sync_state().await?;
    wait_for_blocks(&mut client, 2).await;

    let tx_req = TransactionRequestBuilder::new()
        .input_notes(vec![(redeem_note.note().clone(), None)])
        .foreign_accounts(vec![pool_foreign_account(pool_id, 0)?])
        .build()?;
    client.submit_new_transaction(vault_id, tx_req).await?;
    client.sync_state().await?;
    wait_for_blocks(&mut client, 1).await;

    let vault_info = get_vault_user_asset_info(&client, vault_id, asset0, user_id).await?;
    assert_eq!(vault_info.total_redeems, redeem_amount);
    assert_eq!(vault_info.pending_redeem(), 0);

    // ------------------------------------------------------------------------------------------
    // NEGATIVE: redeem above pending
    // ------------------------------------------------------------------------------------------
    info!("[TEST] negative: redeem above pending");
    let over_redeem_note = ZekeNote::new(
        ZekeNoteInstructions::Redeem(RedeemInstructions {
            user_id,
            vault_id,
            min_expected_asset: FungibleAsset::new(asset0, 5)?,
        }),
        client.code_builder(),
    )?;
    let tx_req = TransactionRequestBuilder::new()
        .own_output_notes(vec![over_redeem_note.note().clone()])
        .build()?;
    client.submit_new_transaction(user_id, tx_req).await?;
    client.sync_state().await?;
    wait_for_blocks(&mut client, 2).await;

    let tx_req = TransactionRequestBuilder::new()
        .input_notes(vec![(over_redeem_note.note().clone(), None)])
        .foreign_accounts(vec![pool_foreign_account(pool_id, 0)?])
        .build()?;
    let result = client.submit_new_transaction(vault_id, tx_req).await;
    assert!(result.is_err(), "redeem above pending should fail");

    // ------------------------------------------------------------------------------------------
    // FINAL: the user consumes the P2ID payout
    // ------------------------------------------------------------------------------------------
    info!("[TEST] consuming P2ID payout");
    consume_all_notes_for(&mut client, user_id).await?;

    let user_wallet = client.account_reader(user_id).get_balance(asset0).await?;
    assert_eq!(
        user_wallet,
        user_wallet_before_fund - FUND_AMOUNT + redeem_amount
    );

    Ok(())
}

/// E2E for the LP liquidity flow: deposit (entitlement credited), operator checkpoint
/// (fees become withdrawable), self-custodial withdraw with P2ID payout, plus the
/// withdraw-above-entitlement and decreasing-checkpoint negative cases. Finishes with a
/// swap whose buy amount comes from the server-side curve quote.
#[tokio::test]
async fn test_lp_deposit_withdraw_e2e() -> Result<()> {
    // try_init: test_vault_pool_e2e may have already installed the subscriber
    let _ = tracing_subscriber::fmt().try_init();
    let mut client = get_client().await?;
    let mut pool_client = get_pool_client().await?;

    const DEPOSIT_AMOUNT: u64 = 100_000;
    const ACCRUED_FEES: u64 = 250;

    // ------------------------------------------------------------------------------------------
    // SETUP: vault, assets, pool, LP account
    // ------------------------------------------------------------------------------------------
    info!("[TEST] deploying vault + faucets + pool");
    let vault_id = get_vault(&mut client).await?;
    let (lp_id, asset0) = get_funded_user(&mut client).await?;
    let asset1 = get_faucet(&mut client, "TSTB").await?;
    let pool = deploy_pool(&mut pool_client, vault_id, asset0, asset1).await?;
    let pool_id = pool.id();
    set_pool_account_id_on_vault(&mut client, vault_id, pool_id).await?;

    mint_asset_to_user(&mut client, asset0, lp_id, DEPOSIT_AMOUNT).await?;
    mint_asset_to_user(&mut client, asset1, lp_id, DEPOSIT_AMOUNT).await?;
    let lp_wallet_start = client.account_reader(lp_id).get_balance(asset0).await?;

    // server-side pool states start empty; deposits initialize them via the curve math
    let mut pool_state0 = PoolState::default();
    let mut pool_state1 = PoolState::default();

    // ------------------------------------------------------------------------------------------
    // DEPOSIT liquidity in both assets
    // ------------------------------------------------------------------------------------------
    info!("[TEST] depositing {DEPOSIT_AMOUNT} of each asset");
    for (asset_id, pool_state) in [(asset0, &mut pool_state0), (asset1, &mut pool_state1)] {
        deposit_liquidity_on_vault(
            &mut client,
            vault_id,
            lp_id,
            FungibleAsset::new(asset_id, DEPOSIT_AMOUNT)?,
        )
        .await?;
        let (lp_shares, new_supply, new_balances) =
            pool_state.get_deposit_lp_amount_out(U256::from(DEPOSIT_AMOUNT))?;
        pool_state.update_state(new_balances, new_supply);
        // first deposit into an empty pool mints shares 1:1
        assert_eq!(lp_shares, U256::from(DEPOSIT_AMOUNT));
    }

    let lp_info = get_vault_lp_info(&client, vault_id, asset0, lp_id).await?;
    assert_eq!(lp_info.entitlement, DEPOSIT_AMOUNT);
    assert_eq!(lp_info.withdrawn, 0);
    assert_eq!(lp_info.withdrawable(), DEPOSIT_AMOUNT);

    let vault_balance = client.account_reader(vault_id).get_balance(asset0).await?;
    assert_eq!(vault_balance, DEPOSIT_AMOUNT);

    // ------------------------------------------------------------------------------------------
    // NEGATIVE: withdraw above entitlement
    // ------------------------------------------------------------------------------------------
    info!("[TEST] negative: withdraw above entitlement");
    let result = withdraw_liquidity_from_vault(
        &mut client,
        vault_id,
        lp_id,
        FungibleAsset::new(asset0, DEPOSIT_AMOUNT + 1)?,
    )
    .await;
    assert!(result.is_err(), "withdraw above entitlement should fail");

    // ------------------------------------------------------------------------------------------
    // WITHDRAW part of the principal (self-custodial: no operator involvement)
    // ------------------------------------------------------------------------------------------
    let first_withdraw = DEPOSIT_AMOUNT / 2;
    info!("[TEST] withdrawing {first_withdraw}");
    withdraw_liquidity_from_vault(
        &mut client,
        vault_id,
        lp_id,
        FungibleAsset::new(asset0, first_withdraw)?,
    )
    .await?;

    let lp_info = get_vault_lp_info(&client, vault_id, asset0, lp_id).await?;
    assert_eq!(lp_info.withdrawn, first_withdraw);
    assert_eq!(lp_info.withdrawable(), DEPOSIT_AMOUNT - first_withdraw);

    info!("[TEST] consuming P2ID payout");
    consume_all_notes_for(&mut client, lp_id).await?;
    let lp_wallet = client.account_reader(lp_id).get_balance(asset0).await?;
    assert_eq!(lp_wallet, lp_wallet_start - DEPOSIT_AMOUNT + first_withdraw);

    // ------------------------------------------------------------------------------------------
    // CHECKPOINT: operator raises the entitlement with accrued fees
    // ------------------------------------------------------------------------------------------
    info!("[TEST] negative: checkpoint below current entitlement");
    let result = checkpoint_lp_entitlement_on_vault(
        &mut client,
        vault_id,
        asset0,
        lp_id,
        DEPOSIT_AMOUNT - 1,
    )
    .await;
    assert!(
        result.is_err(),
        "decreasing entitlement checkpoint should fail"
    );

    info!("[TEST] checkpointing entitlement with {ACCRUED_FEES} accrued fees");
    checkpoint_lp_entitlement_on_vault(
        &mut client,
        vault_id,
        asset0,
        lp_id,
        DEPOSIT_AMOUNT + ACCRUED_FEES,
    )
    .await?;

    let lp_info = get_vault_lp_info(&client, vault_id, asset0, lp_id).await?;
    assert_eq!(lp_info.entitlement, DEPOSIT_AMOUNT + ACCRUED_FEES);
    assert_eq!(
        lp_info.withdrawable(),
        DEPOSIT_AMOUNT + ACCRUED_FEES - first_withdraw
    );

    // ------------------------------------------------------------------------------------------
    // SWAP: quoted by the server-side curve against the deposited pool states
    // ------------------------------------------------------------------------------------------
    info!("[TEST] registering + funding trader");
    let trader_id = get_user(&mut client).await?;
    let trading_key = AuthSecretKey::new_ecdsa_k256_keccak();
    let pk_comm: Word = trading_key.public_key().to_commitment().into();
    register_user_on_vault(&mut client, vault_id, trader_id, pk_comm).await?;
    mint_asset_to_user(&mut client, asset0, trader_id, FUND_AMOUNT).await?;
    fund_user_on_vault(
        &mut client,
        vault_id,
        trader_id,
        FungibleAsset::new(asset0, FUND_AMOUNT)?,
    )
    .await?;

    let swap_amount_in: u64 = 100;
    // pair price 1.0, scaled by 1e12 like PriceData::quote_with
    let price = U256::from(10).pow(U256::from(12));
    let (amount_out, new_balances0, new_balances1) = get_curve_amount_out(
        &pool_state0,
        &pool_state1,
        U256::from(pool_state0.metadata().asset_decimals),
        U256::from(pool_state1.metadata().asset_decimals),
        U256::from(swap_amount_in),
        price,
    )?;
    let amount_out_u64 = amount_out.saturating_to::<u64>();
    info!("[TEST] curve quote: {swap_amount_in} asset0 -> {amount_out_u64} asset1");
    assert!(amount_out_u64 > 0, "curve quote should be non-zero");
    assert!(
        amount_out_u64 <= swap_amount_in,
        "balanced pools at price 1.0 cannot pay out more than put in"
    );
    pool_state0.update_balances(new_balances0);
    pool_state1.update_balances(new_balances1);

    info!("[TEST] executing curve-quoted swap");
    // the LP never registers (deposits don't need it), so the trader gets index 0
    let (intent, advice) = signed_intent(
        trader_id,
        &trading_key,
        0,
        0,
        swap_amount_in,
        1,
        amount_out_u64,
    );
    submit_swap(
        &mut pool_client,
        pool_id,
        vault_id,
        vec![intent],
        advice,
        &[(asset0, trader_id)],
    )
    .await?;

    let pool_account = pool_client.try_get_account(pool_id).await?;
    let trades = user_trades_from_storage(pool_account.storage(), 0)?;
    assert_eq!(trades.sold, [swap_amount_in, 0]);
    assert_eq!(trades.bought, [0, amount_out_u64]);

    // ------------------------------------------------------------------------------------------
    // FINAL WITHDRAW: the fee-covered remainder is self-custodially withdrawable
    // ------------------------------------------------------------------------------------------
    let final_withdraw = DEPOSIT_AMOUNT + ACCRUED_FEES - first_withdraw;
    info!("[TEST] withdrawing the remaining {final_withdraw}");
    withdraw_liquidity_from_vault(
        &mut client,
        vault_id,
        lp_id,
        FungibleAsset::new(asset0, final_withdraw)?,
    )
    .await?;

    let lp_info = get_vault_lp_info(&client, vault_id, asset0, lp_id).await?;
    assert_eq!(lp_info.withdrawable(), 0);

    consume_all_notes_for(&mut client, lp_id).await?;
    let lp_wallet = client.account_reader(lp_id).get_balance(asset0).await?;
    assert_eq!(lp_wallet, lp_wallet_start + ACCRUED_FEES);

    Ok(())
}
