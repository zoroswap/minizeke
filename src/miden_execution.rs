use std::{
    collections::HashMap,
    env,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow};
use miden_client::{
    Client, RemoteTransactionProver,
    account::AccountId,
    keystore::FilesystemKeyStore,
    testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    },
    transaction::TransactionRequestBuilder,
};
use miden_client_sqlite_store::SqliteStore;
use miden_core::{Felt, Word};
use tokio::sync::broadcast::error::RecvError;
use tracing::{error, info};

use crate::{
    assembly_utils::{link_operator, link_pool},
    execution_script::make_exec_script,
    intent::Intent,
    message_broker::message_broker::{AmmEvent, MessageBroker},
    miden_env::MidenNetwork,
    order::{Order, OrderExecutionResult, OrderFailureReason, OrderUpdate, Orders, Processed},
    pool::{PoolState, deploy_pool, get_user_balance_storage_slot_name},
    user::{Users, get_users},
};

pub struct MidenExecution {
    client: Client<FilesystemKeyStore>,
    message_broker: Arc<MessageBroker>,
    prover_timeout: Duration,
    cycle: u64,
    asset0: AccountId,
    asset1: AccountId,
    pool_id: AccountId,
    orders: Orders,
    users: Users,
    pool_states: HashMap<AccountId, PoolState>,
}

impl MidenExecution {
    pub async fn initialize(message_broker: Arc<MessageBroker>) -> Result<Self> {
        const DEFAULT_TX_PROVER_TIMEOUT_SECS: u64 = 30;

        let network = MidenNetwork::from_env();
        let tx_prover_url = env::var("TX_PROVER_URL")
            .ok()
            .or_else(|| network.tx_prover_url());
        let tx_prover_timeout_secs = env::var("TX_PROVER_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(DEFAULT_TX_PROVER_TIMEOUT_SECS);

        let prover_timeout = Duration::from_secs(tx_prover_timeout_secs);

        let sqlite_store = SqliteStore::new(network.store_path().into()).await?;
        let store = Arc::new(sqlite_store);
        let keystore = Arc::new(FilesystemKeyStore::new("keystore".into())?);

        let mut client_builder = MidenNetwork::client_builder()
            .in_debug_mode(true.into())
            .store(store)
            .authenticator(keystore);

        if let Some(ref url) = tx_prover_url {
            let remote_prover =
                Arc::new(RemoteTransactionProver::new(url.clone()).with_timeout(prover_timeout));
            info!(
                network = network.as_str(),
                prover = %url,
                timeout_secs = tx_prover_timeout_secs,
                "Using Miden network with remote prover"
            );
            client_builder = client_builder.prover(remote_prover);
        } else {
            info!(
                network = network.as_str(),
                "Using Miden network with local prover"
            );
        }

        let mut client = client_builder.build().await?;

        client.ensure_genesis_in_place().await?;
        client.sync_state().await?;

        println!("Client ready.");

        // spawn the user accounts
        let users = get_users(1, &mut client).await?;

        let pool_0_balance = 10_000_000_000;
        let pool_1_balance = 10_000_000_000;

        let (pool, _) = deploy_pool(&mut client, users.clone()).await?;

        println!(
            "Pool deployed. BECH32: {}, HEX: {}",
            pool.id().to_bech32(network.endpoint().to_network_id()),
            pool.id().to_hex()
        );

        let tx = TransactionRequestBuilder::new().build()?;
        client.add_account(&pool, true).await?;
        client.submit_new_transaction(pool.id(), tx).await?;
        client.sync_state().await?;

        // sleep(Duration::from_secs(4));

        println!("Pool touched.");

        let asset0 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
        let asset1 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2)?;

        let user_amount = 1_000_000_000u64;

        let users = Users::new(users, user_amount, vec![asset0, asset1]);

        let mut pool_states = HashMap::with_capacity(2);
        pool_states.insert(
            asset0,
            PoolState {
                balance: pool_0_balance,
            },
        );
        pool_states.insert(
            asset1,
            PoolState {
                balance: pool_1_balance,
            },
        );

        Ok(Self {
            cycle: 0,
            pool_id: pool.id(),
            client,
            message_broker,
            prover_timeout,
            asset0,
            asset1,
            users,
            orders: Orders::default(),
            pool_states,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        let mut orders_rx = self.message_broker.subscribe_order_updates();
        let mut pool_state_rx = self.message_broker.subscribe_pool_state();
        let mut processed_batch_rx = self.message_broker.subscribe_processed_batch();

        loop {
            tokio::select! {
                orders = orders_rx.recv() => {
                    match orders {
                        Ok(ev) => self.orders.apply_order_update(ev),
                        Err(RecvError::Lagged(n)) => {
                            eprintln!("orders lagged behind by {n} messages");
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
                pool_states = pool_state_rx.recv() => {
                    match pool_states {
                        Ok(ev) => {
                            for (faucet_id, new_pool_state) in ev.pool_states.iter() {
                                self.pool_states.insert(*faucet_id, *new_pool_state);
                            }
                        }
                        Err(RecvError::Lagged(n)) => {
                            eprintln!("pool_states lagged behind by {n} messages");
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
                batch = processed_batch_rx.recv() => {
                    match batch {
                        Ok(orders) => self.handle_batch(orders).await,
                        Err(RecvError::Lagged(n)) => {
                            eprintln!("processed batch lagged behind by {n} messages");
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            };
        }
        Err(anyhow!("Termination of miden execution."))
    }

    /// Execute a processed batch on the pool. On failure, mark every order in the
    /// batch as failed and still release the processing gate so the engine never
    /// deadlocks waiting for a settlement that will never come.
    pub async fn handle_batch(&mut self, orders: Vec<Order<Processed>>) {
        info!(
            count = orders.len(),
            "Received processed batch for execution"
        );
        let started = Instant::now();
        if let Err(e) = self.execute_on_pool(orders.clone()).await {
            error!(
                elapsed = ?started.elapsed(),
                "Batch execution failed: {e:?}"
            );
            for order in orders {
                let failed = order.failed(OrderFailureReason::ExecutionError, None);
                let _ = self
                    .message_broker
                    .broadcast_order_update(OrderUpdate::Failed(failed));
            }
            // Always release the gate held by the Processing engine, even on failure,
            // so the pipeline never deadlocks.
            let _ = self.message_broker.broadcast_amm(AmmEvent::OrdersExecuted);
        }
    }

    async fn execute_on_pool(&mut self, orders: Vec<Order<Processed>>) -> Result<()> {
        info!(
            cycle = self.cycle,
            count = orders.len(),
            "Executing batch on pool"
        );
        self.cycle += 1;

        if orders.is_empty() {
            // Nothing to execute; release the processing gate immediately.
            self.message_broker
                .broadcast_amm(AmmEvent::OrdersExecuted)?;
            return Ok(());
        }

        let instant = Instant::now();

        // let pool_state_deltas = vec![
        //     PoolStateDelta {
        //         pool_index: sell_pool_index,
        //         set_amount: sell_pool_balance,
        //     },
        //     PoolStateDelta {
        //         pool_index: buy_pool_index,
        //         set_amount: buy_pool_balance,
        //     },
        // ];

        let mut intents = Vec::with_capacity(orders.len());
        let mut advice_data = vec![];

        let asset0 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
        // let slot_names = get_user_balance_storage_slot_names();
        for order in &orders {
            let user_id = order.user_id();
            let user_index = self.users.get_user_index(&order.user_id());
            // let user_index = self.users.get_user_index(&user_id);
            let details = order.details();

            let buy_idx = if details.asset_out.eq(&asset0) { 0 } else { 1 };
            let sell_idx = if details.asset_in.eq(&asset0) { 0 } else { 1 };
            let amount_out = order.execution_result().amount_out;

            let user_suffix: u64 = user_id.suffix().as_canonical_u64();
            let user_prefix: u64 = user_id.prefix().as_u64();
            let user_slot_key = get_user_balance_storage_slot_name(user_index);

            let intent = Intent {
                user_suffix,
                user_prefix,
                user_key_prefix: user_slot_key.id().prefix().as_canonical_u64(),
                user_key_suffix: user_slot_key.id().suffix().as_canonical_u64(),
                sell_idx,
                buy_idx,
                sell_amount: details.amount_in,
                buy_amount: amount_out,
            };

            let signed_order = order.signed_order();
            let pubkey = order.pubkey();

            let msg = intent.message_word();
            let pk_comm: Word = pubkey.to_commitment().into();

            info!(
                "pk_comm: {pk_comm:?}, msg: {:?}, user suffix: {}, user prefix: {}",
                intent.message_word(),
                intent.user_suffix,
                intent.user_prefix
            );

            let prepared: Vec<Felt> = signed_order.to_prepared_signature(msg); // [PK[9], SIG[17]]

            info!("prepared len: {}", prepared.len());

            // advice_data.extend_from_slice(msg.as_elements()); // MSG (4) — consumed first
            // advice_data.extend_from_slice(pk_comm.as_elements()); // PK_COMM (4)
            advice_data.extend_from_slice(&prepared); // PK[9], SIG[17]

            intents.push(intent);
        }

        let tx_script = make_exec_script(intents);

        info!("SCRIPT \n\n{tx_script}\n\n");

        let cb = link_operator(self.client.code_builder())?;
        let cb = link_pool(cb)?;
        let tx_script = cb.compile_tx_script(tx_script)?;

        let advice_map_key = Word::from([Felt::ZERO, Felt::ZERO, Felt::ZERO, Felt::ONE]);

        let tx_req = TransactionRequestBuilder::new()
            .custom_script(tx_script)
            .extend_advice_map([(advice_map_key, advice_data)])
            .build()?;

        let submit_started = Instant::now();
        let pool_id = self.pool_id;

        let tx_result = self.client.execute_transaction(pool_id, tx_req).await?;
        let measurements = tx_result.executed_transaction().measurements();
        info!(
            total_cycles = measurements.total_cycles(),
            auth_cycles = measurements.auth_procedure,
            "Transaction cycle count",
        );
        let prove_started = Instant::now();
        let proven_transaction = self
            .client
            .prove_transaction_with(&tx_result, self.client.prover())
            .await?;
        let prove_elapsed = prove_started.elapsed();
        let submission_height = self
            .client
            .submit_proven_transaction(proven_transaction, &tx_result)
            .await?;
        self.client
            .apply_transaction(&tx_result, submission_height)
            .await?;
        println!("Elapsed: {prove_elapsed:?}");
        self.client.sync_state().await?;

        // let res = self
        //     .client
        //     .submit_new_transaction(pool_id, tx_req)
        //     .await
        //     .map_err(|_| {
        //         anyhow!(
        //             "transaction prove/submit timed out after {:?}",
        //             self.prover_timeout
        //         )
        //     })?;

        info!(
            elapsed = ?submit_started.elapsed(),
            "Transaction proven and submitted"
        );

        let tx_hash = tx_result.id().to_hex().to_string();

        self.client.sync_state().await?;
        info!(%tx_hash, "Client state synced");

        let mut executed_count = 0usize;
        for order in orders {
            let details = order.details();
            let executed = order.executed(
                tx_hash.clone(),
                OrderExecutionResult {
                    amount_out: details.min_amount_out,
                },
            );
            self.message_broker
                .broadcast_order_update(OrderUpdate::Executed(executed))?;
            executed_count += 1;
        }

        // Release the gate held by the Processing engine. We do not wait for
        // on-chain settlement; the order lifecycle ends at Executed.
        self.message_broker
            .broadcast_amm(AmmEvent::OrdersExecuted)?;

        info!(
            count = executed_count,
            elapsed = ?instant.elapsed(),
            "Batch executed"
        );

        Ok(())
    }

    pub fn pool_id(&self) -> AccountId {
        self.pool_id
    }

    pub fn users(&self) -> Users {
        self.users.clone()
    }

    pub fn pool_states(&self) -> HashMap<AccountId, PoolState> {
        self.pool_states.clone()
    }
}

/// The depositor's user-id word: `[id_prefix, id_suffix, 0, 0]`. This is the raw `StorageMap` key
/// under which the operator stores/looks up this depositor's pubkey commitment (Plan 2 Q1).
pub fn user_id_word(account_id: AccountId) -> Word {
    Word::from([
        account_id.prefix().as_felt(),
        account_id.suffix(),
        0u32.into(),
        0u32.into(),
    ])
}
