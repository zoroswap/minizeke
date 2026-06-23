use std::{collections::HashMap, sync::Arc, time::Instant};

use anyhow::{Result, anyhow};
use miden_client::{
    Client, RemoteTransactionProver,
    account::AccountId,
    builder::ClientBuilder,
    keystore::FilesystemKeyStore,
    rpc::{Endpoint, GrpcClient},
    testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    },
    transaction::TransactionRequestBuilder,
};
use miden_client_sqlite_store::SqliteStore;
use tokio::sync::broadcast::error::RecvError;

use crate::{
    execution::{Trade, make_exec_script},
    message_broker::message_broker::{AmmEvent, MessageBroker, MessageBrokerEvent},
    order::{OrderExecutionResult, OrderUpdate, Orders},
    pool::{PoolState, deploy_pool, link_pool},
    user::{Users, get_users},
};

pub struct MidenExecution {
    client: Client<FilesystemKeyStore>,
    message_broker: Arc<MessageBroker>,
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
        let remote_prover = Arc::new(RemoteTransactionProver::new(
            "https://tx-prover.devnet.miden.io",
        ));
        let sqlite_store = SqliteStore::new("store.sqlite3".into()).await?;
        let store = Arc::new(sqlite_store);
        let rpc_client = Arc::new(GrpcClient::new(&Endpoint::devnet(), 30_000));
        let keystore = Arc::new(FilesystemKeyStore::new("keystore".into())?);

        // Build client with remote prover as default
        let mut client = ClientBuilder::new()
            //.in_debug_mode(true.into())
            .prover(remote_prover.clone())
            .store(store)
            .rpc(rpc_client)
            .authenticator(keystore)
            .build()
            .await?;

        client.ensure_genesis_in_place().await?;
        client.sync_state().await?;

        println!("Client ready.");

        // spawn the user accounts
        let users = get_users(10, &mut client).await?;

        let pool_0_balance = 10_000_000;
        let pool_1_balance = 10_000_000;
        // spawn the pool account
        let (pool, pool_component) =
            deploy_pool(&mut client, users.clone(), pool_0_balance, pool_1_balance).await?;

        println!(
            "Pool deployed. BECH32: {}, HEX: {}",
            pool.id().to_bech32(Endpoint::devnet().to_network_id()),
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

        let user_amount = 1000u64;
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
        let mut amm_rx = self.message_broker.subscribe_amm();

        loop {
            let event = tokio::select! {
                orders = orders_rx.recv() => {
                    match orders {
                        Ok(ev) => MessageBrokerEvent::Order(ev),
                        Err(RecvError::Lagged(n)) => {
                            eprintln!("orders lagged behind by {n} messages");
                            continue;
                        }
                        Err(RecvError::Closed) => {
                            break;
                        }
                    }
                }
                pool_states = pool_state_rx.recv() => {
                    match pool_states {
                        Ok(ev) => MessageBrokerEvent::PoolState(ev),
                        Err(RecvError::Lagged(n)) => {
                            eprintln!("pool_states lagged behind by {n} messages");
                            continue;
                        }
                        Err(RecvError::Closed) => {
                            break;
                        }
                    }
                }
                amm = amm_rx.recv() => {
                    match amm {
                        Ok(ev) => MessageBrokerEvent::Amm(ev),
                        Err(RecvError::Lagged(n)) => {
                            eprintln!("amm lagged behind by {n} messages");
                            continue;
                        }
                        Err(RecvError::Closed) => {
                            break;
                        }
                    }
                }

            };
            self.handle_event(event).await;
        }
        Err(anyhow!("Termination of miden execution."))
    }

    async fn handle_event(&mut self, event: MessageBrokerEvent) {
        match event {
            MessageBrokerEvent::Order(ev) => {
                self.orders.apply_order_update(ev);
            }
            MessageBrokerEvent::PoolState(ev) => {
                for (faucet_id, new_pool_state) in ev.pool_states.iter() {
                    self.pool_states.insert(*faucet_id, *new_pool_state);
                }
            }
            MessageBrokerEvent::Amm(ev) => match ev {
                AmmEvent::OrdersProcessed => {
                    if let Err(e) = self.execute_on_pool().await {
                        eprintln!("[MIDEN EXECUTION] Error: {e:?}");
                    }
                }
                _ => {}
            },
            _ => {}
        }
    }

    async fn execute_on_pool(&mut self) -> Result<()> {
        println!("[MIDEN EXECUTION] cycle {}", self.cycle);
        self.cycle += 1;

        let instant = Instant::now();
        let orders = self.orders.orders_processed();

        let mut trades = Vec::new();
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
        let asset0 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
        let asset1 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2)?;

        for order in &orders {
            let user_index = self
                .users
                .get_user_index(&order.user_id())
                .ok_or(anyhow!("user not found"))?;
            let details = order.details();
            let buy_asset_index = if details.asset_out.eq(&asset0) { 0 } else { 1 };
            let sell_asset_index = if details.asset_out.eq(&asset1) { 0 } else { 1 };
            let amount_out = order.execution_result().amount_out;
            let trade = Trade {
                user_index,
                sell_asset_index,
                buy_asset_index,
                sell_amount: details.amount_in,
                buy_amount: amount_out,
            };
            trades.push(trade);
        }

        let tx_script = make_exec_script(trades);

        println!("SCRIPT \n\n{tx_script}\n\n");

        let cb = link_pool(self.client.code_builder())?;
        let tx_script = cb.compile_tx_script(tx_script)?;

        let tx_req = TransactionRequestBuilder::new()
            .custom_script(tx_script)
            .build()?;

        let tx_result = self
            .client
            .execute_transaction(self.pool_id, tx_req)
            .await?;
        let measurements = tx_result.executed_transaction().measurements();
        println!(
            "Cycle count: {}, auth: {}",
            measurements.total_cycles(),
            measurements.auth_procedure
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
        let tx_hash = tx_result.id().to_string();
        println!("Elapsed: {prove_elapsed:?}");
        self.client.sync_state().await?;

        for order in orders {
            let details = order.details();
            self.message_broker
                .broadcast_order_update(OrderUpdate::Executed(order.executed(
                    tx_hash.clone(),
                    OrderExecutionResult {
                        amount_out: details.min_amount_out,
                    },
                )))?;
        }

        self.message_broker
            .broadcast_amm(AmmEvent::OrdersExecuted)?;

        self.message_broker
            .broadcast_amm(AmmEvent::StartProcessing)?;

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
