//! Dedicated finality worker: syncs separate per-pool clients and confirms submissions
//! without sharing the execute/prove task.

use std::{
    collections::{HashMap, HashSet},
    env,
    sync::Arc,
    time::Duration,
};

use anyhow::{Result, anyhow};
use miden_client::{
    Client, Deserializable, Serializable,
    account::AccountId,
    keystore::FilesystemKeyStore,
    store::TransactionFilter,
    transaction::{TransactionId, TransactionStatus, TransactionStoreUpdate},
};
use tokio::sync::broadcast::error::RecvError;
use tracing::{error, info, warn};

use crate::{
    execution_store::{BatchOrderOutcome, ExecutionStore},
    message_broker::message_broker::{AmmEvent, MessageBroker},
    order::{Order, OrderFailureReason, OrderUpdate, Processed},
    pool_registry::PoolRegistry,
    test_utils::get_pool_finality_client_for,
};

/// Cadence for rechecking submitted txs for chain confirmation.
///
/// Prefers `FINALITY_RETRY_MS` (default **500**). Falls back to legacy
/// `FINALITY_RETRY_SECS * 1000` when only that env var is set.
fn finality_retry_millis() -> u64 {
    if let Some(ms) = env::var("FINALITY_RETRY_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        return ms;
    }
    if let Some(secs) = env::var("FINALITY_RETRY_SECS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        return secs.saturating_mul(1_000);
    }
    500
}

pub struct FinalityObserver {
    pool_clients: HashMap<AccountId, Client<FilesystemKeyStore>>,
    message_broker: Arc<MessageBroker>,
    execution_store: Arc<ExecutionStore>,
    pool_registry: Arc<PoolRegistry>,
}

impl FinalityObserver {
    pub async fn initialize(
        message_broker: Arc<MessageBroker>,
        execution_store: Arc<ExecutionStore>,
        pool_registry: Arc<PoolRegistry>,
    ) -> Result<Self> {
        let pools = pool_registry.pools();
        let mut pool_clients = HashMap::with_capacity(pools.len());
        for pool_id in &pools {
            pool_clients.insert(*pool_id, attach_finality_client(*pool_id).await?);
        }
        info!(
            pools = pool_clients.len(),
            observer = "finality",
            "Finality observer attached to pool shards"
        );
        Ok(Self {
            pool_clients,
            message_broker,
            execution_store,
            pool_registry,
        })
    }

    pub async fn start(&mut self) -> Result<()> {
        let mut amm_rx = self.message_broker.subscribe_amm();
        let mut registry_rx = self.pool_registry.subscribe();
        let mut durable_poll = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                _ = durable_poll.tick() => {
                    self.attach_pending_pools().await;
                    self.poll_once().await;
                }
                changed = registry_rx.changed() => {
                    if changed.is_err() {
                        break;
                    }
                    self.attach_pending_pools().await;
                }
                event = amm_rx.recv() => {
                    match event {
                        Ok(AmmEvent::BatchSubmitted) => self.poll_once().await,
                        Ok(_) => {}
                        Err(RecvError::Lagged(n)) => {
                            warn!(
                                lagged = n,
                                observer = "finality",
                                "amm events lagged; polling durable store"
                            );
                            self.poll_once().await;
                        }
                        Err(RecvError::Closed) => break,
                    }
                }
            }
        }
        Err(anyhow!("Termination of finality observer."))
    }

    async fn poll_once(&mut self) {
        let now = chrono::Utc::now().timestamp_millis() as u64;
        if let Err(error) = self.reconcile_submissions(now).await {
            error!(%error, observer = "finality", "Failed to reconcile submitted transactions");
        }
        match self.execution_store.pending_outbox(100) {
            Ok(entries) => {
                for entry in entries
                    .into_iter()
                    .filter(|entry| entry.topic == "batch_terminal")
                {
                    let Ok(batch_id) = uuid::Uuid::parse_str(&entry.aggregate_id) else {
                        continue;
                    };
                    match self.execution_store.recover_terminal_batch(batch_id) {
                        Ok(Some(batch)) => {
                            self.publish_terminal_notifications(
                                batch.id,
                                &batch.orders,
                                &batch.outcomes,
                            );
                        }
                        Ok(None) => {}
                        Err(error) => {
                            error!(
                                %batch_id,
                                %error,
                                observer = "finality",
                                "Failed to recover terminal batch notifications"
                            )
                        }
                    }
                }
            }
            Err(error) => {
                error!(%error, observer = "finality", "Failed to read execution outbox")
            }
        }
    }

    async fn ensure_pool_attached(&mut self, pool_id: AccountId) -> Result<()> {
        if self.pool_clients.contains_key(&pool_id) {
            self.pool_registry
                .acknowledge(pool_id, crate::pool_registry::PoolWorker::Finality);
            return Ok(());
        }
        if !self.pool_registry.contains(&pool_id)
            && !self.pool_registry.ensure_from_deployment(pool_id)?
        {
            return Err(anyhow!(
                "submission references unlisted pool {}",
                pool_id.to_hex()
            ));
        }
        self.pool_clients
            .insert(pool_id, attach_finality_client(pool_id).await?);
        self.pool_registry
            .acknowledge(pool_id, crate::pool_registry::PoolWorker::Finality);
        info!(
            pool = %pool_id.to_hex(),
            observer = "finality",
            "attached finality client for published shard"
        );
        Ok(())
    }

    async fn attach_pending_pools(&mut self) {
        for pool_id in self.pool_registry.pending_attach() {
            if let Err(error) = self.ensure_pool_attached(pool_id).await {
                warn!(
                    pool = %pool_id.to_hex(),
                    %error,
                    observer = "finality",
                    "failed to attach published pool shard"
                );
            }
        }
    }

    /// Observe chain commitment for submitted txs.
    ///
    /// Exec-client `local_applied` keeps the prove/submit SQLite in sync. This observer uses
    /// a separate store, so it must apply the durable `transaction_update` here before
    /// `sync_state` / `get_transactions` can see the pending row.
    async fn reconcile_submissions(&mut self, now: u64) -> Result<()> {
        let retry_millis = finality_retry_millis();
        let submissions: Vec<_> = self
            .execution_store
            .submitted_transactions()?
            .into_iter()
            .filter(|submission| now.saturating_sub(submission.updated_at) >= retry_millis)
            .collect();
        if submissions.is_empty() {
            return Ok(());
        }

        for pool_id in submissions
            .iter()
            .map(|s| s.pool_id)
            .collect::<HashSet<_>>()
        {
            if let Err(error) = self.ensure_pool_attached(pool_id).await {
                for submission in submissions.iter().filter(|s| s.pool_id == pool_id) {
                    self.execution_store.record_reconciliation_attempt(
                        &submission.tx_hash,
                        Some(&format!("attach finality client: {error}")),
                        now,
                    )?;
                }
            }
        }

        // Seed each finality store with pending txs (exec apply is a different SQLite file).
        let mut skip_tx_hashes = HashSet::new();
        for submission in &submissions {
            let tx_id = match TransactionId::read_from_bytes(&submission.tx_id) {
                Ok(tx_id) => tx_id,
                Err(error) => {
                    self.execution_store.fail_submission(
                        &submission.tx_hash,
                        &format!("persisted transaction id is invalid: {error}"),
                        now,
                    )?;
                    skip_tx_hashes.insert(submission.tx_hash.clone());
                    continue;
                }
            };
            let Some(client) = self.pool_clients.get_mut(&submission.pool_id) else {
                skip_tx_hashes.insert(submission.tx_hash.clone());
                continue;
            };
            let already_tracked = client
                .get_transactions(TransactionFilter::Ids(vec![tx_id]))
                .await?
                .into_iter()
                .any(|record| record.id == tx_id);
            if already_tracked {
                continue;
            }
            match TransactionStoreUpdate::read_from_bytes(&submission.transaction_update) {
                Ok(update) => match client.apply_transaction_update(update).await {
                    Ok(()) => {}
                    Err(error) => {
                        self.execution_store.record_reconciliation_attempt(
                            &submission.tx_hash,
                            Some(&format!("finality apply: {error}")),
                            now,
                        )?;
                        skip_tx_hashes.insert(submission.tx_hash.clone());
                    }
                },
                Err(error) => {
                    self.execution_store.fail_submission(
                        &submission.tx_hash,
                        &format!("persisted transaction update is invalid: {error}"),
                        now,
                    )?;
                    skip_tx_hashes.insert(submission.tx_hash.clone());
                }
            }
        }

        let sync_pools: HashSet<_> = submissions
            .iter()
            .filter(|s| !skip_tx_hashes.contains(&s.tx_hash))
            .map(|s| s.pool_id)
            .collect();
        let mut sync_by_pool = HashMap::new();
        for pool_id in sync_pools {
            let Some(client) = self.pool_clients.get_mut(&pool_id) else {
                continue;
            };
            match client.sync_state().await {
                Ok(summary) => {
                    sync_by_pool.insert(pool_id, summary);
                }
                Err(error) => {
                    for submission in submissions.iter().filter(|s| s.pool_id == pool_id) {
                        if skip_tx_hashes.contains(&submission.tx_hash) {
                            continue;
                        }
                        self.execution_store.record_reconciliation_attempt(
                            &submission.tx_hash,
                            Some(&format!("sync: {error}")),
                            now,
                        )?;
                    }
                }
            }
        }
        if sync_by_pool.is_empty() {
            return Ok(());
        }
        let max_attempts = env::var("FINALITY_MAX_ATTEMPTS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .unwrap_or(900);
        let timeout_secs = env::var("FINALITY_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(1_800);

        for submission in submissions {
            if skip_tx_hashes.contains(&submission.tx_hash) {
                continue;
            }
            let tx_id = match TransactionId::read_from_bytes(&submission.tx_id) {
                Ok(tx_id) => tx_id,
                Err(error) => {
                    self.execution_store.fail_submission(
                        &submission.tx_hash,
                        &format!("persisted transaction id is invalid: {error}"),
                        now,
                    )?;
                    continue;
                }
            };
            let Some(client) = self.pool_clients.get_mut(&submission.pool_id) else {
                self.execution_store.record_reconciliation_attempt(
                    &submission.tx_hash,
                    Some("no finality client for submission pool"),
                    now,
                )?;
                continue;
            };
            let Some(sync) = sync_by_pool.get(&submission.pool_id) else {
                continue;
            };
            let records = client
                .get_transactions(TransactionFilter::Ids(vec![tx_id]))
                .await?;
            let Some(record) = records.into_iter().find(|record| record.id == tx_id) else {
                self.execution_store.record_reconciliation_attempt(
                    &submission.tx_hash,
                    Some("transaction is not present in local client store"),
                    now,
                )?;
                if submission.attempts.saturating_add(1) >= max_attempts
                    || now.saturating_sub(submission.submitted_at)
                        >= timeout_secs.saturating_mul(1_000)
                {
                    self.execution_store.fail_submission(
                        &submission.tx_hash,
                        "confirmation timeout before transaction became observable",
                        now,
                    )?;
                }
                continue;
            };
            match record.status {
                TransactionStatus::Committed { block_number, .. } => {
                    let commitments_match = record.details.account_id == submission.pool_id
                        && record.details.init_account_state.to_bytes()
                            == submission.expected_initial_state
                        && record.details.final_account_state.to_bytes()
                            == submission.expected_final_state;
                    if commitments_match {
                        let finality_ms = now.saturating_sub(submission.submitted_at);
                        self.execution_store.confirm_submission(
                            &submission.tx_hash,
                            block_number.as_u32(),
                            now,
                        )?;
                        info!(
                            tx_hash = %submission.tx_hash,
                            pool = %submission.pool_id.to_hex(),
                            trades = submission.order_ids.len(),
                            block = block_number.as_u32(),
                            finality_ms,
                            observer = "finality",
                            "Shard transaction confirmed on chain"
                        );
                    } else {
                        self.execution_store.fail_submission(
                            &submission.tx_hash,
                            "committed transaction account commitments do not match submission",
                            now,
                        )?;
                    }
                }
                TransactionStatus::Discarded(cause) => {
                    self.execution_store.fail_submission(
                        &submission.tx_hash,
                        &format!("Miden discarded transaction: {cause}"),
                        now,
                    )?;
                }
                TransactionStatus::Pending => {
                    self.execution_store.record_reconciliation_attempt(
                        &submission.tx_hash,
                        None,
                        now,
                    )?;
                    let expired_by_height =
                        sync.block_num.as_u32() > submission.expiration_height.saturating_add(20);
                    let timed_out = now.saturating_sub(submission.submitted_at)
                        >= timeout_secs.saturating_mul(1_000)
                        || submission.attempts.saturating_add(1) >= max_attempts;
                    if expired_by_height || timed_out {
                        self.execution_store.fail_submission(
                            &submission.tx_hash,
                            "confirmation timeout while Miden transaction remained pending",
                            now,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn publish_terminal_notifications(
        &self,
        batch_id: uuid::Uuid,
        orders: &[Order<Processed>],
        outcomes: &[BatchOrderOutcome],
    ) {
        let orders_by_id: HashMap<_, _> = orders
            .iter()
            .cloned()
            .map(|order| (order.id, order))
            .collect();
        let mut confirmed = 0_usize;
        let mut failed = 0_usize;
        let mut tx_hashes = Vec::new();
        for outcome in outcomes {
            let Some(order) = orders_by_id.get(&outcome.order_id).cloned() else {
                continue;
            };
            let update = if let Some(error) = &outcome.error {
                failed += 1;
                warn!(
                    order_id = %order.id,
                    %error,
                    observer = "finality",
                    "Order execution failed"
                );
                OrderUpdate::Failed(order.failed(OrderFailureReason::ExecutionError, None))
            } else {
                confirmed += 1;
                if let Some(tx_hash) = outcome.tx_hash.as_deref()
                    && !tx_hash.is_empty()
                    && !tx_hashes.iter().any(|existing| existing == tx_hash)
                {
                    tx_hashes.push(tx_hash.to_owned());
                }
                OrderUpdate::Confirmed(
                    order
                        .clone()
                        .confirmed(outcome.tx_hash.clone().unwrap_or_default()),
                )
            };
            let _ = self.message_broker.broadcast_order_update(update);
        }
        if failed > 0 {
            let _ = self.message_broker.broadcast_amm(AmmEvent::BatchFailed);
        }
        info!(
            batch_id = %batch_id,
            trades = outcomes.len(),
            confirmed,
            failed,
            tx_hashes = %tx_hashes.join(","),
            observer = "finality",
            "Execution batch confirmed"
        );
        let _ = self.message_broker.broadcast_amm(AmmEvent::OrdersExecuted);
        let now = chrono::Utc::now().timestamp_millis() as u64;
        if let Ok(entries) = self.execution_store.pending_outbox(100) {
            for entry in entries {
                if entry.topic == "batch_terminal" && entry.aggregate_id == batch_id.to_string() {
                    let _ = self.execution_store.mark_outbox_delivered(entry.id, now);
                }
            }
        }
    }
}

async fn attach_finality_client(pool_id: AccountId) -> Result<Client<FilesystemKeyStore>> {
    let mut client = get_pool_finality_client_for(pool_id).await?;
    client.ensure_genesis_in_place().await?;
    // Vault must stay untracked (same as exec pool clients).
    client.import_account_by_id(pool_id).await?;
    client.sync_state().await?;
    Ok(client)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn finality_retry_prefers_ms_then_legacy_secs_then_default() {
        let _guard = ENV_LOCK.lock().unwrap();
        // SAFETY: serialized by ENV_LOCK; test-only env mutation.
        unsafe {
            env::remove_var("FINALITY_RETRY_MS");
            env::remove_var("FINALITY_RETRY_SECS");
        }
        assert_eq!(finality_retry_millis(), 500);

        unsafe {
            env::set_var("FINALITY_RETRY_SECS", "2");
        }
        assert_eq!(finality_retry_millis(), 2_000);

        unsafe {
            env::set_var("FINALITY_RETRY_MS", "250");
        }
        assert_eq!(finality_retry_millis(), 250);

        unsafe {
            env::remove_var("FINALITY_RETRY_MS");
            env::remove_var("FINALITY_RETRY_SECS");
        }
    }
}
