use std::{
    collections::VecDeque,
    path::PathBuf,
    sync::{Arc, LazyLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use miden_client::{
    Client, RemoteTransactionProver,
    account::{
        Account, AccountBuilder, AccountId, AccountType,
        component::{AuthSingleSig, BasicWallet},
    },
    asset::FungibleAsset,
    auth::{AuthScheme, AuthSecretKey},
    keystore::{FilesystemKeyStore, Keystore},
};
use miden_client_sqlite_store::SqliteStore;
use miden_core::Word;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::{Mutex, Semaphore, mpsc, watch},
    task::JoinSet,
};
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    deployment::{AssetInfo, Deployment},
    intent::Intent,
    miden_env::{MidenNetwork, miden_debug_mode_enabled},
    order::OrderDetails,
    pool::deploy_pool,
    simulate::{
        api::{OrderOutcome, Session, SimulationApi},
        config::{Config, TraderTier},
        metrics::{Metrics, SetupPhase, TradeMeasurement, jittered_interval},
        oracle::{OracleClient, minimum_amount_out},
        ws::{self, TerminalOrderStatus},
    },
    test_utils::{
        consume_all_notes_for, consume_all_notes_for_setup, fund_user_on_vault, get_client,
        get_pool_client_for, init_redeem_on_vault, redeem_on_vault,
        register_and_fund_user_on_vault,
    },
    vault::{add_pool_to_vault, get_vault_user_asset_info},
};

/// Shared registry of live traders (growth + vault cycles).
pub type TraderRegistry = Arc<Mutex<Vec<Trader>>>;

/// Serializes live-onboard steps that touch the shared operator store / deployment file.
/// Concurrent `get_client()` opens the same SQLite DB and fails with bare "storage error" /
/// "RPC error".
static LIVE_ONBOARD_SHARED: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
const MAX_IN_FLIGHT_PER_TRADER: usize = 2;

#[derive(Clone)]
pub struct Trader {
    pub index: usize,
    pub tier: TraderTier,
    pub user_id: AccountId,
    key: AuthSecretKey,
}

/// Authenticate every trader before trade loops so cycle metrics are not dominated by
/// auth rate-limit waits. Returns sessions aligned with `traders`.
pub async fn warm_auth_sessions(
    traders: &[Trader],
    api: &SimulationApi,
    gap: Duration,
) -> Result<Vec<Session>> {
    let mut sessions = Vec::with_capacity(traders.len());
    for (index, trader) in traders.iter().enumerate() {
        let (session, timing) = api.authenticate(trader.user_id, &trader.key).await?;
        debug!(
            trader = trader.index,
            auth_ms = timing.http.as_millis(),
            auth_wait_ms = timing.wait.as_millis(),
            "auth session ready"
        );
        sessions.push(session);
        if index + 1 < traders.len() && !gap.is_zero() {
            tokio::time::sleep(gap).await;
        }
    }
    Ok(sessions)
}

struct WalletAccount {
    user_id: AccountId,
    key: AuthSecretKey,
}

struct SetupJob {
    index: usize,
    tier: TraderTier,
    user_id: AccountId,
    account: Account,
    keystore_dir: PathBuf,
    store_path: PathBuf,
    vault_id: AccountId,
    assets: Vec<AssetInfo>,
    fund_amount: u64,
    metrics: Metrics,
    /// Pool-assignment wave; worker waits until the coordinator activates this wave.
    shard_wave: usize,
    shard_wave_rx: watch::Receiver<Option<usize>>,
    shard_done_tx: mpsc::Sender<usize>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SimulationState {
    network: String,
    vault_id: String,
    #[serde(default)]
    setup_complete: bool,
    traders: Vec<TraderState>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TraderState {
    index: usize,
    user_id: String,
    public_key_commitment: String,
}

pub async fn build_simulation_client(
    config: &Config,
) -> Result<(Client<FilesystemKeyStore>, Arc<FilesystemKeyStore>)> {
    let network = MidenNetwork::from_env();
    let store = Arc::new(SqliteStore::new(config.store_path.clone()).await?);
    let keystore = Arc::new(FilesystemKeyStore::new(config.keystore_dir.clone())?);
    let debug_mode = miden_debug_mode_enabled();
    if debug_mode {
        warn!("MIDEN_DEBUG_MODE enabled; simulation client execution will be substantially slower");
    }
    let mut builder = MidenNetwork::client_builder()
        .in_debug_mode(debug_mode.into())
        .store(store)
        .authenticator(keystore.clone());
    if let Some(url) = std::env::var("TX_PROVER_URL")
        .ok()
        .or_else(|| network.tx_prover_url())
    {
        let timeout = std::env::var("TX_PROVER_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(30);
        builder = builder.prover(Arc::new(
            RemoteTransactionProver::new(url).with_timeout(Duration::from_secs(timeout)),
        ));
    }
    let mut client = builder.build().await?;
    client.ensure_genesis_in_place().await?;
    client.sync_state().await?;
    Ok((client, keystore))
}

/// Removes prior setup artifacts so a full setup run starts clean. Call before opening the
/// simulation client when `--skip-setup` is not set.
pub fn reset_setup_artifacts(config: &Config) -> Result<()> {
    let removed = remove_path_if_exists(&config.state_file)?
        + remove_sqlite_store(&config.store_path)?
        + remove_worker_stores(&config.store_path)?;
    if removed > 0 {
        info!(removed, "removed previous setup artifacts");
    }
    Ok(())
}

/// Moves stores created by older simulator versions out of the repository root.
/// Existing files in the destination win, so a completed migrated cohort is never overwritten.
pub fn migrate_legacy_setup_artifacts(config: &Config) -> Result<()> {
    let storage_dir = config
        .state_file
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("simulation_stores"));
    std::fs::create_dir_all(storage_dir)
        .with_context(|| format!("create simulator storage {}", storage_dir.display()))?;

    let mut moved = 0;
    moved += move_path_if_destination_missing(
        std::path::Path::new("simulate_traders.state.json"),
        &config.state_file,
    )?;
    moved += move_path_if_destination_missing(
        std::path::Path::new("simulate_keystore"),
        &config.keystore_dir,
    )?;

    for entry in std::fs::read_dir(".").context("read repository root for legacy stores")? {
        let entry = entry.context("read legacy simulator store entry")?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if !name.starts_with("simulate.store.sqlite3") {
            continue;
        }
        moved += move_path_if_destination_missing(&entry.path(), &storage_dir.join(name))?;
    }
    if moved > 0 {
        info!(
            moved,
            directory = %storage_dir.display(),
            "migrated legacy simulator artifacts"
        );
    }
    Ok(())
}

fn move_path_if_destination_missing(
    source: &std::path::Path,
    destination: &std::path::Path,
) -> Result<usize> {
    if !source.exists() || destination.exists() {
        return Ok(0);
    }
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    std::fs::rename(source, destination).with_context(|| {
        format!(
            "move legacy simulator artifact {} to {}",
            source.display(),
            destination.display()
        )
    })?;
    Ok(1)
}

fn remove_worker_stores(base: &PathBuf) -> Result<usize> {
    let file_name = base
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("simulate.store.sqlite3");
    let prefix = format!("{file_name}.setup.");
    let parent = base.parent().filter(|path| !path.as_os_str().is_empty());
    let dir = parent.unwrap_or_else(|| std::path::Path::new("."));
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(error) => {
            return Err(error).with_context(|| format!("read {}", dir.display()));
        }
    };
    let mut removed = 0;
    for entry in entries {
        let entry = entry.with_context(|| format!("read entry in {}", dir.display()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(&prefix) {
            removed += remove_sqlite_store(&entry.path())?;
        }
    }
    Ok(removed)
}

fn remove_path_if_exists(path: &PathBuf) -> Result<usize> {
    if path.exists() {
        std::fs::remove_file(path).with_context(|| format!("remove {}", path.display()))?;
        return Ok(1);
    }
    Ok(0)
}

fn remove_sqlite_store(path: &PathBuf) -> Result<usize> {
    let mut removed = remove_path_if_exists(path)?;
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        removed += remove_path_if_exists(&PathBuf::from(sidecar))?;
    }
    Ok(removed)
}

pub async fn setup_traders(
    config: &Config,
    deployment: &mut Deployment,
    api: &SimulationApi,
    metrics: &Metrics,
    client: &mut Client<FilesystemKeyStore>,
    keystore: &FilesystemKeyStore,
) -> Result<Vec<Trader>> {
    let tiers = config.tier_assignments();
    info!(traders = config.max_traders, "creating trader wallets");

    ensure_pool_shards(config, deployment, config.max_traders).await?;

    let mut traders = Vec::with_capacity(config.max_traders);
    for (index, tier) in tiers.into_iter().enumerate() {
        let wallet = create_wallet(client, keystore).await?;
        debug!(
            trader = index,
            tier = tier.label(),
            user = %wallet.user_id.to_hex(),
            "trader wallet created"
        );
        traders.push(Trader {
            index,
            tier,
            user_id: wallet.user_id,
            key: wallet.key,
        });
    }
    save_state(config, deployment, &traders, false)?;

    // Workers must add_account (and thus track note tags) *before* mint notes are
    // committed. Fresh stores created after mint sync at tip with notes=0 forever.
    let concurrency = config.setup_concurrency.min(traders.len()).max(1);
    let max_users = config.max_users_per_pool;
    let num_waves = traders.len().div_ceil(max_users);
    let (shard_wave_tx, shard_wave_rx) = watch::channel(None::<usize>);
    let (shard_done_tx, mut shard_done_rx) = mpsc::channel(traders.len());
    let mut jobs = Vec::with_capacity(traders.len());
    for trader in &traders {
        let account = client
            .try_get_account(trader.user_id)
            .await
            .with_context(|| format!("load account for trader {}", trader.index))?;
        let store_path = worker_store_path(&config.store_path, trader.index);
        jobs.push(SetupJob {
            index: trader.index,
            tier: trader.tier,
            user_id: trader.user_id,
            account,
            keystore_dir: config.keystore_dir.clone(),
            store_path,
            vault_id: deployment.vault_id,
            assets: deployment.assets.clone(),
            fund_amount: config.fund_amount,
            metrics: metrics.clone(),
            shard_wave: trader.index / max_users,
            shard_wave_rx: shard_wave_rx.clone(),
            shard_done_tx: shard_done_tx.clone(),
        });
    }
    drop(shard_done_tx);

    info!(
        traders = traders.len(),
        concurrency,
        pools = deployment.pools.len(),
        max_users_per_pool = max_users,
        waves = num_waves,
        "preparing worker clients before mint"
    );
    let (mint_done_tx, mint_done_rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = mpsc::channel(traders.len());
    let prepare_slots = Arc::new(Semaphore::new(concurrency));
    let prove_slots = Arc::new(Semaphore::new(concurrency));
    let mut set = JoinSet::new();
    let expected = jobs.len();
    for job in jobs {
        let ready_tx = ready_tx.clone();
        let mint_done_rx = mint_done_rx.clone();
        let prepare_slots = prepare_slots.clone();
        let prove_slots = prove_slots.clone();
        set.spawn_blocking(move || {
            run_setup_job(job, ready_tx, mint_done_rx, prepare_slots, prove_slots)
        });
    }
    drop(ready_tx);

    for prepared in 1..=expected {
        ready_rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("setup worker exited before signaling ready"))?;
        if prepared == expected || prepared % 10 == 0 {
            info!(prepared, expected, "worker clients ready");
        }
    }

    info!(
        assets = deployment.assets.len(),
        traders = traders.len(),
        "requesting faucet mints for all assets"
    );
    mint_all_assets(api, metrics, &traders, &deployment.assets).await?;
    mint_done_tx
        .send(true)
        .map_err(|_| anyhow!("no setup workers waiting for mint"))?;

    for wave in 0..num_waves {
        activate_pool_wave(deployment, wave).await?;
        shard_wave_tx
            .send(Some(wave))
            .map_err(|_| anyhow!("no setup workers waiting for shard wave {wave}"))?;
        let wave_size = traders
            .iter()
            .filter(|trader| trader.index / max_users == wave)
            .count();
        for completed in 1..=wave_size {
            shard_done_rx
                .recv()
                .await
                .ok_or_else(|| anyhow!("setup worker exited before finishing shard wave {wave}"))?;
            if completed == wave_size || completed % 5 == 0 {
                info!(wave, completed, wave_size, "shard wave register progress");
            }
        }
    }

    while let Some(joined) = set.join_next().await {
        joined.map_err(|error| anyhow!("setup worker join failed: {error}"))??;
    }

    save_state(config, deployment, &traders, true)?;
    info!(
        traders = traders.len(),
        pools = deployment.pools.len(),
        "parallel trader setup complete"
    );
    Ok(traders)
}

/// Ensure `ceil(trader_count / max_users_per_pool)` pools exist and are listed in deployment.
async fn ensure_pool_shards(
    config: &Config,
    deployment: &mut Deployment,
    trader_count: usize,
) -> Result<()> {
    let needed = trader_count.div_ceil(config.max_users_per_pool).max(1);
    let pools_before = deployment.pools.len();
    while deployment.pools.len() < needed {
        let pool_index = deployment.pools.len();
        info!(
            pool_index,
            needed, "deploying additional pool shard for user cap"
        );
        let mut deploy_client = crate::test_utils::get_pool_client().await?;
        deploy_client.ensure_genesis_in_place().await?;
        deploy_client.sync_state().await?;
        let pool_id = deploy_pool(&mut deploy_client, deployment.vault_id)
            .await?
            .id();
        let mut operator = get_client().await?;
        operator.ensure_genesis_in_place().await?;
        operator.sync_state().await?;
        add_pool_to_vault(
            &mut operator,
            deployment.operator_account_id,
            deployment.vault_id,
            pool_id,
        )
        .await?;
        // Warm the per-pool execution store so the server can attach later.
        let mut shard_client = get_pool_client_for(pool_id).await?;
        shard_client.ensure_genesis_in_place().await?;
        shard_client.import_account_by_id(pool_id).await?;
        shard_client.sync_state().await?;
        deployment.pools.push(pool_id);
        deployment.save()?;
        info!(pool = %pool_id.to_hex(), pools = deployment.pools.len(), "pool shard added");
    }
    if deployment.pools.len() > pools_before {
        let pools = deployment.pools.len();
        warn!(
            pools,
            "deployment now has {pools} pools — restart the API server before trading (or rely on hot-attach)"
        );
        eprintln!(
            "WARNING: deployment now has {pools} pools — restart the API server before trading"
        );
    }
    Ok(())
}

/// Re-assert ACTIVE_POOL for this registration wave (safe if already authorized).
async fn activate_pool_wave(deployment: &Deployment, wave: usize) -> Result<()> {
    let pool_id = *deployment
        .pools
        .get(wave)
        .ok_or_else(|| anyhow!("missing pool for shard wave {wave}"))?;
    let mut operator = get_client().await?;
    operator.ensure_genesis_in_place().await?;
    operator.sync_state().await?;
    add_pool_to_vault(
        &mut operator,
        deployment.operator_account_id,
        deployment.vault_id,
        pool_id,
    )
    .await?;
    info!(wave, pool = %pool_id.to_hex(), "activated pool for registration wave");
    Ok(())
}

/// Fire concurrent faucet mints for every (trader, asset). Consumption happens in workers.
async fn mint_all_assets(
    api: &SimulationApi,
    metrics: &Metrics,
    traders: &[Trader],
    assets: &[AssetInfo],
) -> Result<()> {
    let asset_futures = assets.iter().map(|asset| {
        let api = api.clone();
        let metrics = metrics.clone();
        let recipients = traders
            .iter()
            .map(|trader| (trader.index, trader.tier, trader.user_id))
            .collect::<Vec<_>>();
        let faucet_id = asset.faucet_id;
        let symbol = asset.symbol.clone();
        async move {
            let started = Instant::now();
            let recipient_count = recipients.len();
            info!(
                asset = %symbol,
                recipients = recipient_count,
                "minting to traders via faucet batch"
            );
            let mint_futures = recipients.into_iter().map(|(index, tier, user_id)| {
                let api = api.clone();
                async move { (index, tier, api.mint(user_id, faucet_id).await) }
            });
            let results = futures_util::future::join_all(mint_futures).await;
            for (index, tier, result) in results {
                let latency = result.with_context(|| format!("mint {symbol} to trader {index}"))?;
                metrics.record_setup(index, tier, SetupPhase::Mint, latency);
            }
            info!(
                asset = %symbol,
                recipients = recipient_count,
                elapsed_ms = started.elapsed().as_millis(),
                "faucet batch mint requests completed"
            );
            Ok::<(), anyhow::Error>(())
        }
    });
    for result in futures_util::future::join_all(asset_futures).await {
        result?;
    }
    Ok(())
}

fn run_setup_job(
    job: SetupJob,
    ready_tx: mpsc::Sender<usize>,
    mint_done_rx: watch::Receiver<bool>,
    prepare_slots: Arc<Semaphore>,
    prove_slots: Arc<Semaphore>,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build setup worker runtime")?;
    runtime.block_on(onboard_trader(
        job,
        ready_tx,
        mint_done_rx,
        prepare_slots,
        prove_slots,
    ))
}

async fn onboard_trader(
    job: SetupJob,
    ready_tx: mpsc::Sender<usize>,
    mut mint_done_rx: watch::Receiver<bool>,
    prepare_slots: Arc<Semaphore>,
    prove_slots: Arc<Semaphore>,
) -> Result<()> {
    let SetupJob {
        index,
        tier,
        user_id,
        account,
        keystore_dir,
        store_path,
        vault_id,
        assets,
        fund_amount,
        metrics,
        shard_wave,
        mut shard_wave_rx,
        shard_done_tx,
    } = job;

    let prepare_slot = prepare_slots
        .acquire()
        .await
        .map_err(|_| anyhow!("prepare slot closed for trader {index}"))?;
    let mut client = build_worker_client(&keystore_dir, &store_path).await?;
    client.add_account(&account, false).await?;
    client.sync_state().await?;
    ready_tx
        .send(index)
        .await
        .map_err(|_| anyhow!("setup coordinator dropped while trader {index} was preparing"))?;
    drop(prepare_slot);

    mint_done_rx
        .wait_for(|done| *done)
        .await
        .map_err(|_| anyhow!("mint signal closed before trader {index} started consume"))?;

    {
        let _prove_slot = prove_slots
            .acquire()
            .await
            .map_err(|_| anyhow!("prove slot closed for trader {index}"))?;
        ensure_wallet_balances(&mut client, index, user_id, &assets, fund_amount)
            .await
            .with_context(|| format!("fund wallet for trader {index}"))?;
    }

    let fund_assets = assets
        .iter()
        .map(|asset| {
            FungibleAsset::new(asset.faucet_id, fund_amount)
                .map_err(|error| anyhow!("invalid funding asset {}: {error:?}", asset.symbol))
        })
        .collect::<Result<Vec<_>>>()?;

    let keys = FilesystemKeyStore::new(keystore_dir)?
        .get_keys_for_account(&user_id)
        .await
        .with_context(|| format!("load key for trader {index}"))?;
    let pubkey = keys
        .first()
        .ok_or_else(|| anyhow!("trader {index} key missing from keystore"))?
        .public_key()
        .to_commitment()
        .into();

    // Do not hold prove slots while waiting for the coordinator to activate this wave.
    shard_wave_rx
        .wait_for(|wave| matches!(wave, Some(w) if *w >= shard_wave))
        .await
        .map_err(|_| anyhow!("shard wave signal closed before trader {index} registered"))?;

    let started = Instant::now();
    {
        let _prove_slot = prove_slots
            .acquire()
            .await
            .map_err(|_| anyhow!("prove slot closed for trader {index} register"))?;
        register_and_fund_user_on_vault(&mut client, vault_id, user_id, pubkey, &fund_assets)
            .await
            .with_context(|| format!("register/fund trader {index} on vault"))?;
    }
    let elapsed = started.elapsed();
    metrics.record_setup(index, tier, SetupPhase::Register, elapsed);
    metrics.record_setup(index, tier, SetupPhase::Fund, elapsed);

    shard_done_tx
        .send(index)
        .await
        .map_err(|_| anyhow!("setup coordinator dropped after trader {index} registered"))?;

    debug!(
        trader = index,
        shard_wave,
        user = %user_id.to_hex(),
        elapsed_ms = elapsed.as_millis(),
        "trader setup complete"
    );
    Ok(())
}

async fn ensure_wallet_balances(
    client: &mut Client<FilesystemKeyStore>,
    index: usize,
    user_id: AccountId,
    assets: &[AssetInfo],
    needed: u64,
) -> Result<()> {
    // Notes are minted only after this worker already tracks the account. Do not remint:
    // faucet cooldown is per (recipient, faucet) and reminting just burns the wait.
    let deadline = Instant::now() + Duration::from_secs(300);

    loop {
        sync_for_notes(client)
            .await
            .with_context(|| format!("sync while waiting for mint notes (trader {index})"))?;
        let consumable = client.get_consumable_notes(Some(user_id)).await?;
        if !consumable.is_empty() {
            consume_all_notes_for_setup(client, user_id)
                .await
                .with_context(|| format!("consume mint notes for trader {index}"))?;
            continue;
        }

        let mut short = Vec::new();
        for asset in assets {
            let balance = client
                .account_reader(user_id)
                .get_balance(asset.faucet_id)
                .await?;
            if balance < needed {
                short.push(asset.symbol.as_str());
            }
        }
        if short.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "trader {index} still missing funds for {} after timeout \
                 (mint notes never became consumable on this worker store)",
                short.join(", ")
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Sync notes with a timeout; fall back to `sync_chain` on NTL/tag-cap or hung `sync_state`.
async fn sync_for_notes(client: &mut Client<FilesystemKeyStore>) -> Result<()> {
    const SYNC_TIMEOUT: Duration = Duration::from_secs(45);
    match tokio::time::timeout(SYNC_TIMEOUT, client.sync_state()).await {
        Ok(Ok(_)) => Ok(()),
        Ok(Err(error)) => {
            let message = format!("{error:#}");
            warn!(%error, "sync_state failed; falling back to sync_chain");
            if message.to_ascii_lowercase().contains("too many tags")
                || message.to_ascii_lowercase().contains("note transport")
            {
                tokio::time::timeout(SYNC_TIMEOUT, client.sync_chain())
                    .await
                    .map_err(|_| anyhow!("sync_chain timed out after note-transport failure"))?
                    .map_err(|error| anyhow!("sync_chain failed: {error:#}"))?;
                return Ok(());
            }
            // Still try chain sync once before surfacing the original error.
            match tokio::time::timeout(SYNC_TIMEOUT, client.sync_chain()).await {
                Ok(Ok(_)) => Ok(()),
                Ok(Err(_)) | Err(_) => Err(error.into()),
            }
        }
        Err(_) => {
            warn!("sync_state timed out; falling back to sync_chain");
            tokio::time::timeout(SYNC_TIMEOUT, client.sync_chain())
                .await
                .map_err(|_| anyhow!("sync_chain timed out after sync_state timeout"))?
                .map_err(|error| anyhow!("sync_chain failed: {error:#}"))?;
            Ok(())
        }
    }
}

async fn build_worker_client(
    keystore_dir: &PathBuf,
    store_path: &PathBuf,
) -> Result<Client<FilesystemKeyStore>> {
    let network = MidenNetwork::from_env();
    if let Some(parent) = store_path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    let store = Arc::new(SqliteStore::new(store_path.clone()).await?);
    let keystore = Arc::new(FilesystemKeyStore::new(keystore_dir.clone())?);
    let debug_mode = miden_debug_mode_enabled();
    if debug_mode {
        warn!("MIDEN_DEBUG_MODE enabled; worker client execution will be substantially slower");
    }
    let mut builder = MidenNetwork::client_builder()
        .in_debug_mode(debug_mode.into())
        .store(store)
        .authenticator(keystore);
    if let Some(url) = std::env::var("TX_PROVER_URL")
        .ok()
        .or_else(|| network.tx_prover_url())
    {
        let timeout = std::env::var("TX_PROVER_TIMEOUT_SECS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(30);
        builder = builder.prover(Arc::new(
            RemoteTransactionProver::new(url).with_timeout(Duration::from_secs(timeout)),
        ));
    }
    let mut client = builder.build().await?;
    client.ensure_genesis_in_place().await?;
    client.sync_state().await?;
    Ok(client)
}

fn worker_store_path(base: &PathBuf, index: usize) -> PathBuf {
    let file_name = base
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("simulate.store.sqlite3");
    let worker_name = format!("{file_name}.setup.{index}");
    match base.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.join(worker_name),
        _ => PathBuf::from(worker_name),
    }
}

async fn create_wallet(
    client: &mut Client<FilesystemKeyStore>,
    keystore: &FilesystemKeyStore,
) -> Result<WalletAccount> {
    let mut seed = [0_u8; 32];
    client.rng().fill_bytes(&mut seed);
    let key = AuthSecretKey::new_ecdsa_k256_keccak();
    let account = AccountBuilder::new(seed)
        .account_type(AccountType::Public)
        .with_auth_component(AuthSingleSig::new(
            key.public_key().to_commitment(),
            AuthScheme::EcdsaK256Keccak,
        ))
        .with_component(BasicWallet)
        .build()?;
    let user_id = account.id();
    client.add_account(&account, false).await?;
    keystore.add_key(&key, user_id).await?;
    Ok(WalletAccount { user_id, key })
}

pub async fn load_traders(
    config: &Config,
    deployment: &Deployment,
    keystore: &FilesystemKeyStore,
) -> Result<Vec<Trader>> {
    let contents = std::fs::read_to_string(&config.state_file)
        .with_context(|| format!("read state file {}", config.state_file.display()))?;
    let state: SimulationState = serde_json::from_str(&contents)
        .with_context(|| format!("parse state file {}", config.state_file.display()))?;
    let network = MidenNetwork::from_env();
    if state.network != network.as_str() || state.vault_id != deployment.vault_id.to_hex() {
        bail!("state file belongs to a different network or vault");
    }
    if !state.setup_complete && !legacy_setup_looks_complete(config, state.traders.len()) {
        bail!("saved trader setup did not complete");
    }
    if state.traders.len() < config.max_traders {
        bail!(
            "state file contains {} traders, but {} were requested",
            state.traders.len(),
            config.max_traders
        );
    }

    let tiers = config.tier_assignments();
    let mut traders = Vec::with_capacity(config.max_traders);
    for (tier, saved) in tiers.into_iter().zip(state.traders.into_iter()) {
        let user_id = AccountId::from_hex(&saved.user_id)
            .map_err(|error| anyhow!("invalid trader user id: {error}"))?;
        let keys = keystore
            .get_keys_for_account(&user_id)
            .await
            .with_context(|| format!("load key for trader {}", saved.index))?;
        let key = keys
            .into_iter()
            .find(|key| {
                Word::from(key.public_key().to_commitment()).to_hex() == saved.public_key_commitment
            })
            .ok_or_else(|| anyhow!("trader {} key is missing from the keystore", saved.index))?;
        traders.push(Trader {
            index: saved.index,
            tier,
            user_id,
            key,
        });
    }
    Ok(traders)
}

fn legacy_setup_looks_complete(config: &Config, trader_count: usize) -> bool {
    if trader_count < config.max_traders {
        return false;
    }
    let Ok(state_modified) = std::fs::metadata(&config.state_file).and_then(|meta| meta.modified())
    else {
        return false;
    };
    (0..config.max_traders).all(|index| {
        let path = worker_store_path(&config.store_path, index);
        std::fs::metadata(path)
            .and_then(|meta| meta.modified())
            .is_ok_and(|store_modified| store_modified <= state_modified)
    })
}

pub async fn run_trader(
    trader: Trader,
    config: Arc<Config>,
    deployment: Arc<Deployment>,
    api: SimulationApi,
    oracle: OracleClient,
    metrics: Metrics,
    session: Option<Session>,
    mut shutdown: watch::Receiver<bool>,
) {
    let Some(session) = session else {
        warn!(
            trader = trader.index,
            "trader started without an auth session"
        );
        return;
    };
    let ws_url = match ws::ws_url_from_api(api.api_url()) {
        Ok(url) => url,
        Err(error) => {
            warn!(trader = trader.index, %error, "invalid settlement websocket URL");
            return;
        }
    };
    let tracker = ws::SettlementTracker::spawn(
        ws_url,
        session.access_token.clone(),
        config.order_timeout_secs,
        shutdown.clone(),
    );
    let session = Arc::new(Mutex::new(session));
    let slots = Arc::new(Semaphore::new(MAX_IN_FLIGHT_PER_TRADER));
    let mut trades = JoinSet::new();
    let tier_secs = trader.tier.interval_secs(&config);
    let base_interval = Duration::from_secs(tier_secs);
    let initial_delay = Duration::from_secs(rand::random_range(0..=tier_secs));
    let mut next_schedule = tokio::time::Instant::now() + initial_delay;
    loop {
        let sleep = tokio::time::sleep_until(next_schedule);
        tokio::pin!(sleep);
        tokio::select! {
            _ = &mut sleep => {
                next_schedule += jittered_interval(base_interval, config.jitter);
                metrics.record_schedule();
                let Ok(permit) = slots.clone().try_acquire_owned() else {
                    metrics.record_skipped_schedule();
                    continue;
                };
                metrics.order_started();
                let trader = trader.clone();
                let config = config.clone();
                let deployment = deployment.clone();
                let api = api.clone();
                let oracle = oracle.clone();
                let metrics = metrics.clone();
                let session = session.clone();
                let tracker = tracker.clone();
                trades.spawn(async move {
                    let started = Instant::now();
                    let result = trade_once(
                        &trader,
                        &config,
                        &deployment,
                        &api,
                        &oracle,
                        &metrics,
                        &session,
                        &tracker,
                        started,
                    )
                    .await;
                    if let Err(error) = result {
                        metrics.record_cycle_failure(
                            trader.index,
                            trader.tier,
                            started.elapsed(),
                        );
                        debug!(trader = trader.index, %error, "trade attempt failed");
                    }
                    metrics.order_finished();
                    drop(permit);
                });
            }
            Some(joined) = trades.join_next(), if !trades.is_empty() => {
                if let Err(error) = joined {
                    metrics.order_finished();
                    warn!(trader = trader.index, %error, "trade task failed");
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    trades.abort_all();
}

#[allow(clippy::too_many_arguments)]
async fn trade_once(
    trader: &Trader,
    config: &Config,
    deployment: &Deployment,
    api: &SimulationApi,
    oracle: &OracleClient,
    metrics: &Metrics,
    session: &Arc<Mutex<Session>>,
    tracker: &ws::SettlementTracker,
    cycle_started: Instant,
) -> Result<()> {
    let sell_index = rand::random_range(0..deployment.assets.len());
    let mut buy_index = rand::random_range(0..deployment.assets.len() - 1);
    if buy_index >= sell_index {
        buy_index += 1;
    }
    let sell = &deployment.assets[sell_index];
    let buy = &deployment.assets[buy_index];
    let (prices, oracle_latency) = oracle.prices(sell, buy).await?;
    let min_amount_out = minimum_amount_out(
        config.trade_amount,
        sell.decimals,
        buy.decimals,
        prices,
        config.slippage_bps,
    )?;

    let now = u64::try_from(Utc::now().timestamp()).context("system clock is before Unix epoch")?;
    let (current_session, auth_timing, refreshed_token) = {
        let mut current = session.lock().await;
        if current.needs_refresh(now) {
            let (new_session, timing) = api.authenticate(trader.user_id, &trader.key).await?;
            let token = new_session.access_token.clone();
            *current = new_session;
            (current.clone(), Some(timing), Some(token))
        } else {
            (current.clone(), None, None)
        }
    };
    if let Some(token) = refreshed_token {
        tracker.update_token(token).await;
    }

    let client_order_id = Uuid::new_v4();
    let expires_at = now.saturating_add(config.intent_ttl_secs);
    let intent = Intent::new_swap(
        trader.user_id,
        sell.faucet_id,
        config.trade_amount,
        buy.faucet_id,
        min_amount_out,
        client_order_id,
        expires_at,
    );
    let response = api
        .submit_order(
            &current_session,
            trader.user_id,
            trader.key.public_key(),
            trader.key.sign(intent.message_word()),
            client_order_id,
            expires_at,
            OrderDetails::new(
                sell.faucet_id,
                config.trade_amount,
                buy.faucet_id,
                min_amount_out,
            ),
        )
        .await?;
    metrics.record_submitted();

    let mut outcome = response.outcome;
    let mut settle_latency = None;
    if outcome == OrderOutcome::RateLimited {
        debug!(
            trader = trader.index,
            status = %response.status,
            body = %response.body,
            "execution queue full or rate limited"
        );
    } else if outcome == OrderOutcome::Accepted {
        match response.order_id {
            Some(order_id) => {
                let wait_started = Instant::now();
                match tracker.track(order_id).await {
                    Ok(TerminalOrderStatus::Confirmed) => {
                        outcome = OrderOutcome::Confirmed;
                        settle_latency = Some(wait_started.elapsed());
                    }
                    Ok(TerminalOrderStatus::Failed) => {
                        outcome = OrderOutcome::ExecutionFailed;
                        settle_latency = Some(wait_started.elapsed());
                        debug!(
                            trader = trader.index,
                            %order_id,
                            "order failed after admit"
                        );
                    }
                    Err(error) => {
                        outcome = OrderOutcome::TimedOut;
                        settle_latency = Some(wait_started.elapsed());
                        debug!(
                            trader = trader.index,
                            %order_id,
                            %error,
                            "order websocket wait failed"
                        );
                    }
                }
            }
            None => {
                debug!(
                    trader = trader.index,
                    body = %response.body,
                    "admit succeeded but order id missing; counting as accepted only"
                );
            }
        }
    } else {
        debug!(
            trader = trader.index,
            status = %response.status,
            body = %response.body,
            "order was not accepted"
        );
    }
    debug!(
        trader = trader.index,
        tier = trader.tier.label(),
        pair = %format!("{}/{}", sell.symbol, buy.symbol),
        outcome = ?outcome,
        oracle_ms = oracle_latency.as_millis(),
        auth_ms = auth_timing.map(|timing| timing.http.as_millis()),
        auth_wait_ms = auth_timing.map(|timing| timing.wait.as_millis()),
        order_ms = response.latency.as_millis(),
        settle_ms = settle_latency.map(|d| d.as_millis()),
        cycle_ms = cycle_started.elapsed().as_millis(),
        "trade completed"
    );
    metrics.record_trade(TradeMeasurement {
        trader_index: trader.index,
        tier: trader.tier,
        outcome,
        oracle: oracle_latency,
        auth: auth_timing.map(|timing| timing.http),
        auth_wait: auth_timing
            .map(|timing| timing.wait)
            .filter(|wait| !wait.is_zero()),
        order: response.latency,
        settle: settle_latency,
        cycle: cycle_started.elapsed(),
    });
    Ok(())
}

pub fn activation_stage_size(start: usize, max: usize) -> usize {
    max.saturating_sub(start).div_ceil(8).max(1)
}

pub async fn run_activation_ramp(
    config: Arc<Config>,
    deployment: Arc<Deployment>,
    api: SimulationApi,
    oracle: OracleClient,
    metrics: Metrics,
    staged: Vec<Trader>,
    mut shutdown: watch::Receiver<bool>,
) {
    let stage_size = activation_stage_size(config.num_traders, config.max_traders);
    let mut staged = VecDeque::from(staged);
    let mut active = config.num_traders;
    let mut stages = tokio::time::interval(Duration::from_secs(60));
    stages.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    stages.tick().await;
    let mut traders = JoinSet::new();
    let mut activation_stopped = false;

    loop {
        tokio::select! {
            _ = stages.tick(), if !staged.is_empty() && !activation_stopped => {
                if metrics.is_saturated() {
                    activation_stopped = true;
                    warn!(
                        active,
                        remaining = staged.len(),
                        "load saturated; stopping trader activation"
                    );
                    continue;
                }
                let count = stage_size.min(staged.len());
                let batch = staged.drain(..count).collect::<Vec<_>>();
                let mut activated = 0;
                let mut retry = Vec::new();
                let mut ready = Vec::new();
                for trader in batch {
                    match api.authenticate(trader.user_id, &trader.key).await {
                        Ok((session, _)) => {
                            ready.push((trader, session));
                            activated += 1;
                        }
                        Err(error) => {
                            warn!(%error, "failed to authenticate staged trader");
                            retry.push(trader);
                        }
                    }
                }
                staged.extend(retry);
                for (trader, session) in ready {
                    traders.spawn(run_trader(
                        trader,
                        config.clone(),
                        deployment.clone(),
                        api.clone(),
                        oracle.clone(),
                        metrics.clone(),
                        Some(session),
                        shutdown.clone(),
                    ));
                }
                active = active.saturating_add(activated);
                metrics.set_active_traders(active);
                info!(
                    active,
                    activated,
                    remaining = staged.len(),
                    "activated trader stage"
                );
            }
            Some(result) = traders.join_next(), if !traders.is_empty() => {
                if let Err(error) = result {
                    warn!(%error, "activated trader task failed");
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    traders.abort_all();
}

/// Live-onboard traders until `max_traders`, spawning a trade loop for each.
///
/// Starts a new onboard attempt every `grow_interval_secs` without waiting for the previous
/// one to finish (faucet/prove still serialize via shared semaphores).
pub async fn run_growth_loop(
    config: Arc<Config>,
    api: SimulationApi,
    oracle: OracleClient,
    metrics: Metrics,
    registry: TraderRegistry,
    prove_slots: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
) {
    if !config.should_grow() {
        return;
    }
    let mut grow = tokio::time::interval(Duration::from_secs(config.grow_interval_secs));
    grow.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    grow.tick().await;

    let mut next_index = {
        let guard = registry.lock().await;
        guard
            .iter()
            .map(|trader| trader.index)
            .max()
            .map(|max| max + 1)
            .unwrap_or(0)
    };
    let mut in_flight = 0_usize;
    let mut onboard_tasks: JoinSet<(
        usize,
        Instant,
        Result<Result<Trader, anyhow::Error>, tokio::task::JoinError>,
    )> = JoinSet::new();
    let mut child_tasks = JoinSet::new();

    loop {
        tokio::select! {
            _ = grow.tick() => {
                if *shutdown.borrow() {
                    break;
                }
                let live = registry.lock().await.len();
                let projected = live.saturating_add(in_flight);
                if projected >= config.max_traders {
                    if in_flight == 0 {
                        info!(
                            current = live,
                            max = config.max_traders,
                            "trader growth reached cap"
                        );
                        break;
                    }
                    continue;
                }
                let index = next_index;
                next_index = next_index.saturating_add(1);
                in_flight = in_flight.saturating_add(1);
                info!(
                    trader = index,
                    current = live,
                    in_flight,
                    "starting live trader onboarding"
                );
                let onboard_started = Instant::now();
                let onboard_config = config.clone();
                let onboard_api = api.clone();
                let onboard_metrics = metrics.clone();
                let onboard_slots = prove_slots.clone();
                onboard_tasks.spawn(async move {
                    let result = tokio::task::spawn_blocking(move || {
                        let runtime = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .context("build live-onboard runtime")?;
                        runtime.block_on(onboard_live_trader(
                            &onboard_config,
                            &onboard_api,
                            &onboard_metrics,
                            &onboard_slots,
                            index,
                        ))
                    })
                    .await;
                    (index, onboard_started, result)
                });
            }
            Some(joined) = onboard_tasks.join_next() => {
                in_flight = in_flight.saturating_sub(1);
                let Ok((index, onboard_started, onboard)) = joined else {
                    error!("live onboard joinset task panicked");
                    continue;
                };
                match onboard {
                    Ok(Ok(trader)) => {
                        info!(
                            trader = trader.index,
                            user = %trader.user_id.to_hex(),
                            elapsed_ms = onboard_started.elapsed().as_millis(),
                            "live trader onboarded"
                        );
                        registry.lock().await.push(trader.clone());
                        match api.authenticate(trader.user_id, &trader.key).await {
                            Ok((session, _)) => {
                                let deployment = match Deployment::load() {
                                    Ok(deployment) => Arc::new(deployment),
                                    Err(error) => {
                                        error!(%error, "failed to reload deployment after growth");
                                        continue;
                                    }
                                };
                                child_tasks.spawn(run_trader(
                                    trader,
                                    config.clone(),
                                    deployment,
                                    api.clone(),
                                    oracle.clone(),
                                    metrics.clone(),
                                    Some(session),
                                    shutdown.clone(),
                                ));
                            }
                            Err(error) => {
                                error!(%error, "auth failed for live-onboarded trader");
                            }
                        }
                    }
                    Ok(Err(error)) => {
                        error!(
                            trader = index,
                            elapsed_ms = onboard_started.elapsed().as_millis(),
                            error = %format!("{error:#}"),
                            "live trader onboarding failed"
                        );
                    }
                    Err(error) => {
                        error!(
                            trader = index,
                            elapsed_ms = onboard_started.elapsed().as_millis(),
                            error = %format!("{error:#}"),
                            "live trader onboarding task failed"
                        );
                    }
                }
            }
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
    // Wait for shutdown so grown traders keep running until the sim ends.
    while !*shutdown.borrow() {
        if shutdown.changed().await.is_err() {
            break;
        }
    }
    while let Some(result) = child_tasks.join_next().await {
        if let Err(error) = result {
            error!(%error, "grown trader task failed");
        }
    }
}

/// Periodically run fund → init_redeem → redeem on a random live trader and assert balances.
pub async fn run_vault_cycle_loop(
    config: Arc<Config>,
    api: SimulationApi,
    metrics: Metrics,
    registry: TraderRegistry,
    prove_slots: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
) {
    if config.vault_cycle_interval_secs == 0 {
        return;
    }
    let mut ticker = tokio::time::interval(Duration::from_secs(config.vault_cycle_interval_secs));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;
    loop {
        tokio::select! {
            _ = ticker.tick() => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
                continue;
            }
        }
        if *shutdown.borrow() {
            break;
        }
        let traders = registry.lock().await.clone();
        if traders.is_empty() {
            continue;
        }
        let trader = traders[rand::random_range(0..traders.len())].clone();
        let deployment = match Deployment::load() {
            Ok(d) => d,
            Err(error) => {
                error!(%error, "vault cycle: failed to load deployment");
                metrics.record_vault_cycle(false);
                continue;
            }
        };
        if deployment.assets.is_empty() {
            error!("vault cycle: deployment has no assets");
            metrics.record_vault_cycle(false);
            continue;
        }
        let asset = deployment.assets[rand::random_range(0..deployment.assets.len())].clone();
        let cycle_config = config.clone();
        let cycle_api = api.clone();
        let cycle_trader = trader.clone();
        let cycle_slots = prove_slots.clone();
        let asset_symbol = asset.symbol.clone();
        let cycle = tokio::task::spawn_blocking(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .context("build vault-cycle runtime")?;
            runtime.block_on(run_vault_cycle_once(
                &cycle_config,
                &cycle_api,
                &deployment,
                &cycle_trader,
                &asset,
                &cycle_slots,
            ))
        })
        .await;
        match cycle {
            Ok(Ok(())) => {
                info!(
                    trader = trader.index,
                    asset = %asset_symbol,
                    amount = config.vault_cycle_amount,
                    "vault cycle ok"
                );
                metrics.record_vault_cycle(true);
            }
            Ok(Err(error)) => {
                error!(
                    trader = trader.index,
                    asset = %asset_symbol,
                    %error,
                    "vault cycle failed"
                );
                metrics.record_vault_cycle(false);
            }
            Err(error) => {
                error!(
                    trader = trader.index,
                    asset = %asset_symbol,
                    %error,
                    "vault cycle task failed"
                );
                metrics.record_vault_cycle(false);
            }
        }
    }
}

async fn onboard_live_trader(
    config: &Config,
    api: &SimulationApi,
    metrics: &Metrics,
    prove_slots: &Arc<Semaphore>,
    index: usize,
) -> Result<Trader> {
    let wave = index / config.max_users_per_pool;
    // Pool deploy / ACTIVE_POOL / deployment.save all use the shared operator store.
    // Reload under the lock so concurrent onboards see each other's new pools.
    let deployment = {
        let _shared = LIVE_ONBOARD_SHARED.lock().await;
        let mut deployment = Deployment::load().context("load deployment for live onboard")?;
        info!(trader = index, "live onboard: ensuring pool shards");
        ensure_pool_shards(config, &mut deployment, index + 1)
            .await
            .with_context(|| format!("ensure pool shards for live trader {index}"))?;
        info!(trader = index, wave, "live onboard: activating pool wave");
        activate_pool_wave(&deployment, wave)
            .await
            .with_context(|| format!("activate pool wave {wave} for live trader {index}"))?;
        deployment
    };

    // Isolated worker store only — never open the shared main sim store (it accumulates every
    // prior trader and can hang/fail note sync under the NTL tag cap).
    let store_path = worker_store_path(&config.store_path, index);
    remove_sqlite_store(&store_path)?;
    let keystore = FilesystemKeyStore::new(config.keystore_dir.clone())?;
    info!(trader = index, store = %store_path.display(), "live onboard: building worker client");
    let mut worker = build_worker_client(&config.keystore_dir, &store_path).await?;
    worker
        .import_account_by_id(deployment.vault_id)
        .await
        .with_context(|| {
            format!(
                "import vault {} for live trader {index}",
                deployment.vault_id.to_hex()
            )
        })?;

    let tier = TraderTier::HighFrequency;
    let wallet = {
        // Keystore is a shared directory; serialize creates so we don't race key files.
        let _shared = LIVE_ONBOARD_SHARED.lock().await;
        create_wallet(&mut worker, &keystore)
            .await
            .with_context(|| format!("create wallet for live trader {index}"))?
    };
    let trader = Trader {
        index,
        tier,
        user_id: wallet.user_id,
        key: wallet.key,
    };
    info!(
        trader = index,
        user = %trader.user_id.to_hex(),
        "live onboard: wallet created; minting"
    );

    for asset in &deployment.assets {
        info!(
            trader = index,
            asset = %asset.symbol,
            "live onboard: requesting faucet mint"
        );
        let latency = api
            .mint(trader.user_id, asset.faucet_id)
            .await
            .with_context(|| format!("live onboard mint {} for trader {index}", asset.symbol))?;
        metrics.record_setup(trader.index, trader.tier, SetupPhase::Mint, latency);
        info!(
            trader = index,
            asset = %asset.symbol,
            mint_ms = latency.as_millis(),
            "live onboard: mint completed"
        );
    }

    // Consume mint notes without holding a prove slot (can take minutes).
    info!(trader = index, "live onboard: waiting for mint notes");
    ensure_wallet_balances(
        &mut worker,
        trader.index,
        trader.user_id,
        &deployment.assets,
        config.fund_amount,
    )
    .await
    .with_context(|| format!("consume/fund wallet for live trader {index}"))?;

    let fund_assets = deployment
        .assets
        .iter()
        .map(|asset| {
            FungibleAsset::new(asset.faucet_id, config.fund_amount)
                .map_err(|error| anyhow!("invalid funding asset {}: {error:?}", asset.symbol))
        })
        .collect::<Result<Vec<_>>>()?;
    let pubkey = trader.key.public_key().to_commitment().into();

    info!(
        trader = index,
        "live onboard: acquiring prove slot for register/fund"
    );
    let started = Instant::now();
    {
        let _slot = prove_slots
            .acquire()
            .await
            .map_err(|_| anyhow!("prove slot closed for live trader {index}"))?;
        register_and_fund_user_on_vault(
            &mut worker,
            deployment.vault_id,
            trader.user_id,
            pubkey,
            &fund_assets,
        )
        .await
        .with_context(|| format!("register/fund live trader {index} on vault"))?;
    }
    let elapsed = started.elapsed();
    metrics.record_setup(trader.index, trader.tier, SetupPhase::Register, elapsed);
    metrics.record_setup(trader.index, trader.tier, SetupPhase::Fund, elapsed);
    info!(
        trader = index,
        register_ms = elapsed.as_millis(),
        "live onboard: registered and funded"
    );

    {
        let _shared = LIVE_ONBOARD_SHARED.lock().await;
        append_trader_state(config, &deployment, &trader)
            .with_context(|| format!("append state for live trader {index}"))?;
    }
    Ok(trader)
}

/// Append one trader to the resumable state file without reloading keys for everyone.
fn append_trader_state(config: &Config, deployment: &Deployment, trader: &Trader) -> Result<()> {
    let mut state = if config.state_file.exists() {
        let contents = std::fs::read_to_string(&config.state_file)
            .with_context(|| format!("read state file {}", config.state_file.display()))?;
        let state: SimulationState = serde_json::from_str(&contents)
            .with_context(|| format!("parse state file {}", config.state_file.display()))?;
        let network = MidenNetwork::from_env();
        if state.network != network.as_str() || state.vault_id != deployment.vault_id.to_hex() {
            bail!("state file belongs to a different network or vault");
        }
        state
    } else {
        SimulationState {
            network: MidenNetwork::from_env().as_str().to_owned(),
            vault_id: deployment.vault_id.to_hex(),
            setup_complete: true,
            traders: Vec::new(),
        }
    };
    if !state
        .traders
        .iter()
        .any(|saved| saved.index == trader.index)
    {
        state.traders.push(TraderState {
            index: trader.index,
            user_id: trader.user_id.to_hex(),
            public_key_commitment: Word::from(trader.key.public_key().to_commitment()).to_hex(),
        });
        state.traders.sort_by_key(|saved| saved.index);
    }
    if let Some(parent) = config
        .state_file
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(
        &config.state_file,
        serde_json::to_vec_pretty(&state).context("serialize simulation state")?,
    )
    .with_context(|| format!("write state file {}", config.state_file.display()))
}

async fn run_vault_cycle_once(
    config: &Config,
    api: &SimulationApi,
    deployment: &Deployment,
    trader: &Trader,
    asset: &AssetInfo,
    prove_slots: &Arc<Semaphore>,
) -> Result<()> {
    let amount = config.vault_cycle_amount;
    let store_path = worker_store_path(&config.store_path, trader.index);
    let mut client = build_worker_client(&config.keystore_dir, &store_path).await?;
    // Import account if the worker store is cold.
    if client.try_get_account(trader.user_id).await.is_err() {
        let (main, _) = build_simulation_client(config).await?;
        let account = main.try_get_account(trader.user_id).await?;
        client.add_account(&account, false).await?;
    }
    // Fresh worker stores do not track the vault; counters need a local copy.
    if client.try_get_account(deployment.vault_id).await.is_err() {
        client
            .import_account_by_id(deployment.vault_id)
            .await
            .with_context(|| {
                format!(
                    "import vault {} into worker store for trader {}",
                    deployment.vault_id.to_hex(),
                    trader.index
                )
            })?;
    }
    sync_for_notes(&mut client).await?;

    // Ensure wallet has funds for FUND.
    let wallet_before = client
        .account_reader(trader.user_id)
        .get_balance(asset.faucet_id)
        .await?;
    if wallet_before < amount {
        api.mint(trader.user_id, asset.faucet_id).await?;
        ensure_wallet_balances(
            &mut client,
            trader.index,
            trader.user_id,
            std::slice::from_ref(asset),
            amount,
        )
        .await?;
    }
    let wallet_before = client
        .account_reader(trader.user_id)
        .get_balance(asset.faucet_id)
        .await?;
    let vault_before = get_vault_user_asset_info(
        &client,
        deployment.vault_id,
        asset.faucet_id,
        trader.user_id,
    )
    .await?;

    let fund_asset = FungibleAsset::new(asset.faucet_id, amount)
        .map_err(|error| anyhow!("invalid vault cycle asset: {error:?}"))?;
    {
        let _slot = prove_slots
            .acquire()
            .await
            .map_err(|_| anyhow!("prove slot closed for vault fund"))?;
        fund_user_on_vault(&mut client, deployment.vault_id, trader.user_id, fund_asset).await?;
    }
    sync_for_notes(&mut client).await?;
    let vault_after_fund = get_vault_user_asset_info(
        &client,
        deployment.vault_id,
        asset.faucet_id,
        trader.user_id,
    )
    .await?;
    let wallet_after_fund = client
        .account_reader(trader.user_id)
        .get_balance(asset.faucet_id)
        .await?;
    if vault_after_fund.total_funding != vault_before.total_funding.saturating_add(amount) {
        bail!(
            "funding counter mismatch: before={} after={} expected_delta={amount}",
            vault_before.total_funding,
            vault_after_fund.total_funding
        );
    }
    if wallet_after_fund + amount != wallet_before {
        bail!(
            "wallet after fund mismatch: before={wallet_before} after={wallet_after_fund} amount={amount}"
        );
    }

    {
        let _slot = prove_slots
            .acquire()
            .await
            .map_err(|_| anyhow!("prove slot closed for vault init_redeem"))?;
        init_redeem_on_vault(&mut client, deployment.vault_id, trader.user_id, fund_asset).await?;
    }
    sync_for_notes(&mut client).await?;
    let vault_after_init = get_vault_user_asset_info(
        &client,
        deployment.vault_id,
        asset.faucet_id,
        trader.user_id,
    )
    .await?;
    if vault_after_init.total_initiated_redeems
        != vault_after_fund
            .total_initiated_redeems
            .saturating_add(amount)
    {
        bail!(
            "init_redeem counter mismatch: before={} after={}",
            vault_after_fund.total_initiated_redeems,
            vault_after_init.total_initiated_redeems
        );
    }
    if vault_after_init.pending_redeem() != vault_after_fund.pending_redeem().saturating_add(amount)
    {
        bail!(
            "pending_redeem mismatch after init: before={} after={}",
            vault_after_fund.pending_redeem(),
            vault_after_init.pending_redeem()
        );
    }

    {
        let _slot = prove_slots
            .acquire()
            .await
            .map_err(|_| anyhow!("prove slot closed for vault redeem"))?;
        redeem_on_vault(&mut client, deployment.vault_id, trader.user_id, fund_asset).await?;
    }
    sync_for_notes(&mut client).await?;
    let vault_after_redeem = get_vault_user_asset_info(
        &client,
        deployment.vault_id,
        asset.faucet_id,
        trader.user_id,
    )
    .await?;
    if vault_after_redeem.total_redeems != vault_after_init.total_redeems.saturating_add(amount) {
        bail!(
            "redeem counter mismatch: before={} after={}",
            vault_after_init.total_redeems,
            vault_after_redeem.total_redeems
        );
    }
    let expected_pending = vault_after_init.pending_redeem().saturating_sub(amount);
    if vault_after_redeem.pending_redeem() != expected_pending {
        bail!(
            "pending_redeem mismatch after redeem: before={} after={} expected={}",
            vault_after_init.pending_redeem(),
            vault_after_redeem.pending_redeem(),
            expected_pending
        );
    }

    // Consume P2ID payout; wallet should return to the pre-fund balance.
    // Do not hold a prove slot while waiting for the note to appear.
    let consume_deadline = Instant::now() + Duration::from_secs(120);
    let mut consumed = false;
    while Instant::now() < consume_deadline {
        let _ = sync_for_notes(&mut client).await;
        if client
            .get_consumable_notes(Some(trader.user_id))
            .await?
            .is_empty()
        {
            tokio::time::sleep(Duration::from_secs(1)).await;
            continue;
        }
        {
            let _slot = prove_slots
                .acquire()
                .await
                .map_err(|_| anyhow!("prove slot closed for vault payout consume"))?;
            consume_all_notes_for(&mut client, trader.user_id).await?;
        }
        consumed = true;
        break;
    }
    if !consumed {
        bail!("timed out waiting for redeem P2ID payout note");
    }
    let _ = sync_for_notes(&mut client).await;
    let wallet_final = client
        .account_reader(trader.user_id)
        .get_balance(asset.faucet_id)
        .await?;
    if wallet_final != wallet_before {
        bail!(
            "wallet after redeem/consume mismatch: before_fund={wallet_before} \
             after_fund={wallet_after_fund} final={wallet_final} amount={amount}"
        );
    }
    Ok(())
}

fn save_state(
    config: &Config,
    deployment: &Deployment,
    traders: &[Trader],
    setup_complete: bool,
) -> Result<()> {
    let state = SimulationState {
        network: MidenNetwork::from_env().as_str().to_owned(),
        vault_id: deployment.vault_id.to_hex(),
        setup_complete,
        traders: traders
            .iter()
            .map(|trader| TraderState {
                index: trader.index,
                user_id: trader.user_id.to_hex(),
                public_key_commitment: Word::from(trader.key.public_key().to_commitment()).to_hex(),
            })
            .collect(),
    };
    if let Some(parent) = config
        .state_file
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(
        &config.state_file,
        serde_json::to_vec_pretty(&state).context("serialize simulation state")?,
    )
    .with_context(|| format!("write state file {}", config.state_file.display()))
}

pub fn validate_deployment(deployment: &Deployment) -> Result<()> {
    if deployment.network != "testnet" || MidenNetwork::from_env() != MidenNetwork::Testnet {
        bail!("signed v2 intents currently require MIDEN_NETWORK=testnet");
    }
    if deployment.assets.len() < 2 {
        bail!("simulation requires at least two deployment assets");
    }
    Ok(())
}

#[cfg(test)]
mod load_tests {
    use std::sync::Arc;

    use tokio::sync::Semaphore;

    use super::{MAX_IN_FLIGHT_PER_TRADER, SimulationState, activation_stage_size};

    #[test]
    fn derives_eight_stable_activation_stages() {
        assert_eq!(activation_stage_size(20, 100), 10);
        assert_eq!(activation_stage_size(20, 21), 1);
        assert_eq!(activation_stage_size(100, 100), 1);
    }

    #[tokio::test]
    async fn trader_in_flight_limit_skips_a_third_order() {
        let slots = Arc::new(Semaphore::new(MAX_IN_FLIGHT_PER_TRADER));
        let _first = slots.clone().try_acquire_owned().unwrap();
        let _second = slots.clone().try_acquire_owned().unwrap();
        assert!(slots.try_acquire_owned().is_err());
    }

    #[test]
    fn legacy_state_is_not_assumed_complete() {
        let state: SimulationState =
            serde_json::from_str(r#"{"network":"testnet","vault_id":"0x01","traders":[]}"#)
                .unwrap();
        assert!(!state.setup_complete);
    }
}
