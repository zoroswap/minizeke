use std::{
    path::PathBuf,
    sync::Arc,
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
    sync::{Semaphore, mpsc, watch},
    task::JoinSet,
};
use tracing::{info, warn};
use uuid::Uuid;

use crate::{
    deployment::{AssetInfo, Deployment},
    intent::Intent,
    miden_env::MidenNetwork,
    order::OrderDetails,
    simulate::{
        api::{OrderOutcome, Session, SimulationApi},
        config::{Config, TraderTier},
        metrics::{Metrics, SetupPhase, TradeMeasurement, jittered_interval},
        oracle::{OracleClient, minimum_amount_out},
    },
    test_utils::{consume_all_notes_for_setup, register_and_fund_user_on_vault},
};

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
        info!(
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
}

#[derive(Debug, Serialize, Deserialize)]
struct SimulationState {
    network: String,
    vault_id: String,
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
    let mut builder = MidenNetwork::client_builder()
        .in_debug_mode(true.into())
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
    remove_path_if_exists(&config.state_file)?;
    remove_sqlite_store(&config.store_path)?;
    remove_worker_stores(&config.store_path)?;
    Ok(())
}

fn remove_worker_stores(base: &PathBuf) -> Result<()> {
    let file_name = base
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("simulate.store.sqlite3");
    let prefix = format!("{file_name}.setup.");
    let parent = base.parent().filter(|path| !path.as_os_str().is_empty());
    let dir = parent.unwrap_or_else(|| std::path::Path::new("."));
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(error).with_context(|| format!("read {}", dir.display()));
        }
    };
    for entry in entries {
        let entry = entry.with_context(|| format!("read entry in {}", dir.display()))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.starts_with(&prefix) {
            remove_sqlite_store(&entry.path())?;
        }
    }
    Ok(())
}

fn remove_path_if_exists(path: &PathBuf) -> Result<()> {
    if path.exists() {
        std::fs::remove_file(path)
            .with_context(|| format!("remove {}", path.display()))?;
        info!(path = %path.display(), "removed previous setup artifact");
    }
    Ok(())
}

fn remove_sqlite_store(path: &PathBuf) -> Result<()> {
    remove_path_if_exists(path)?;
    for suffix in ["-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        remove_path_if_exists(&PathBuf::from(sidecar))?;
    }
    Ok(())
}

pub async fn setup_traders(
    config: &Config,
    deployment: &Deployment,
    api: &SimulationApi,
    metrics: &Metrics,
    client: &mut Client<FilesystemKeyStore>,
    keystore: &FilesystemKeyStore,
) -> Result<Vec<Trader>> {
    let tiers = config.tier_assignments();
    info!(traders = config.num_traders, "creating trader wallets");

    let mut traders = Vec::with_capacity(config.num_traders);
    for (index, tier) in tiers.into_iter().enumerate() {
        let wallet = create_wallet(client, keystore).await?;
        info!(
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
    save_state(config, deployment, &traders)?;

    // Workers must add_account (and thus track note tags) *before* mint notes are
    // committed. Fresh stores created after mint sync at tip with notes=0 forever.
    let concurrency = config.setup_concurrency.min(traders.len()).max(1);
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
        });
    }

    info!(
        traders = traders.len(),
        concurrency,
        "preparing worker clients before mint"
    );
    let (mint_done_tx, mint_done_rx) = watch::channel(false);
    let (ready_tx, mut ready_rx) = mpsc::channel(traders.len());
    let prove_slots = Arc::new(Semaphore::new(concurrency));
    let mut set = JoinSet::new();
    let expected = jobs.len();
    for job in jobs {
        let ready_tx = ready_tx.clone();
        let mint_done_rx = mint_done_rx.clone();
        let prove_slots = prove_slots.clone();
        set.spawn_blocking(move || run_setup_job(job, ready_tx, mint_done_rx, prove_slots));
    }
    drop(ready_tx);

    for prepared in 1..=expected {
        ready_rx
            .recv()
            .await
            .ok_or_else(|| anyhow!("setup worker exited before signaling ready"))?;
        if prepared == expected || prepared % 5 == 0 {
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

    while let Some(joined) = set.join_next().await {
        joined.map_err(|error| anyhow!("setup worker join failed: {error}"))??;
    }

    save_state(config, deployment, &traders)?;
    info!(traders = traders.len(), "parallel trader setup complete");
    Ok(traders)
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
            info!(
                asset = %symbol,
                recipients = recipients.len(),
                "minting to traders via faucet batch"
            );
            let mint_futures = recipients.into_iter().map(|(index, tier, user_id)| {
                let api = api.clone();
                async move { (index, tier, api.mint(user_id, faucet_id).await) }
            });
            let results = futures_util::future::join_all(mint_futures).await;
            for (index, tier, result) in results {
                let latency =
                    result.with_context(|| format!("mint {symbol} to trader {index}"))?;
                metrics.record_setup(index, tier, SetupPhase::Mint, latency);
            }
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
    prove_slots: Arc<Semaphore>,
) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build setup worker runtime")?;
    runtime.block_on(onboard_trader(job, ready_tx, mint_done_rx, prove_slots))
}

async fn onboard_trader(
    job: SetupJob,
    ready_tx: mpsc::Sender<usize>,
    mut mint_done_rx: watch::Receiver<bool>,
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
    } = job;

    let mut client = build_worker_client(&keystore_dir, &store_path).await?;
    client.add_account(&account, false).await?;
    client.sync_state().await?;
    ready_tx
        .send(index)
        .await
        .map_err(|_| anyhow!("setup coordinator dropped while trader {index} was preparing"))?;

    mint_done_rx
        .wait_for(|done| *done)
        .await
        .map_err(|_| anyhow!("mint signal closed before trader {index} started consume"))?;

    let _prove_slot = prove_slots
        .acquire()
        .await
        .map_err(|_| anyhow!("prove slot closed for trader {index}"))?;

    ensure_wallet_balances(&mut client, index, user_id, &assets, fund_amount)
        .await
        .with_context(|| format!("fund wallet for trader {index}"))?;

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

    let started = Instant::now();
    register_and_fund_user_on_vault(&mut client, vault_id, user_id, pubkey, &fund_assets)
        .await
        .with_context(|| format!("register/fund trader {index} on vault"))?;
    let elapsed = started.elapsed();
    metrics.record_setup(index, tier, SetupPhase::Register, elapsed);
    metrics.record_setup(index, tier, SetupPhase::Fund, elapsed);

    info!(
        trader = index,
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
        client.sync_state().await?;
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

async fn build_worker_client(
    keystore_dir: &PathBuf,
    store_path: &PathBuf,
) -> Result<Client<FilesystemKeyStore>> {
    let network = MidenNetwork::from_env();
    if let Some(parent) = store_path.parent().filter(|path| !path.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)?;
    }
    let store = Arc::new(SqliteStore::new(store_path.clone()).await?);
    let keystore = Arc::new(FilesystemKeyStore::new(keystore_dir.clone())?);
    let mut builder = MidenNetwork::client_builder()
        .in_debug_mode(true.into())
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
    if state.traders.len() < config.num_traders {
        bail!(
            "state file contains {} traders, but {} were requested",
            state.traders.len(),
            config.num_traders
        );
    }

    let tiers = config.tier_assignments();
    let mut traders = Vec::with_capacity(config.num_traders);
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

pub async fn run_trader(
    trader: Trader,
    config: Arc<Config>,
    deployment: Arc<Deployment>,
    api: SimulationApi,
    oracle: OracleClient,
    metrics: Metrics,
    mut session: Option<Session>,
    mut shutdown: watch::Receiver<bool>,
) {
    let stagger = Duration::from_millis(
        (trader.index as u64).saturating_mul(config.start_stagger_ms),
    );
    if !stagger.is_zero() {
        tokio::select! {
            _ = tokio::time::sleep(stagger) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    return;
                }
            }
        }
    }
    loop {
        if *shutdown.borrow() {
            break;
        }
        let cycle_started = Instant::now();
        if let Err(error) = trade_once(
            &trader,
            &config,
            &deployment,
            &api,
            &oracle,
            &metrics,
            &mut session,
            cycle_started,
        )
        .await
        {
            metrics.record_cycle_failure(trader.index, trader.tier, cycle_started.elapsed());
            warn!(
                trader = trader.index,
                tier = trader.tier.label(),
                %error,
                "trade cycle failed"
            );
        }

        let delay = jittered_interval(
            Duration::from_secs(trader.tier.interval_secs(&config)),
            config.jitter,
        );
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn trade_once(
    trader: &Trader,
    config: &Config,
    deployment: &Deployment,
    api: &SimulationApi,
    oracle: &OracleClient,
    metrics: &Metrics,
    session: &mut Option<Session>,
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
    let auth_timing = if session
        .as_ref()
        .is_none_or(|current| current.needs_refresh(now))
    {
        let (new_session, timing) = api.authenticate(trader.user_id, &trader.key).await?;
        *session = Some(new_session);
        Some(timing)
    } else {
        None
    };

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
            session.as_ref().expect("session was initialized"),
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
    if response.outcome != OrderOutcome::Accepted {
        warn!(
            trader = trader.index,
            status = %response.status,
            body = %response.body,
            "order was not accepted"
        );
    }
    info!(
        trader = trader.index,
        tier = trader.tier.label(),
        pair = %format!("{}/{}", sell.symbol, buy.symbol),
        outcome = ?response.outcome,
        oracle_ms = oracle_latency.as_millis(),
        auth_ms = auth_timing.map(|timing| timing.http.as_millis()),
        auth_wait_ms = auth_timing.map(|timing| timing.wait.as_millis()),
        order_ms = response.latency.as_millis(),
        cycle_ms = cycle_started.elapsed().as_millis(),
        "trade completed"
    );
    metrics.record_trade(TradeMeasurement {
        trader_index: trader.index,
        tier: trader.tier,
        outcome: response.outcome,
        oracle: oracle_latency,
        auth: auth_timing.map(|timing| timing.http),
        auth_wait: auth_timing
            .map(|timing| timing.wait)
            .filter(|wait| !wait.is_zero()),
        order: response.latency,
        cycle: cycle_started.elapsed(),
    });
    Ok(())
}

fn save_state(config: &Config, deployment: &Deployment, traders: &[Trader]) -> Result<()> {
    let state = SimulationState {
        network: MidenNetwork::from_env().as_str().to_owned(),
        vault_id: deployment.vault_id.to_hex(),
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
