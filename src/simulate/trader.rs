use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use chrono::Utc;
use miden_client::{
    Client, RemoteTransactionProver,
    account::{
        AccountBuilder, AccountId, AccountType,
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
use tokio::sync::watch;
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
        transfer::send_p2id_batch,
    },
    test_utils::{consume_all_notes_for, fund_user_on_vault, register_user_on_vault},
};

pub struct Trader {
    pub index: usize,
    pub tier: TraderTier,
    pub user_id: AccountId,
    key: AuthSecretKey,
}

struct BankAccount {
    user_id: AccountId,
    key: AuthSecretKey,
}

#[derive(Debug, Serialize, Deserialize)]
struct SimulationState {
    network: String,
    vault_id: String,
    bank_user_id: String,
    bank_public_key_commitment: String,
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

pub async fn setup_traders(
    config: &Config,
    deployment: &Deployment,
    api: &SimulationApi,
    metrics: &Metrics,
    client: &mut Client<FilesystemKeyStore>,
    keystore: &FilesystemKeyStore,
) -> Result<Vec<Trader>> {
    if config.state_file.exists() {
        bail!(
            "state file {} already exists; use --skip-setup to resume or choose another \
             --state-file",
            config.state_file.display()
        );
    }

    let tiers = config.tier_assignments();
    info!(
        traders = config.num_traders,
        "creating bank and trader wallets"
    );

    let bank = create_wallet(client, keystore).await?;
    info!(bank = %bank.user_id.to_hex(), "bank wallet created");

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
    save_state(config, deployment, &bank, &traders)?;

    let needed = (config.num_traders as u64)
        .checked_mul(config.fund_amount)
        .ok_or_else(|| anyhow!("num_traders * fund_amount overflow"))?;
    let first_tier = traders
        .first()
        .map(|trader| trader.tier)
        .unwrap_or(TraderTier::HighFrequency);

    for asset in &deployment.assets {
        mint_bank_to_needed(client, api, metrics, &bank, asset, needed, first_tier)
            .await
            .with_context(|| format!("mint {} to bank", asset.symbol))?;

        let recipients: Vec<AccountId> = traders.iter().map(|trader| trader.user_id).collect();
        info!(
            asset = %asset.symbol,
            recipients = recipients.len(),
            amount = config.fund_amount,
            "distributing P2ID notes from bank"
        );
        send_p2id_batch(
            client,
            bank.user_id,
            &recipients,
            asset.faucet_id,
            config.fund_amount,
        )
        .await
        .with_context(|| format!("distribute {} from bank", asset.symbol))?;
    }

    for trader in &traders {
        ensure_wallet_balances(
            client,
            trader.user_id,
            &deployment.assets,
            config.fund_amount,
        )
        .await
        .with_context(|| format!("collect P2IDs for trader {}", trader.index))?;

        let started = Instant::now();
        register_user_on_vault(
            client,
            deployment.vault_id,
            trader.user_id,
            trader.key.public_key().to_commitment().into(),
        )
        .await
        .with_context(|| format!("register trader {}", trader.index))?;
        metrics.record_setup(
            trader.index,
            trader.tier,
            SetupPhase::Register,
            started.elapsed(),
        );

        for asset in &deployment.assets {
            let started = Instant::now();
            fund_user_on_vault(
                client,
                deployment.vault_id,
                trader.user_id,
                FungibleAsset::new(asset.faucet_id, config.fund_amount)
                    .map_err(|error| anyhow!("invalid funding asset: {error:?}"))?,
            )
            .await
            .with_context(|| format!("fund {} for trader {}", asset.symbol, trader.index))?;
            metrics.record_setup(
                trader.index,
                trader.tier,
                SetupPhase::Fund,
                started.elapsed(),
            );
        }

        info!(
            trader = trader.index,
            user = %trader.user_id.to_hex(),
            "trader setup complete"
        );
        save_state(config, deployment, &bank, &traders)?;
    }

    Ok(traders)
}

/// Sync and consume incoming notes until every asset balance reaches `amount`.
async fn ensure_wallet_balances(
    client: &mut Client<FilesystemKeyStore>,
    user_id: AccountId,
    assets: &[AssetInfo],
    amount: u64,
) -> Result<()> {
    let timeout_secs = std::env::var("NETWORK_NOTE_TIMEOUT_SECS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(180);
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        client.sync_state().await?;
        let mut missing = Vec::new();
        for asset in assets {
            let balance = client
                .account_reader(user_id)
                .get_balance(asset.faucet_id)
                .await?;
            if balance < amount {
                missing.push(asset.symbol.clone());
            }
        }
        if missing.is_empty() {
            return Ok(());
        }

        let consumable = client.get_consumable_notes(Some(user_id)).await?;
        if !consumable.is_empty() {
            consume_all_notes_for(client, user_id).await?;
            continue;
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for P2ID balances on {}; still missing {}",
                user_id.to_hex(),
                missing.join(", ")
            );
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

async fn mint_bank_to_needed(
    client: &mut Client<FilesystemKeyStore>,
    api: &SimulationApi,
    metrics: &Metrics,
    bank: &BankAccount,
    asset: &AssetInfo,
    needed: u64,
    metric_tier: TraderTier,
) -> Result<()> {
    loop {
        client.sync_state().await?;
        let balance = client
            .account_reader(bank.user_id)
            .get_balance(asset.faucet_id)
            .await?;
        if balance >= needed {
            info!(
                asset = %asset.symbol,
                balance,
                needed,
                "bank balance sufficient"
            );
            return Ok(());
        }

        info!(
            asset = %asset.symbol,
            balance,
            needed,
            "minting to bank"
        );
        let latency = api.mint(bank.user_id, asset.faucet_id).await?;
        metrics.record_setup(0, metric_tier, SetupPhase::Mint, latency);
        consume_all_notes_for(client, bank.user_id)
            .await
            .context("consume bank mint note")?;
    }
}

async fn create_wallet(
    client: &mut Client<FilesystemKeyStore>,
    keystore: &FilesystemKeyStore,
) -> Result<BankAccount> {
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
    Ok(BankAccount { user_id, key })
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
    mut shutdown: watch::Receiver<bool>,
) {
    let mut session: Option<Session> = None;
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
    let auth_latency = if session
        .as_ref()
        .is_none_or(|current| current.needs_refresh(now))
    {
        let (new_session, latency) = api.authenticate(trader.user_id, &trader.key).await?;
        *session = Some(new_session);
        Some(latency)
    } else {
        None
    };

    let client_order_id = Uuid::new_v4();
    let expires_at = now.saturating_add(300);
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
        auth_ms = auth_latency.map(|latency| latency.as_millis()),
        order_ms = response.latency.as_millis(),
        cycle_ms = cycle_started.elapsed().as_millis(),
        "trade completed"
    );
    metrics.record_trade(TradeMeasurement {
        trader_index: trader.index,
        tier: trader.tier,
        outcome: response.outcome,
        oracle: oracle_latency,
        auth: auth_latency,
        order: response.latency,
        cycle: cycle_started.elapsed(),
    });
    Ok(())
}

fn save_state(
    config: &Config,
    deployment: &Deployment,
    bank: &BankAccount,
    traders: &[Trader],
) -> Result<()> {
    let state = SimulationState {
        network: MidenNetwork::from_env().as_str().to_owned(),
        vault_id: deployment.vault_id.to_hex(),
        bank_user_id: bank.user_id.to_hex(),
        bank_public_key_commitment: Word::from(bank.key.public_key().to_commitment()).to_hex(),
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
