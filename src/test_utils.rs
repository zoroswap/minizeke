use std::{cell::OnceCell, env, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Result, anyhow};
use dotenv::dotenv;
use miden_client::{
    Client, ClientError, RemoteTransactionProver,
    account::{
        AccountBuilder, AccountId, AccountType, StorageMapKey,
        component::{
            BasicWallet, BurnPolicyConfig, FungibleFaucet, MintPolicyConfig, PolicyRegistration,
            TokenName, TokenPolicyManager,
        },
    },
    address::NetworkId,
    asset::{AssetAmount, FungibleAsset, TokenSymbol},
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig},
    keystore::{FilesystemKeyStore, Keystore},
    note::NoteType,
    rpc::domain::account::AccountStorageRequirements,
    testing::{
        account_id::{ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2},
        common::wait_for_blocks,
    },
    transaction::{ForeignAccount, TransactionId, TransactionRequest, TransactionRequestBuilder},
};
use miden_client_sqlite_store::SqliteStore;
use miden_core::{Felt, Word};
use rand::RngCore;
use tracing::info;

use crate::{
    assembly_utils::storage_slot_name,
    message_broker::message_broker::MessageBroker,
    miden_env::MidenNetwork,
    miden_execution::MidenExecution,
    note::{
        DepositInstructions, FundInstructions, RegisterInstructions, WithdrawInstructions,
        ZekeNote, ZekeNoteInstructions,
    },
    pool::USER_SLOT_IDS_SLOT,
    vault::{
        USER_ASSET_TOTAL_FUNDING_SLOT, USER_ASSET_TOTAL_INITIATED_REDEEMS_SLOT,
        USER_ASSET_TOTAL_REDEEMS_SLOT, USER_INDICES_SLOT, USER_PUBKEYS_SLOT, deploy_vault,
        vault_user_asset_key, vault_user_key,
    },
};

const ASSET_0: OnceCell<AccountId> = OnceCell::new();
const ASSET_1: OnceCell<AccountId> = OnceCell::new();

const USERS: OnceCell<AccountId> = OnceCell::new();

pub fn get_asset0() -> AccountId {
    *ASSET_0.get_or_init(|| AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap())
}

pub fn get_asset1() -> AccountId {
    *ASSET_1.get_or_init(|| AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2).unwrap())
}

pub async fn get_client() -> Result<Client<FilesystemKeyStore>> {
    let network = MidenNetwork::from_env();
    build_client(network.store_path()).await
}

/// Client with a separate store for pool-native transactions (swaps).
///
/// The swap tx FPIs into the vault, and `miden-client`'s foreign-account fetch uses
/// `VaultFetch::IfChangedFrom(local_vault_root)` for accounts tracked in the local store.
/// When the root matches, the node omits the asset list and the client reconstructs the
/// foreign vault as empty, which breaks the kernel's foreign-account commitment check as
/// soon as the vault holds assets. Keeping the vault untracked in the swap-submitting
/// client forces `VaultFetch::Always`, so the full asset list is fetched. This also
/// mirrors the production topology where the pool operator does not custody the vault.
pub async fn get_pool_client() -> Result<Client<FilesystemKeyStore>> {
    let network = MidenNetwork::from_env();
    build_client(format!("pool.{}", network.store_path())).await
}

/// Client with an independent store for the standalone faucet service. It shares the
/// deployment keystore so it can sign faucet transactions, but never contends with the
/// server's custody or pool-client SQLite stores.
pub async fn get_faucet_client() -> Result<Client<FilesystemKeyStore>> {
    let network = MidenNetwork::from_env();
    build_client(format!("faucet.{}", network.store_path())).await
}

async fn build_client(store_path: String) -> Result<Client<FilesystemKeyStore>> {
    dotenv().ok();

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

    let sqlite_store = SqliteStore::new(store_path.into()).await?;
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

    let client = client_builder.build().await?;
    Ok(client)
}

/// Submits a transaction, surviving the upstream merkle-store race in `miden-client`:
/// on `ApplyTransactionAfterSubmitFailed` the tx has already been accepted by the node
/// and only the local store update failed, so we sync and retry the store update once.
/// If it still fails, we log and continue — for public tracked accounts the accepted
/// state arrives via a later sync anyway.
pub async fn submit_tx_resilient(
    client: &mut Client<FilesystemKeyStore>,
    account_id: AccountId,
    tx_req: TransactionRequest,
) -> Result<TransactionId> {
    match client.submit_new_transaction(account_id, tx_req).await {
        Ok(tx_id) => Ok(tx_id),
        Err(ClientError::ApplyTransactionAfterSubmitFailed {
            pending_update,
            source,
        }) => {
            let tx_id = pending_update.executed_transaction().id();
            tracing::warn!(
                %tx_id,
                account = %account_id.to_hex(),
                "local store update failed after submit ({source}); syncing and re-applying"
            );
            client.sync_state().await?;
            if let Err(retry_err) = client.apply_transaction_update(*pending_update).await {
                tracing::warn!(
                    %tx_id,
                    account = %account_id.to_hex(),
                    "re-apply after sync failed too ({retry_err}); continuing — the accepted \
                     state will arrive via sync"
                );
            }
            Ok(tx_id)
        }
        Err(e) => Err(e.into()),
    }
}

pub async fn get_miden_execution() -> Result<MidenExecution> {
    dotenv().ok();
    let message_broker = Arc::new(MessageBroker::new());
    let miden_execution = MidenExecution::initialize(message_broker).await?;
    Ok(miden_execution)
}

pub async fn get_vault(client: &mut Client<FilesystemKeyStore>) -> Result<AccountId> {
    dotenv().ok();
    let vault = deploy_vault(client).await?;
    Ok(vault.id())
}

pub async fn get_user(client: &mut Client<FilesystemKeyStore>) -> Result<AccountId> {
    let keystore_path = PathBuf::from("./keystore");
    let keystore = Arc::new(FilesystemKeyStore::new(keystore_path).unwrap());

    let mut init_seed = [0_u8; 32];
    client.rng().fill_bytes(&mut init_seed);

    let key_pair = AuthSecretKey::new_falcon512_poseidon2_with_rng(client.rng());

    // Build the account
    let acc = AccountBuilder::new(init_seed)
        .account_type(AccountType::Public)
        .with_auth_component(AuthSingleSig::new(
            key_pair.public_key().to_commitment(),
            AuthScheme::Falcon512Poseidon2,
        ))
        .with_component(BasicWallet)
        .build()
        .unwrap();

    info!(acc_id = acc.id().to_hex(), "New user account");

    // Add the account to the client
    client.add_account(&acc, false).await?;

    // Add the key pair to the keystore
    keystore.add_key(&key_pair, acc.id()).await.unwrap();

    client.sync_state().await?;

    Ok(acc.id())
}

pub async fn get_faucet(
    client: &mut Client<FilesystemKeyStore>,
    symbol: &str,
) -> Result<AccountId> {
    let mut init_seed = [0u8; 32];
    client.rng().fill_bytes(&mut init_seed);
    let keystore_path = PathBuf::from("./keystore");
    let keystore = Arc::new(FilesystemKeyStore::new(keystore_path).unwrap());
    // Faucet parameters
    let name = TokenName::new(symbol).unwrap();
    let symbol = TokenSymbol::new(symbol).unwrap();
    let decimals = 8;
    let max_supply = AssetAmount::new(10_000_000_000).unwrap();

    // Generate key pair
    let key_pair = AuthSecretKey::new_falcon512_poseidon2_with_rng(client.rng());
    let faucet_account = AccountBuilder::new(init_seed)
        .account_type(AccountType::Public)
        .with_auth_component(AuthSingleSig::new(
            key_pair.public_key().to_commitment(),
            AuthScheme::Falcon512Poseidon2,
        ))
        .with_component(
            FungibleFaucet::builder()
                .name(name)
                .symbol(symbol)
                .decimals(decimals)
                .max_supply(max_supply)
                .build()
                .unwrap(),
        )
        .with_components(
            TokenPolicyManager::new()
                .with_mint_policy(MintPolicyConfig::AllowAll, PolicyRegistration::Active)
                .unwrap()
                .with_burn_policy(BurnPolicyConfig::AllowAll, PolicyRegistration::Active)
                .unwrap(),
        )
        .build()
        .unwrap();

    // Add the faucet to the client
    client.add_account(&faucet_account, false).await?;

    // Add the key pair to the keystore
    use miden_client::keystore::Keystore;
    keystore
        .add_key(&key_pair, faucet_account.id())
        .await
        .unwrap();

    let faucet_account_id_bech32 = faucet_account.id().to_bech32(NetworkId::Testnet);
    println!("Faucet account ID: {:?}", faucet_account_id_bech32);

    // Resync to show newly deployed faucet
    client.sync_state().await?;

    Ok(faucet_account.id())
}

pub async fn get_funded_user(
    client: &mut Client<FilesystemKeyStore>,
) -> Result<(AccountId, AccountId)> {
    let faucet_id = get_faucet(client, "TEST").await?;
    let user_id = get_user(client).await?;
    let fungible_asset = FungibleAsset::new(faucet_id, 10_000).unwrap();
    let transaction_request = TransactionRequestBuilder::new()
        .build_mint_fungible_asset(fungible_asset, user_id, NoteType::Public, client.rng())
        .unwrap();

    println!("mint tx request built");

    submit_tx_resilient(client, faucet_id, transaction_request).await?;

    loop {
        // Resync to get the latest data
        client.sync_state().await?;

        let consumable_notes = client.get_consumable_notes(Some(user_id)).await?;
        let notes = consumable_notes
            .iter()
            .map(|(note, _)| note.clone().try_into())
            .collect::<Result<Vec<_>, _>>()?;

        if !notes.is_empty() {
            println!("Consuming notes now...");
            let transaction_request = TransactionRequestBuilder::new()
                .build_consume_notes(notes)
                .unwrap();

            let tx_id = submit_tx_resilient(client, user_id, transaction_request).await?;
            println!("Consume minted tokens TX: {:?}", tx_id);
            break;
        } else {
            println!("Waiting...");
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }

    Ok((user_id, faucet_id))
}

pub async fn touch_account(
    client: &mut Client<FilesystemKeyStore>,
    account_id: &AccountId,
) -> Result<()> {
    let tx_req = TransactionRequestBuilder::new().build()?;
    submit_tx_resilient(client, *account_id, tx_req).await?;
    client.sync_state().await?;
    wait_for_blocks(client, 1).await;
    Ok(())
}

/// Mints `amount` of `faucet_id` to `user_id` and consumes the mint note as the user.
pub async fn mint_asset_to_user(
    client: &mut Client<FilesystemKeyStore>,
    faucet_id: AccountId,
    user_id: AccountId,
    amount: u64,
) -> Result<()> {
    let fungible_asset =
        FungibleAsset::new(faucet_id, amount).map_err(|e| anyhow!("invalid asset: {e:?}"))?;
    let transaction_request = TransactionRequestBuilder::new()
        .build_mint_fungible_asset(fungible_asset, user_id, NoteType::Public, client.rng())
        .map_err(|e| anyhow!("failed to build mint tx: {e:?}"))?;
    submit_tx_resilient(client, faucet_id, transaction_request).await?;
    consume_all_notes_for(client, user_id).await?;
    Ok(())
}

/// Consumes every currently-consumable note addressed to `account_id`, waiting for at least
/// one to show up.
pub async fn consume_all_notes_for(
    client: &mut Client<FilesystemKeyStore>,
    account_id: AccountId,
) -> Result<()> {
    loop {
        client.sync_state().await?;

        let consumable_notes = client.get_consumable_notes(Some(account_id)).await?;
        let notes = consumable_notes
            .iter()
            .map(|(note, _)| note.clone().try_into())
            .collect::<Result<Vec<_>, _>>()?;

        if !notes.is_empty() {
            let tx_req = TransactionRequestBuilder::new().build_consume_notes(notes)?;
            submit_tx_resilient(client, account_id, tx_req).await?;
            client.sync_state().await?;
            wait_for_blocks(client, 1).await;
            return Ok(());
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

/// Sends a note from `sender_id` and consumes it on `consumer_id`, optionally declaring
/// foreign accounts on the consuming transaction (needed when the note's script FPIs).
pub async fn send_and_consume_note(
    client: &mut Client<FilesystemKeyStore>,
    note: &ZekeNote,
    sender_id: AccountId,
    consumer_id: AccountId,
    foreign_accounts: Vec<ForeignAccount>,
) -> Result<()> {
    let tx_req = TransactionRequestBuilder::new()
        .own_output_notes(vec![note.note().clone()])
        .build()?;
    submit_tx_resilient(client, sender_id, tx_req).await?;
    client.sync_state().await?;
    wait_for_blocks(client, 1).await;

    let tx_req = TransactionRequestBuilder::new()
        .input_notes(vec![(note.note().clone(), None)])
        .foreign_accounts(foreign_accounts)
        .build()?;
    submit_tx_resilient(client, consumer_id, tx_req).await?;
    client.sync_state().await?;
    wait_for_blocks(client, 1).await;
    Ok(())
}

/// Registers `user_id`'s trading pubkey commitment on the vault via a REGISTER note.
pub async fn register_user_on_vault(
    client: &mut Client<FilesystemKeyStore>,
    vault_id: AccountId,
    user_id: AccountId,
    pubkey_commitment: Word,
) -> Result<()> {
    let note = ZekeNote::new(
        ZekeNoteInstructions::Register(RegisterInstructions {
            user_id,
            vault_id,
            pubkey_commitment,
        }),
        client.code_builder(),
    )?;
    send_and_consume_note(client, &note, user_id, vault_id, vec![]).await
}

/// Funds the user's trading balance on the vault via a FUND note carrying `asset`.
pub async fn fund_user_on_vault(
    client: &mut Client<FilesystemKeyStore>,
    vault_id: AccountId,
    user_id: AccountId,
    asset: FungibleAsset,
) -> Result<()> {
    let note = ZekeNote::new(
        ZekeNoteInstructions::Fund(FundInstructions {
            user_id,
            vault_id,
            note_assets: vec![asset],
        }),
        client.code_builder(),
    )?;
    send_and_consume_note(client, &note, user_id, vault_id, vec![]).await
}

/// Deposits liquidity into the vault via a DEPOSIT note carrying `asset`; the vault
/// credits `lp_id`'s entitlement with the deposited amount.
pub async fn deposit_liquidity_on_vault(
    client: &mut Client<FilesystemKeyStore>,
    vault_id: AccountId,
    lp_id: AccountId,
    asset: FungibleAsset,
) -> Result<()> {
    let note = ZekeNote::new(
        ZekeNoteInstructions::Deposit(DepositInstructions {
            lp_id,
            vault_id,
            asset,
        }),
        client.code_builder(),
    )?;
    send_and_consume_note(client, &note, lp_id, vault_id, vec![]).await
}

/// Self-custodial LP withdrawal: sends a WITHDRAW note from `lp_id`, the vault checks the
/// entitlement counters and pays `asset` out via a P2ID note to the LP.
/// Does NOT consume the payout note; use `consume_all_notes_for(client, lp_id)` for that.
pub async fn withdraw_liquidity_from_vault(
    client: &mut Client<FilesystemKeyStore>,
    vault_id: AccountId,
    lp_id: AccountId,
    asset: FungibleAsset,
) -> Result<()> {
    let note = ZekeNote::new(
        ZekeNoteInstructions::Withdraw(WithdrawInstructions {
            lp_id,
            vault_id,
            asset_out: asset,
        }),
        client.code_builder(),
    )?;
    send_and_consume_note(client, &note, lp_id, vault_id, vec![]).await
}

/// Foreign-account declaration for vault-native transactions that FPI into the pool
/// (INIT_REDEEM / REDEEM consumption): requests the user's entry of the pool's
/// index -> trades-slot-id map.
pub fn pool_foreign_account(pool_id: AccountId, user_index: u64) -> Result<ForeignAccount> {
    let index_key = StorageMapKey::new(Word::new([
        Felt::new(user_index).map_err(|e| anyhow!("invalid user index: {e:?}"))?,
        Felt::ZERO,
        Felt::ZERO,
        Felt::ZERO,
    ]));
    let requirements = AccountStorageRequirements::new([(
        storage_slot_name(USER_SLOT_IDS_SLOT),
        [&index_key],
    )]);
    ForeignAccount::public(pool_id, requirements)
        .map_err(|e| anyhow!("failed to build pool foreign account: {e:?}"))
}

/// Foreign-account declaration for pool-native swap transactions that FPI into the vault:
/// requests the funding/initiated/redeems entries for every (asset, user) pair plus the
/// registration entries (pubkey + index) for every user.
pub fn vault_foreign_account(
    vault_id: AccountId,
    asset_user_pairs: &[(AccountId, AccountId)],
) -> Result<ForeignAccount> {
    let asset_user_keys: Vec<StorageMapKey> = asset_user_pairs
        .iter()
        .map(|(asset_id, user_id)| StorageMapKey::new(vault_user_asset_key(*asset_id, *user_id)))
        .collect();
    let user_keys: Vec<StorageMapKey> = asset_user_pairs
        .iter()
        .map(|(_, user_id)| StorageMapKey::new(vault_user_key(*user_id)))
        .collect();

    let requirements = AccountStorageRequirements::new([
        (
            storage_slot_name(USER_ASSET_TOTAL_FUNDING_SLOT),
            asset_user_keys.iter().collect::<Vec<_>>(),
        ),
        (
            storage_slot_name(USER_ASSET_TOTAL_INITIATED_REDEEMS_SLOT),
            asset_user_keys.iter().collect::<Vec<_>>(),
        ),
        (
            storage_slot_name(USER_ASSET_TOTAL_REDEEMS_SLOT),
            asset_user_keys.iter().collect::<Vec<_>>(),
        ),
        (
            storage_slot_name(USER_PUBKEYS_SLOT),
            user_keys.iter().collect::<Vec<_>>(),
        ),
        (
            storage_slot_name(USER_INDICES_SLOT),
            user_keys.iter().collect::<Vec<_>>(),
        ),
    ]);
    ForeignAccount::public(vault_id, requirements)
        .map_err(|e| anyhow!("failed to build vault foreign account: {e:?}"))
}
