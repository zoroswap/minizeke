//! Standalone, rate-limited faucet service. It owns its own Miden client/store while
//! sharing the deployment `keystore` with `spawn`, which holds each faucet's signing key.
//!
//! Concurrent mint requests are batched per faucet into one transaction via
//! [`TransactionRequestBuilder::own_output_notes`] — the same path as a single
//! `build_mint_fungible_asset`, with N public P2ID notes.

use std::{collections::HashMap, env, time::Duration};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
};
use miden_client::{
    Client,
    account::AccountId,
    address::{Address, AddressId},
    asset::{Asset, FungibleAsset},
    keystore::FilesystemKeyStore,
    note::{Note, NoteType},
    transaction::{TransactionRequest, TransactionRequestBuilder},
};
use miden_protocol::note::NoteAttachments;
use miden_standards::note::P2idNote;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, info};

use crate::{
    deployment::Deployment,
    service_auth::ServiceCredentials,
    test_utils::{get_faucet_client, submit_tx_resilient},
};

pub const DEFAULT_FAUCET_SERVER_URL: &str = "127.0.0.1:7800";
pub const DEFAULT_MINT_AMOUNT: u64 = 10_000_000;
pub const DEFAULT_MINT_COOLDOWN_SECS: u64 = 240;
pub const DEFAULT_BATCH_SIZE: usize = 32;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MintRequest {
    /// Recipient Miden account id in bech32 or `0x`-hex form.
    pub address: String,
    /// Faucet Miden account id in bech32 or `0x`-hex form.
    pub faucet_id: String,
}

#[derive(Debug, Serialize)]
pub struct MintResponse {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

struct FaucetWorker {
    client: Client<FilesystemKeyStore>,
    supported_faucets: Vec<AccountId>,
    mint_amount: u64,
    cooldown: Duration,
    batch_size: usize,
    last_mint: HashMap<(AccountId, AccountId), tokio::time::Instant>,
}

struct FaucetCommand {
    recipient: AccountId,
    faucet_id: AccountId,
    response: oneshot::Sender<Result<String, String>>,
}

/// Sendable handle used by Axum. Miden's client remains exclusively owned by the
/// dedicated current-thread runtime, preventing concurrent use of a faucet account nonce.
#[derive(Clone)]
pub struct FaucetService {
    commands: mpsc::Sender<FaucetCommand>,
    credentials: ServiceCredentials,
}

/// Creates the independent faucet service state and imports all faucet accounts
/// in the active deployment. The `spawn` deployment must have written their keys into
/// `./keystore`.
pub async fn initialize() -> Result<FaucetService> {
    let credentials =
        ServiceCredentials::from_env("FAUCET_SERVICE_TOKEN", "FAUCET_SERVICE_TOKEN_NEXT")?;
    let deployment = Deployment::load()?;
    let mint_amount = env::var("FAUCET_MINT_AMOUNT")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(DEFAULT_MINT_AMOUNT);
    let cooldown_secs = env::var("FAUCET_MINT_COOLDOWN_SECS")
        .ok()
        .map(|value| value.parse())
        .transpose()?
        .unwrap_or(DEFAULT_MINT_COOLDOWN_SECS);
    let batch_size = env::var("FAUCET_BATCH_SIZE")
        .ok()
        .map(|value| value.parse::<usize>())
        .transpose()?
        .unwrap_or(DEFAULT_BATCH_SIZE)
        .max(1);

    let (commands, receiver) = mpsc::channel(100);
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = started_tx.send(Err(error.to_string()));
                return;
            }
        };
        match runtime.block_on(FaucetWorker::new(
            deployment,
            mint_amount,
            cooldown_secs,
            batch_size,
        )) {
            Ok(worker) => {
                let _ = started_tx.send(Ok(()));
                runtime.block_on(worker.run(receiver));
            }
            Err(error) => {
                // Prefer alternate formatting — ClientError's Display omits the cause.
                let _ = started_tx.send(Err(format!("{error:#}")));
            }
        }
    });
    started_rx
        .recv()
        .map_err(|error| anyhow::anyhow!("faucet worker exited during startup: {error}"))?
        .map_err(anyhow::Error::msg)?;

    Ok(FaucetService {
        commands,
        credentials,
    })
}

impl FaucetWorker {
    async fn new(
        deployment: Deployment,
        mint_amount: u64,
        cooldown_secs: u64,
        batch_size: usize,
    ) -> Result<Self> {
        let mut client = get_faucet_client().await?;
        client.ensure_genesis_in_place().await?;
        // Public mint notes only — skip note-transport sync. The faucet store can
        // accumulate more note tags than NTL allows (max 128), which makes
        // `sync_state` fail with "Too many tags in fetch_notes request".
        client.sync_chain().await?;
        let supported_faucets: Vec<_> = deployment
            .assets
            .iter()
            .map(|asset| asset.faucet_id)
            .collect();
        for faucet_id in &supported_faucets {
            client.import_account_by_id(*faucet_id).await?;
        }
        client.sync_chain().await?;
        info!(
            assets = supported_faucets.len(),
            mint_amount,
            cooldown_secs,
            batch_size,
            "Standalone faucet service initialized"
        );
        Ok(Self {
            client,
            supported_faucets,
            mint_amount,
            cooldown: Duration::from_secs(cooldown_secs),
            batch_size,
            last_mint: HashMap::new(),
        })
    }

    async fn run(mut self, mut receiver: mpsc::Receiver<FaucetCommand>) {
        let mut buffer = Vec::with_capacity(self.batch_size);
        loop {
            buffer.clear();
            let Some(first) = receiver.recv().await else {
                break;
            };
            buffer.push(first);
            let _ = receiver.recv_many(&mut buffer, self.batch_size - 1).await;
            self.process_batch(std::mem::take(&mut buffer)).await;
        }
    }

    async fn process_batch(&mut self, commands: Vec<FaucetCommand>) {
        let mut by_faucet: HashMap<AccountId, Vec<FaucetCommand>> = HashMap::new();
        for command in commands {
            if let Err(error) = self.validate_command(command.recipient, command.faucet_id) {
                let _ = command.response.send(Err(error));
                continue;
            }
            by_faucet
                .entry(command.faucet_id)
                .or_default()
                .push(command);
        }

        for (faucet_id, group) in by_faucet {
            self.mint_group(faucet_id, group).await;
        }
    }

    fn validate_command(
        &self,
        recipient: AccountId,
        faucet_id: AccountId,
    ) -> Result<(), String> {
        if !self.supported_faucets.contains(&faucet_id) {
            return Err(format!("faucet {} is not supported", faucet_id.to_hex()));
        }

        let key = (recipient, faucet_id);
        if let Some(last_mint) = self.last_mint.get(&key) {
            let elapsed = last_mint.elapsed();
            if elapsed < self.cooldown {
                return Err(format!(
                    "mint cooldown active; retry in {}s",
                    (self.cooldown - elapsed).as_secs().max(1)
                ));
            }
        }
        Ok(())
    }

    async fn mint_group(&mut self, faucet_id: AccountId, group: Vec<FaucetCommand>) {
        let mut notes = Vec::with_capacity(group.len());
        let mut accepted = Vec::with_capacity(group.len());

        for command in group {
            match self.create_mint_note(faucet_id, command.recipient) {
                Ok(note) => {
                    notes.push(note);
                    accepted.push(command);
                }
                Err(error) => {
                    let _ = command.response.send(Err(error));
                }
            }
        }

        if notes.is_empty() {
            return;
        }

        let request = match build_batch_mint_request(notes) {
            Ok(request) => request,
            Err(error) => {
                for command in accepted {
                    let _ = command.response.send(Err(error.clone()));
                }
                return;
            }
        };

        match submit_tx_resilient(&mut self.client, faucet_id, request).await {
            Ok(transaction_id) => {
                let tx_hex = transaction_id.to_hex();
                let now = tokio::time::Instant::now();
                for command in &accepted {
                    self.last_mint
                        .insert((command.recipient, faucet_id), now);
                }
                info!(
                    faucet = %faucet_id.to_hex(),
                    recipients = accepted.len(),
                    amount = self.mint_amount,
                    transaction = %tx_hex,
                    "Faucet batch mint submitted"
                );
                for command in accepted {
                    let _ = command.response.send(Ok(tx_hex.clone()));
                }
            }
            Err(error) => {
                // Surface the full chain — bare "RPC error" Display is useless for ops.
                let message = format!("{error:#}");
                error!(
                    faucet = %faucet_id.to_hex(),
                    recipients = accepted.len(),
                    error = %message,
                    "Faucet batch mint failed"
                );
                for command in accepted {
                    let _ = command.response.send(Err(message.clone()));
                }
            }
        }
    }

    fn create_mint_note(
        &mut self,
        faucet_id: AccountId,
        recipient: AccountId,
    ) -> Result<Note, String> {
        let asset =
            FungibleAsset::new(faucet_id, self.mint_amount).map_err(|error| error.to_string())?;
        P2idNote::create(
            faucet_id,
            recipient,
            vec![Asset::from(asset)],
            NoteType::Public,
            NoteAttachments::empty(),
            self.client.rng(),
        )
        .map_err(|error| error.to_string())
    }
}

/// Builds a multi-recipient mint request using the standard faucet `send_notes` script
/// (same mechanism as [`TransactionRequestBuilder::build_mint_fungible_asset`]).
pub fn build_batch_mint_request(notes: Vec<Note>) -> Result<TransactionRequest, String> {
    if notes.is_empty() {
        return Err("batch mint requires at least one note".into());
    }
    TransactionRequestBuilder::new()
        .own_output_notes(notes)
        .build()
        .map_err(|error| error.to_string())
}

pub fn router(state: FaucetService) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/mint", post(mint))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    Json(serde_json::json!({ "status": "healthy" }))
}

async fn mint(
    State(state): State<FaucetService>,
    headers: HeaderMap,
    Json(request): Json<MintRequest>,
) -> impl IntoResponse {
    if !state.credentials.authorizes(&headers) {
        return mint_error(
            StatusCode::UNAUTHORIZED,
            "invalid faucet service token".into(),
        );
    }
    let recipient = match parse_account_id(&request.address) {
        Ok(account_id) => account_id,
        Err(error) => {
            return mint_error(StatusCode::BAD_REQUEST, format!("invalid address: {error}"));
        }
    };
    let faucet_id = match parse_account_id(&request.faucet_id) {
        Ok(account_id) => account_id,
        Err(error) => {
            return mint_error(
                StatusCode::BAD_REQUEST,
                format!("invalid faucet_id: {error}"),
            );
        }
    };

    let (response_tx, response_rx) = oneshot::channel();
    if state
        .commands
        .send(FaucetCommand {
            recipient,
            faucet_id,
            response: response_tx,
        })
        .await
        .is_err()
    {
        return mint_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "faucet worker unavailable".to_string(),
        );
    }

    match response_rx.await {
        Ok(Ok(transaction_id)) => (
            StatusCode::ACCEPTED,
            Json(MintResponse {
                success: true,
                transaction_id: Some(transaction_id),
                message: None,
            }),
        )
            .into_response(),
        Ok(Err(error)) => mint_error(StatusCode::BAD_GATEWAY, error),
        Err(_) => mint_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "faucet worker stopped".to_string(),
        ),
    }
}

fn parse_account_id(value: &str) -> Result<AccountId, String> {
    let value = value.trim();
    if value.starts_with("0x") {
        return AccountId::from_hex(value).map_err(|error| error.to_string());
    }

    let (_, address) = Address::decode(value).map_err(|error| error.to_string())?;
    match address.id() {
        AddressId::AccountId(account_id) => Ok(account_id),
        _ => Err("address does not contain an account ID".to_string()),
    }
}

fn mint_error(status: StatusCode, message: String) -> axum::response::Response {
    (
        status,
        Json(MintResponse {
            success: false,
            transaction_id: None,
            message: Some(message),
        }),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use miden_client::{
        Felt, Word,
        account::AccountId,
        asset::{Asset, FungibleAsset},
        crypto::RandomCoin,
        note::NoteType,
        testing::account_id::{
            ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        },
        transaction::TransactionScriptTemplate,
    };
    use miden_protocol::note::NoteAttachments;
    use miden_standards::note::P2idNote;

    use super::{build_batch_mint_request, parse_account_id};

    fn test_rng(seed: u64) -> RandomCoin {
        RandomCoin::new(Word::from([
            Felt::new_unchecked(seed),
            Felt::new_unchecked(seed.wrapping_add(1)),
            Felt::new_unchecked(seed.wrapping_add(2)),
            Felt::new_unchecked(seed.wrapping_add(3)),
        ]))
    }

    #[test]
    fn parses_bech32_address_with_routing_parameters() {
        let account_id =
            parse_account_id("mtst1apdp0kf27ytzqcf5zn4dynclecpw7z8z_qr7qqq9wr6w").unwrap();
        let plain_account_id = parse_account_id("mtst1apdp0kf27ytzqcf5zn4dynclecpw7z8z").unwrap();

        assert_eq!(account_id, plain_account_id);
    }

    #[test]
    fn batch_mint_request_uses_send_notes_with_all_recipients() {
        let faucet_id = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1).unwrap();
        let recipient =
            AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE).unwrap();
        let amount = 10_000_000u64;
        let mut rng = test_rng(1);
        let note1 = P2idNote::create(
            faucet_id,
            recipient,
            vec![Asset::from(FungibleAsset::new(faucet_id, amount).unwrap())],
            NoteType::Public,
            NoteAttachments::empty(),
            &mut rng,
        )
        .unwrap();
        let note2 = P2idNote::create(
            faucet_id,
            recipient,
            vec![Asset::from(FungibleAsset::new(faucet_id, amount).unwrap())],
            NoteType::Public,
            NoteAttachments::empty(),
            &mut rng,
        )
        .unwrap();

        let request = build_batch_mint_request(vec![note1.clone(), note2.clone()]).unwrap();
        match request.script_template() {
            Some(TransactionScriptTemplate::SendNotes(notes)) => {
                assert_eq!(notes.len(), 2);
            }
            other => panic!("expected SendNotes template, got {other:?}"),
        }
        let expected: Vec<_> = [note1, note2]
            .iter()
            .map(|note| note.recipient().digest())
            .collect();
        let got: Vec<_> = request
            .expected_output_recipients()
            .map(|recipient| recipient.digest())
            .collect();
        assert_eq!(got.len(), 2);
        for digest in expected {
            assert!(got.contains(&digest));
        }
    }
}
