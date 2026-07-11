//! Standalone, rate-limited faucet service. It owns its own Miden client/store while
//! sharing the deployment `keystore` with `spawn`, which holds each faucet's signing key.

use std::{collections::HashMap, env, time::Duration};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use miden_client::{
    Client,
    account::AccountId,
    address::{Address, AddressId},
    asset::FungibleAsset,
    keystore::FilesystemKeyStore,
    note::NoteType,
    transaction::TransactionRequestBuilder,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};
use tracing::info;

use crate::{
    deployment::Deployment,
    test_utils::{get_faucet_client, submit_tx_resilient},
};

pub const DEFAULT_FAUCET_SERVER_URL: &str = "127.0.0.1:7800";
pub const DEFAULT_MINT_AMOUNT: u64 = 10_000_000;
pub const DEFAULT_MINT_COOLDOWN_SECS: u64 = 240;

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
    supported_faucets: [AccountId; 2],
    mint_amount: u64,
    cooldown: Duration,
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
}

/// Creates the independent faucet service state and imports only the two faucet accounts
/// in the active deployment. The `spawn` deployment must have written their keys into
/// `./keystore`.
pub async fn initialize() -> Result<FaucetService> {
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
        match runtime.block_on(FaucetWorker::new(deployment, mint_amount, cooldown_secs)) {
            Ok(worker) => {
                let _ = started_tx.send(Ok(()));
                runtime.block_on(worker.run(receiver));
            }
            Err(error) => {
                let _ = started_tx.send(Err(error.to_string()));
            }
        }
    });
    started_rx
        .recv()
        .map_err(|error| anyhow::anyhow!("faucet worker exited during startup: {error}"))?
        .map_err(anyhow::Error::msg)?;

    Ok(FaucetService { commands })
}

impl FaucetWorker {
    async fn new(deployment: Deployment, mint_amount: u64, cooldown_secs: u64) -> Result<Self> {
        let mut client = get_faucet_client().await?;
        client.ensure_genesis_in_place().await?;
        client.sync_state().await?;
        client.import_account_by_id(deployment.asset0).await?;
        client.import_account_by_id(deployment.asset1).await?;
        client.sync_state().await?;
        info!(
            asset0 = %deployment.asset0.to_hex(),
            asset1 = %deployment.asset1.to_hex(),
            mint_amount,
            cooldown_secs,
            "Standalone faucet service initialized"
        );
        Ok(Self {
            client,
            supported_faucets: [deployment.asset0, deployment.asset1],
            mint_amount,
            cooldown: Duration::from_secs(cooldown_secs),
            last_mint: HashMap::new(),
        })
    }

    async fn run(mut self, mut receiver: mpsc::Receiver<FaucetCommand>) {
        while let Some(command) = receiver.recv().await {
            let result = self.mint(command.recipient, command.faucet_id).await;
            let _ = command.response.send(result);
        }
    }

    async fn mint(&mut self, recipient: AccountId, faucet_id: AccountId) -> Result<String, String> {
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

        let asset =
            FungibleAsset::new(faucet_id, self.mint_amount).map_err(|error| error.to_string())?;
        let request = TransactionRequestBuilder::new()
            .build_mint_fungible_asset(asset, recipient, NoteType::Public, self.client.rng())
            .map_err(|error| error.to_string())?;
        let transaction_id = submit_tx_resilient(&mut self.client, faucet_id, request)
            .await
            .map_err(|error| error.to_string())?;

        self.last_mint.insert(key, tokio::time::Instant::now());
        info!(
            faucet = %faucet_id.to_hex(),
            recipient = %recipient.to_hex(),
            amount = self.mint_amount,
            transaction = %transaction_id.to_hex(),
            "Faucet mint submitted"
        );
        Ok(transaction_id.to_hex())
    }
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
    Json(request): Json<MintRequest>,
) -> impl IntoResponse {
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
    use super::parse_account_id;

    #[test]
    fn parses_bech32_address_with_routing_parameters() {
        let account_id =
            parse_account_id("mtst1apdp0kf27ytzqcf5zn4dynclecpw7z8z_qr7qqq9wr6w").unwrap();
        let plain_account_id = parse_account_id("mtst1apdp0kf27ytzqcf5zn4dynclecpw7z8z").unwrap();

        assert_eq!(account_id, plain_account_id);
    }
}
