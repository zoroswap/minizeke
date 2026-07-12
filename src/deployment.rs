use std::path::PathBuf;

use anyhow::{Context, Result, anyhow};
use miden_client::account::AccountId;
use serde::{Deserialize, Serialize};

use crate::miden_env::MidenNetwork;

pub const DEPLOYMENT_SCHEMA_VERSION: u32 = 2;

/// One fungible asset supported by this deployment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssetInfo {
    #[serde(with = "account_id_hex")]
    pub faucet_id: AccountId,
    pub symbol: String,
    pub decimals: u8,
    pub oracle_feed_id: String,
}

/// One recorded liquidity seeding deposit (see `deposit_pools` binary).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepositRecord {
    #[serde(with = "account_id_hex")]
    pub faucet_id: AccountId,
    pub amount: u64,
}

/// On-chain deployment descriptor produced by the `spawn` binary and consumed by the
/// server at startup (attach-only: the server never deploys accounts itself).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Deployment {
    pub schema_version: u32,
    pub network: String,
    /// Server-controlled account authorized to send vault maintenance notes.
    #[serde(with = "account_id_hex")]
    pub operator_account_id: AccountId,
    #[serde(with = "account_id_hex")]
    pub vault_id: AccountId,
    pub assets: Vec<AssetInfo>,
    #[serde(with = "account_id_hex_vec")]
    pub pools: Vec<AccountId>,
    /// LP account used by `deposit_pools` to seed liquidity.
    #[serde(default, with = "account_id_hex_opt")]
    pub lp_account_id: Option<AccountId>,
    /// Liquidity seeding deposits, in the order they were made on chain. The server
    /// replays these through the curve's deposit math to rebuild pool states.
    #[serde(default)]
    pub deposits: Vec<DepositRecord>,
}

impl Deployment {
    /// Path of the deployment file: `DEPLOYMENT_FILE` env override, otherwise
    /// `deployment.{network}.json` in the working directory.
    pub fn path() -> PathBuf {
        if let Ok(path) = std::env::var("DEPLOYMENT_FILE") {
            return PathBuf::from(path);
        }
        PathBuf::from(format!(
            "deployment.{}.json",
            MidenNetwork::from_env().as_str()
        ))
    }

    pub fn exists() -> bool {
        Self::path().exists()
    }

    pub fn load() -> Result<Self> {
        let path = Self::path();
        let contents = std::fs::read_to_string(&path).with_context(|| {
            format!(
                "failed to read deployment file {}; run `cargo run --bin spawn` first",
                path.display()
            )
        })?;
        let value: serde_json::Value = serde_json::from_str(&contents)
            .with_context(|| format!("failed to parse deployment file {}", path.display()))?;
        let schema_version = value
            .get("schema_version")
            .and_then(serde_json::Value::as_u64);
        if schema_version != Some(u64::from(DEPLOYMENT_SCHEMA_VERSION)) {
            return Err(anyhow!(
                "deployment file {} uses schema version {}; expected {}. Re-run `cargo run --bin \
                 spawn` to create a new deployment",
                path.display(),
                schema_version
                    .map(|version| version.to_string())
                    .unwrap_or_else(|| "1 (legacy/unversioned)".to_string()),
                DEPLOYMENT_SCHEMA_VERSION
            ));
        }
        let deployment: Self = serde_json::from_value(value)
            .with_context(|| format!("failed to parse deployment file {}", path.display()))?;

        let network = MidenNetwork::from_env().as_str();
        if deployment.network != network {
            return Err(anyhow!(
                "deployment file {} is for network '{}' but MIDEN_NETWORK is '{}'",
                path.display(),
                deployment.network,
                network
            ));
        }
        if deployment.assets.len() < 2 {
            return Err(anyhow!("deployment must contain at least two assets"));
        }
        if deployment.pools.is_empty() {
            return Err(anyhow!("deployment must contain at least one pool"));
        }
        Ok(deployment)
    }

    pub fn save(&self) -> Result<()> {
        let path = Self::path();
        let contents = serde_json::to_string_pretty(self)?;
        std::fs::write(&path, contents)
            .with_context(|| format!("failed to write deployment file {}", path.display()))?;
        Ok(())
    }
}

mod account_id_hex {
    use miden_client::account::AccountId;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(id: &AccountId, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&id.to_hex())
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<AccountId, D::Error> {
        let hex = String::deserialize(deserializer)?;
        AccountId::from_hex(&hex).map_err(serde::de::Error::custom)
    }
}

mod account_id_hex_opt {
    use miden_client::account::AccountId;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        id: &Option<AccountId>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match id {
            Some(id) => serializer.serialize_some(&id.to_hex()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<AccountId>, D::Error> {
        let hex: Option<String> = Option::deserialize(deserializer)?;
        hex.map(|h| AccountId::from_hex(&h).map_err(serde::de::Error::custom))
            .transpose()
    }
}

mod account_id_hex_vec {
    use miden_client::account::AccountId;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S: Serializer>(ids: &[AccountId], serializer: S) -> Result<S::Ok, S::Error> {
        ids.iter()
            .map(|id| id.to_hex())
            .collect::<Vec<_>>()
            .serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<AccountId>, D::Error> {
        Vec::<String>::deserialize(deserializer)?
            .into_iter()
            .map(|hex| AccountId::from_hex(&hex).map_err(serde::de::Error::custom))
            .collect()
    }
}
