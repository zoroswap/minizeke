use std::env;

use miden_client::{
    builder::ClientBuilder,
    keystore::FilesystemKeyStore,
    rpc::Endpoint,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MidenNetwork {
    Localhost,
    Devnet,
    Testnet,
}

impl MidenNetwork {
    pub fn from_env() -> Self {
        match env::var("MIDEN_NETWORK")
            .unwrap_or_else(|_| "testnet".to_string())
            .to_ascii_lowercase()
            .as_str()
        {
            "localhost" | "local" => Self::Localhost,
            "devnet" => Self::Devnet,
            "testnet" => Self::Testnet,
            other => {
                tracing::warn!(network = other, "unknown MIDEN_NETWORK, defaulting to testnet");
                Self::Testnet
            }
        }
    }

    pub fn endpoint(&self) -> Endpoint {
        match self {
            Self::Localhost => Endpoint::localhost(),
            Self::Devnet => Endpoint::devnet(),
            Self::Testnet => Endpoint::testnet(),
        }
    }

    pub fn client_builder() -> ClientBuilder<FilesystemKeyStore> {
        match Self::from_env() {
            Self::Localhost => ClientBuilder::for_localhost(),
            Self::Devnet => ClientBuilder::for_devnet(),
            Self::Testnet => ClientBuilder::for_testnet(),
        }
    }

    pub fn tx_prover_url(&self) -> Option<String> {
        match self {
            Self::Localhost => None,
            Self::Devnet | Self::Testnet => {
                Some(format!("https://tx-prover.{}.miden.io", self.as_str()))
            }
        }
    }

    pub fn store_path(&self) -> String {
        format!("store.{}.sqlite3", self.as_str())
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Localhost => "localhost",
            Self::Devnet => "devnet",
            Self::Testnet => "testnet",
        }
    }
}
