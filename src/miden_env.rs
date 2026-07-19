use std::env;

use miden_client::{builder::ClientBuilder, keystore::FilesystemKeyStore, rpc::Endpoint};

/// Whether Miden VM debug execution is enabled.
///
/// Defaults to `false` because debug mode makes transaction execution dramatically slower.
/// Opt in with `MIDEN_DEBUG_MODE=1|true|yes`.
pub fn miden_debug_mode_enabled() -> bool {
    matches!(
        env::var("MIDEN_DEBUG_MODE").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("YES")
    )
}

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
                tracing::warn!(
                    network = other,
                    "unknown MIDEN_NETWORK, defaulting to testnet"
                );
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn miden_debug_mode_defaults_off_and_accepts_opt_in() {
        let _guard = ENV_LOCK.lock().unwrap();
        // SAFETY: serialized by ENV_LOCK; test-only env mutation.
        unsafe {
            env::remove_var("MIDEN_DEBUG_MODE");
        }
        assert!(!miden_debug_mode_enabled());

        for value in ["1", "true", "TRUE", "yes", "YES"] {
            unsafe {
                env::set_var("MIDEN_DEBUG_MODE", value);
            }
            assert!(
                miden_debug_mode_enabled(),
                "expected MIDEN_DEBUG_MODE={value} to enable debug mode"
            );
        }

        unsafe {
            env::set_var("MIDEN_DEBUG_MODE", "0");
        }
        assert!(!miden_debug_mode_enabled());
        unsafe {
            env::remove_var("MIDEN_DEBUG_MODE");
        }
    }
}
