use std::{
    collections::HashSet,
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

pub const DEFAULT_ASSETS_FILE: &str = "assets.toml";

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct AssetConfig {
    pub symbol: String,
    pub decimals: u8,
    pub max_supply: u64,
    /// Initial pool depth expressed in whole tokens.
    pub initial_liquidity: u64,
}

#[derive(Deserialize)]
struct AssetsFile {
    assets: Vec<AssetConfig>,
}

pub fn assets_file_path() -> PathBuf {
    env::var("ASSETS_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_ASSETS_FILE))
}

pub fn load_asset_configs() -> Result<Vec<AssetConfig>> {
    load_asset_configs_from(assets_file_path())
}

pub fn initial_liquidity_base_units(asset: &AssetConfig) -> Result<u64> {
    let scale = 10_u64.checked_pow(asset.decimals.into()).with_context(|| {
        format!(
            "decimals for {} are too large to represent base units",
            asset.symbol
        )
    })?;
    asset
        .initial_liquidity
        .checked_mul(scale)
        .with_context(|| format!("initial_liquidity for {} overflows u64", asset.symbol))
}

pub fn load_asset_configs_from(path: impl AsRef<Path>) -> Result<Vec<AssetConfig>> {
    let path = path.as_ref();
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read asset config {}", path.display()))?;
    let config: AssetsFile = toml::from_str(&contents)
        .with_context(|| format!("failed to parse asset config {}", path.display()))?;

    if config.assets.len() < 2 {
        bail!("asset config must contain at least two [[assets]] entries");
    }

    let mut symbols = HashSet::with_capacity(config.assets.len());
    for asset in &config.assets {
        let symbol = asset.symbol.trim();
        if symbol.is_empty() {
            bail!("asset symbol must not be empty");
        }
        if !symbols.insert(symbol.to_ascii_uppercase()) {
            bail!("duplicate asset symbol: {symbol}");
        }
        if asset.max_supply == 0 {
            bail!("max_supply for {symbol} must be greater than zero");
        }
        if asset.initial_liquidity == 0 {
            bail!("initial_liquidity for {symbol} must be greater than zero");
        }
        if initial_liquidity_base_units(asset)? > asset.max_supply {
            bail!("initial_liquidity for {symbol} exceeds max_supply after decimal scaling");
        }
    }

    Ok(config.assets)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loads_assets_file() -> Result<()> {
        let assets = load_asset_configs_from(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(DEFAULT_ASSETS_FILE),
        )?;
        assert_eq!(
            assets
                .iter()
                .map(|asset| asset.symbol.as_str())
                .collect::<Vec<_>>(),
            ["BTC", "ETH", "USDC"]
        );
        assert_eq!(assets[0].initial_liquidity, 10);
        Ok(())
    }
}
