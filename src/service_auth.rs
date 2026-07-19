//! Scoped, rotatable bearer credentials for service-to-service and admin APIs.

use std::env;

use anyhow::{Context, Result, bail};
use axum::http::{HeaderMap, header};
use subtle::ConstantTimeEq;

#[derive(Clone, Debug)]
pub struct ServiceCredentials {
    primary: String,
    next: Option<String>,
}

impl ServiceCredentials {
    pub fn from_env(primary_name: &str, next_name: &str) -> Result<Self> {
        let primary =
            env::var(primary_name).with_context(|| format!("{primary_name} is required"))?;
        let next = env::var(next_name).ok();
        Self::new(primary, next)
            .with_context(|| format!("invalid credentials in {primary_name}/{next_name}"))
    }

    pub fn new(primary: String, next: Option<String>) -> Result<Self> {
        if primary.trim().is_empty() {
            bail!("primary credential must not be empty");
        }
        if next.as_ref().is_some_and(|value| value.trim().is_empty()) {
            bail!("next credential must not be empty when configured");
        }
        Ok(Self { primary, next })
    }

    pub fn primary(&self) -> &str {
        &self.primary
    }

    pub fn authorizes(&self, headers: &HeaderMap) -> bool {
        let Some(candidate) = bearer(headers) else {
            return false;
        };
        constant_time_eq(candidate, &self.primary)
            | self
                .next
                .as_deref()
                .is_some_and(|next| constant_time_eq(candidate, next))
    }
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    let value = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let token = value.strip_prefix("Bearer ")?;
    (!token.is_empty()).then_some(token)
}

fn constant_time_eq(candidate: &str, expected: &str) -> bool {
    let candidate = candidate.as_bytes();
    let expected = expected.as_bytes();
    let max_len = candidate.len().max(expected.len());
    let mut difference = candidate.len() ^ expected.len();
    for index in 0..max_len {
        let left = candidate.get(index).copied().unwrap_or_default();
        let right = expected.get(index).copied().unwrap_or_default();
        difference |= usize::from(left ^ right);
    }
    difference.ct_eq(&0).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            format!("Bearer {token}").parse().unwrap(),
        );
        headers
    }

    #[test]
    fn accepts_primary_and_next_during_rotation() {
        let credentials = ServiceCredentials::new("primary".into(), Some("next".into())).unwrap();
        assert!(credentials.authorizes(&headers("primary")));
        assert!(credentials.authorizes(&headers("next")));
        assert!(!credentials.authorizes(&headers("other")));
    }

    #[test]
    fn credentials_are_scoped_to_their_configured_service() {
        let faucet = ServiceCredentials::new("faucet".into(), Some("faucet-next".into())).unwrap();
        let updater =
            ServiceCredentials::new("updater".into(), Some("updater-next".into())).unwrap();
        let admin = ServiceCredentials::new("admin".into(), Some("admin-next".into())).unwrap();

        for token in ["faucet", "faucet-next"] {
            assert!(faucet.authorizes(&headers(token)));
            assert!(!updater.authorizes(&headers(token)));
            assert!(!admin.authorizes(&headers(token)));
        }
        for token in ["updater", "updater-next"] {
            assert!(updater.authorizes(&headers(token)));
            assert!(!faucet.authorizes(&headers(token)));
            assert!(!admin.authorizes(&headers(token)));
        }
        for token in ["admin", "admin-next"] {
            assert!(admin.authorizes(&headers(token)));
            assert!(!faucet.authorizes(&headers(token)));
            assert!(!updater.authorizes(&headers(token)));
        }
    }

    #[test]
    fn rejects_empty_and_malformed_credentials() {
        assert!(ServiceCredentials::new(String::new(), None).is_err());
        let credentials = ServiceCredentials::new("primary".into(), None).unwrap();
        assert!(!credentials.authorizes(&HeaderMap::new()));
        let mut malformed = HeaderMap::new();
        malformed.insert(header::AUTHORIZATION, "Basic primary".parse().unwrap());
        assert!(!credentials.authorizes(&malformed));
    }
}
