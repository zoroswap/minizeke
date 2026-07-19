//! Durable wallet challenges and opaque bearer sessions.
//!
//! The store deliberately accepts already-decoded Miden authentication types. HTTP/JSON
//! integration should decode `PublicKey` and `Signature` with Miden's `Deserializable` format,
//! then pass them to [`AuthStore::authenticate`].

use std::{
    fmt,
    path::Path,
    sync::{Mutex, MutexGuard},
};

use axum::{
    extract::FromRequestParts,
    http::{HeaderMap, StatusCode, header},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use miden_client::{
    account::AccountId,
    auth::{PublicKey, Signature},
};
use miden_core::{Felt, Word};
use miden_protocol::crypto::hash::poseidon2::Poseidon2;
use rand::RngCore;
use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};

const CHALLENGE_PURPOSE: u64 = 0x4d5a_4b45_4155_5448; // "MZKEAUTH"
const TOKEN_PURPOSE: u64 = 0x4d5a_4b45_5345_5353; // "MZKESESS"

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub domain: String,
    pub network: String,
    pub challenge_ttl_secs: u64,
    pub session_ttl_secs: u64,
}

impl AuthConfig {
    pub fn new(
        domain: impl Into<String>,
        network: impl Into<String>,
        challenge_ttl_secs: u64,
        session_ttl_secs: u64,
    ) -> Result<Self, AuthError> {
        let config = Self {
            domain: domain.into(),
            network: network.into(),
            challenge_ttl_secs,
            session_ttl_secs,
        };
        if config.domain.is_empty() || config.network.is_empty() {
            return Err(AuthError::Configuration(
                "auth domain and network must not be empty".into(),
            ));
        }
        if challenge_ttl_secs == 0 || session_ttl_secs == 0 {
            return Err(AuthError::Configuration(
                "challenge and session TTLs must be non-zero".into(),
            ));
        }
        Ok(config)
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            domain: "minizeke".into(),
            network: "testnet".into(),
            challenge_ttl_secs: 5 * 60,
            session_ttl_secs: 60 * 60,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthChallenge {
    pub id: String,
    pub nonce: [u8; 32],
    pub user_id: AccountId,
    pub vault_commitment: Word,
    pub issued_at: u64,
    pub expires_at: u64,
    pub message: Word,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewSession {
    /// The only copy of the bearer secret. It is never persisted by this module.
    pub bearer_token: String,
    pub session: Session,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub user_id: AccountId,
    pub vault_commitment: Word,
    pub created_at: u64,
    pub expires_at: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    Configuration(String),
    Storage(String),
    ChallengeNotFound,
    ChallengeExpired,
    ChallengeConsumed,
    WrongBinding,
    UnsupportedKeyScheme,
    InvalidSignature,
    InvalidTimestamp,
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(message) => write!(f, "invalid auth configuration: {message}"),
            Self::Storage(message) => write!(f, "auth storage error: {message}"),
            Self::ChallengeNotFound => f.write_str("challenge not found"),
            Self::ChallengeExpired => f.write_str("challenge expired"),
            Self::ChallengeConsumed => f.write_str("challenge already consumed"),
            Self::WrongBinding => f.write_str("challenge, user, or public-key binding is wrong"),
            Self::UnsupportedKeyScheme => {
                f.write_str("only ECDSA k256-keccak wallet keys are accepted")
            }
            Self::InvalidSignature => f.write_str("invalid challenge signature"),
            Self::InvalidTimestamp => f.write_str("timestamp exceeds supported range"),
        }
    }
}

impl std::error::Error for AuthError {}

impl From<rusqlite::Error> for AuthError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Storage(error.to_string())
    }
}

pub struct AuthStore {
    connection: Mutex<Connection>,
    config: AuthConfig,
}

impl AuthStore {
    pub fn open(path: impl AsRef<Path>, config: AuthConfig) -> Result<Self, AuthError> {
        validate_config(&config)?;
        let connection = Connection::open(path)?;
        Self::from_connection(connection, config)
    }

    pub fn open_in_memory(config: AuthConfig) -> Result<Self, AuthError> {
        validate_config(&config)?;
        Self::from_connection(Connection::open_in_memory()?, config)
    }

    fn from_connection(connection: Connection, config: AuthConfig) -> Result<Self, AuthError> {
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS auth_challenges (
                challenge_id TEXT PRIMARY KEY,
                nonce BLOB NOT NULL,
                user_id TEXT NOT NULL,
                vault_commitment BLOB NOT NULL,
                issued_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                consumed_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_auth_challenges_expiry
                ON auth_challenges(expires_at);

            CREATE TABLE IF NOT EXISTS auth_sessions (
                token_commitment BLOB PRIMARY KEY,
                user_id TEXT NOT NULL,
                vault_commitment BLOB NOT NULL,
                created_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL,
                revoked_at INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_auth_sessions_expiry
                ON auth_sessions(expires_at);
            "#,
        )?;
        Ok(Self {
            connection: Mutex::new(connection),
            config,
        })
    }

    /// Creates a durable challenge bound to this service, network, user, and vault key.
    pub fn issue_challenge(
        &self,
        user_id: AccountId,
        vault_commitment: Word,
        now: u64,
    ) -> Result<AuthChallenge, AuthError> {
        let expires_at = now
            .checked_add(self.config.challenge_ttl_secs)
            .ok_or(AuthError::InvalidTimestamp)?;
        let mut nonce = [0_u8; 32];
        rand::rng().fill_bytes(&mut nonce);
        let mut id_bytes = [0_u8; 18];
        rand::rng().fill_bytes(&mut id_bytes);
        let id = URL_SAFE_NO_PAD.encode(id_bytes);
        let message = challenge_word(
            &self.config.domain,
            &self.config.network,
            user_id,
            vault_commitment,
            &nonce,
            now,
            expires_at,
        );

        self.connection()?.execute(
            "INSERT INTO auth_challenges
             (challenge_id, nonce, user_id, vault_commitment, issued_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                id,
                nonce.as_slice(),
                user_id.to_hex(),
                word_bytes(vault_commitment).as_slice(),
                timestamp(now)?,
                timestamp(expires_at)?,
            ],
        )?;

        Ok(AuthChallenge {
            id,
            nonce,
            user_id,
            vault_commitment,
            issued_at: now,
            expires_at,
            message,
        })
    }

    /// Atomically verifies and consumes a challenge, then creates an opaque session.
    ///
    /// `vault_commitment` must be the value read from the caller's registered vault entry.
    pub fn authenticate(
        &self,
        challenge_id: &str,
        user_id: AccountId,
        vault_commitment: Word,
        public_key: PublicKey,
        signature: Signature,
        now: u64,
    ) -> Result<NewSession, AuthError> {
        if !matches!(&public_key, PublicKey::EcdsaK256Keccak(_))
            || !matches!(&signature, Signature::EcdsaK256Keccak(_))
        {
            return Err(AuthError::UnsupportedKeyScheme);
        }
        if Word::from(public_key.to_commitment()) != vault_commitment {
            return Err(AuthError::WrongBinding);
        }

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let stored = transaction
            .query_row(
                "SELECT nonce, user_id, vault_commitment, issued_at, expires_at, consumed_at
                 FROM auth_challenges WHERE challenge_id = ?1",
                [challenge_id],
                |row| {
                    Ok((
                        row.get::<_, Vec<u8>>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, Vec<u8>>(2)?,
                        row.get::<_, i64>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i64>>(5)?,
                    ))
                },
            )
            .optional()?
            .ok_or(AuthError::ChallengeNotFound)?;

        let (nonce, stored_user, stored_commitment, issued_at, expires_at, consumed_at) = stored;
        if consumed_at.is_some() {
            return Err(AuthError::ChallengeConsumed);
        }
        if timestamp(now)? > expires_at {
            return Err(AuthError::ChallengeExpired);
        }
        if stored_user != user_id.to_hex()
            || stored_commitment.as_slice() != word_bytes(vault_commitment).as_slice()
        {
            return Err(AuthError::WrongBinding);
        }
        let nonce: [u8; 32] = nonce
            .try_into()
            .map_err(|_| AuthError::Storage("invalid stored challenge nonce".into()))?;
        let issued_at = u64::try_from(issued_at)
            .map_err(|_| AuthError::Storage("negative issued_at".into()))?;
        let expires_at = u64::try_from(expires_at)
            .map_err(|_| AuthError::Storage("negative expires_at".into()))?;
        let message = challenge_word(
            &self.config.domain,
            &self.config.network,
            user_id,
            vault_commitment,
            &nonce,
            issued_at,
            expires_at,
        );
        if !public_key.verify(message, signature) {
            return Err(AuthError::InvalidSignature);
        }

        let changed = transaction.execute(
            "UPDATE auth_challenges SET consumed_at = ?2
             WHERE challenge_id = ?1 AND consumed_at IS NULL AND expires_at >= ?2",
            params![challenge_id, timestamp(now)?],
        )?;
        if changed != 1 {
            return Err(AuthError::ChallengeConsumed);
        }

        let session_expires_at = now
            .checked_add(self.config.session_ttl_secs)
            .ok_or(AuthError::InvalidTimestamp)?;
        let mut token_bytes = [0_u8; 32];
        rand::rng().fill_bytes(&mut token_bytes);
        let bearer_token = URL_SAFE_NO_PAD.encode(token_bytes);
        let token_commitment = token_commitment(&bearer_token);
        transaction.execute(
            "INSERT INTO auth_sessions
             (token_commitment, user_id, vault_commitment, created_at, expires_at)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                word_bytes(token_commitment).as_slice(),
                user_id.to_hex(),
                word_bytes(vault_commitment).as_slice(),
                timestamp(now)?,
                timestamp(session_expires_at)?,
            ],
        )?;
        transaction.commit()?;

        Ok(NewSession {
            bearer_token,
            session: Session {
                user_id,
                vault_commitment,
                created_at: now,
                expires_at: session_expires_at,
            },
        })
    }

    pub fn lookup_session(
        &self,
        bearer_token: &str,
        now: u64,
    ) -> Result<Option<Session>, AuthError> {
        let commitment = word_bytes(token_commitment(bearer_token));
        let row = self
            .connection()?
            .query_row(
                "SELECT user_id, vault_commitment, created_at, expires_at
                 FROM auth_sessions
                 WHERE token_commitment = ?1 AND revoked_at IS NULL AND expires_at >= ?2",
                params![commitment.as_slice(), timestamp(now)?],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Vec<u8>>(1)?,
                        row.get::<_, i64>(2)?,
                        row.get::<_, i64>(3)?,
                    ))
                },
            )
            .optional()?;
        row.map(|(user_id, vault_commitment, created_at, expires_at)| {
            Ok(Session {
                user_id: AccountId::from_hex(&user_id)
                    .map_err(|error| AuthError::Storage(error.to_string()))?,
                vault_commitment: bytes_word(&vault_commitment)?,
                created_at: u64::try_from(created_at)
                    .map_err(|_| AuthError::Storage("negative created_at".into()))?,
                expires_at: u64::try_from(expires_at)
                    .map_err(|_| AuthError::Storage("negative expires_at".into()))?,
            })
        })
        .transpose()
    }

    pub fn revoke_session(&self, bearer_token: &str, now: u64) -> Result<bool, AuthError> {
        let commitment = word_bytes(token_commitment(bearer_token));
        Ok(self.connection()?.execute(
            "UPDATE auth_sessions SET revoked_at = ?2
             WHERE token_commitment = ?1 AND revoked_at IS NULL",
            params![commitment.as_slice(), timestamp(now)?],
        )? == 1)
    }

    /// Removes expired challenges and expired/revoked sessions.
    pub fn purge(&self, now: u64) -> Result<usize, AuthError> {
        let connection = self.connection()?;
        let now = timestamp(now)?;
        let challenges = connection.execute(
            "DELETE FROM auth_challenges WHERE expires_at < ?1 OR consumed_at IS NOT NULL",
            [now],
        )?;
        let sessions = connection.execute(
            "DELETE FROM auth_sessions WHERE expires_at < ?1 OR revoked_at IS NOT NULL",
            [now],
        )?;
        Ok(challenges + sessions)
    }

    fn connection(&self) -> Result<MutexGuard<'_, Connection>, AuthError> {
        self.connection
            .lock()
            .map_err(|_| AuthError::Storage("auth database lock poisoned".into()))
    }
}

/// Rebuilds the exact Poseidon2 `Word` a wallet must sign.
pub fn challenge_word(
    domain: &str,
    network: &str,
    user_id: AccountId,
    vault_commitment: Word,
    nonce: &[u8; 32],
    issued_at: u64,
    expires_at: u64,
) -> Word {
    let mut felts = vec![CHALLENGE_PURPOSE];
    push_bytes(&mut felts, domain.as_bytes());
    push_bytes(&mut felts, network.as_bytes());
    // Match the intent/vault convention: account suffix precedes account prefix.
    felts.push(user_id.suffix().as_canonical_u64());
    felts.push(user_id.prefix().as_felt().as_canonical_u64());
    felts.extend(
        vault_commitment
            .into_iter()
            .map(|felt| felt.as_canonical_u64()),
    );
    push_bytes(&mut felts, nonce);
    felts.push(issued_at);
    felts.push(expires_at);
    Poseidon2::hash_elements(
        &felts
            .into_iter()
            .map(|value| Felt::new(value).expect("canonical limbs fit in a Felt"))
            .collect::<Vec<_>>(),
    )
}

fn token_commitment(token: &str) -> Word {
    let mut felts = vec![TOKEN_PURPOSE];
    push_bytes(&mut felts, token.as_bytes());
    Poseidon2::hash_elements(
        &felts
            .into_iter()
            .map(|value| Felt::new(value).expect("canonical limbs fit in a Felt"))
            .collect::<Vec<_>>(),
    )
}

/// Adds a length-prefixed byte string as little-endian 7-byte field limbs.
fn push_bytes(target: &mut Vec<u64>, bytes: &[u8]) {
    target.push(bytes.len() as u64);
    for chunk in bytes.chunks(7) {
        let mut limb = [0_u8; 8];
        limb[..chunk.len()].copy_from_slice(chunk);
        target.push(u64::from_le_bytes(limb));
    }
}

fn word_bytes(word: Word) -> [u8; 32] {
    let mut bytes = [0_u8; 32];
    for (index, felt) in word.into_iter().enumerate() {
        bytes[index * 8..(index + 1) * 8].copy_from_slice(&felt.as_canonical_u64().to_le_bytes());
    }
    bytes
}

fn bytes_word(bytes: &[u8]) -> Result<Word, AuthError> {
    if bytes.len() != 32 {
        return Err(AuthError::Storage("invalid stored Word length".into()));
    }
    let mut elements = [Felt::ZERO; 4];
    for (index, element) in elements.iter_mut().enumerate() {
        let limb = u64::from_le_bytes(
            bytes[index * 8..(index + 1) * 8]
                .try_into()
                .expect("slice length is fixed"),
        );
        *element = Felt::new(limb)
            .map_err(|_| AuthError::Storage("invalid stored field element".into()))?;
    }
    Ok(Word::new(elements))
}

fn timestamp(value: u64) -> Result<i64, AuthError> {
    i64::try_from(value).map_err(|_| AuthError::InvalidTimestamp)
}

fn validate_config(config: &AuthConfig) -> Result<(), AuthError> {
    AuthConfig::new(
        config.domain.clone(),
        config.network.clone(),
        config.challenge_ttl_secs,
        config.session_ttl_secs,
    )
    .map(|_| ())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BearerToken(String);

impl BearerToken {
    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BearerParseError {
    Missing,
    Invalid,
}

pub fn parse_bearer(headers: &HeaderMap) -> Result<BearerToken, BearerParseError> {
    let value = headers
        .get(header::AUTHORIZATION)
        .ok_or(BearerParseError::Missing)?
        .to_str()
        .map_err(|_| BearerParseError::Invalid)?;
    let mut parts = value.split_ascii_whitespace();
    let scheme = parts.next().ok_or(BearerParseError::Invalid)?;
    let token = parts.next().ok_or(BearerParseError::Invalid)?;
    if !scheme.eq_ignore_ascii_case("bearer") || token.is_empty() || parts.next().is_some() {
        return Err(BearerParseError::Invalid);
    }
    Ok(BearerToken(token.to_owned()))
}

impl<S> FromRequestParts<S> for BearerToken
where
    S: Send + Sync,
{
    type Rejection = (StatusCode, &'static str);

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> Result<Self, Self::Rejection> {
        parse_bearer(&parts.headers).map_err(|error| match error {
            BearerParseError::Missing => (StatusCode::UNAUTHORIZED, "missing bearer token"),
            BearerParseError::Invalid => (StatusCode::UNAUTHORIZED, "invalid bearer token"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use miden_client::auth::AuthSecretKey;

    fn store(challenge_ttl: u64, session_ttl: u64) -> AuthStore {
        AuthStore::open_in_memory(
            AuthConfig::new("api.minizeke.test", "devnet", challenge_ttl, session_ttl).unwrap(),
        )
        .unwrap()
    }

    fn user_id() -> AccountId {
        // A known-valid account id generated by the Miden test account machinery.
        AccountId::from_hex("0x5a17d92af11620613414ead24f1fce").unwrap()
    }

    fn other_user_id() -> AccountId {
        AccountId::from_hex("0x57a179f33b726c315fcfd5e0ff3309").unwrap()
    }

    #[test]
    fn challenge_is_one_time() {
        let store = store(30, 60);
        let key = AuthSecretKey::new_ecdsa_k256_keccak();
        let public_key = key.public_key();
        let commitment = Word::from(public_key.to_commitment());
        let challenge = store.issue_challenge(user_id(), commitment, 100).unwrap();
        let signature = key.sign(challenge.message);

        let session = store
            .authenticate(
                &challenge.id,
                user_id(),
                commitment,
                public_key.clone(),
                signature.clone(),
                101,
            )
            .unwrap();
        assert!(
            store
                .lookup_session(&session.bearer_token, 101)
                .unwrap()
                .is_some()
        );
        assert_eq!(
            store.authenticate(
                &challenge.id,
                user_id(),
                commitment,
                public_key,
                signature,
                102,
            ),
            Err(AuthError::ChallengeConsumed)
        );
    }

    #[test]
    fn challenge_and_session_expire() {
        let store = store(2, 3);
        let key = AuthSecretKey::new_ecdsa_k256_keccak();
        let public_key = key.public_key();
        let commitment = Word::from(public_key.to_commitment());
        let expired = store.issue_challenge(user_id(), commitment, 10).unwrap();
        assert_eq!(
            store.authenticate(
                &expired.id,
                user_id(),
                commitment,
                public_key.clone(),
                key.sign(expired.message),
                13,
            ),
            Err(AuthError::ChallengeExpired)
        );

        let current = store.issue_challenge(user_id(), commitment, 20).unwrap();
        let session = store
            .authenticate(
                &current.id,
                user_id(),
                commitment,
                public_key,
                key.sign(current.message),
                20,
            )
            .unwrap();
        assert!(
            store
                .lookup_session(&session.bearer_token, 23)
                .unwrap()
                .is_some()
        );
        assert!(
            store
                .lookup_session(&session.bearer_token, 24)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn wrong_vault_binding_is_rejected_without_consuming_challenge() {
        let store = store(30, 60);
        let key = AuthSecretKey::new_ecdsa_k256_keccak();
        let right_public_key = key.public_key();
        let right_commitment = Word::from(right_public_key.to_commitment());
        let wrong_key = AuthSecretKey::new_ecdsa_k256_keccak();
        let wrong_public_key = wrong_key.public_key();
        let wrong_commitment = Word::from(wrong_public_key.to_commitment());
        let challenge = store
            .issue_challenge(user_id(), right_commitment, 100)
            .unwrap();

        assert_eq!(
            store.authenticate(
                &challenge.id,
                other_user_id(),
                right_commitment,
                right_public_key.clone(),
                key.sign(challenge.message),
                101,
            ),
            Err(AuthError::WrongBinding)
        );
        assert_eq!(
            store.authenticate(
                &challenge.id,
                user_id(),
                right_commitment,
                wrong_public_key,
                wrong_key.sign(challenge.message),
                101,
            ),
            Err(AuthError::WrongBinding)
        );
        assert!(
            store
                .authenticate(
                    &challenge.id,
                    user_id(),
                    right_commitment,
                    right_public_key,
                    key.sign(challenge.message),
                    101,
                )
                .is_ok()
        );
        assert_ne!(right_commitment, wrong_commitment);
    }

    #[test]
    fn database_never_stores_plaintext_token() {
        let store = store(30, 60);
        let key = AuthSecretKey::new_ecdsa_k256_keccak();
        let public_key = key.public_key();
        let commitment = Word::from(public_key.to_commitment());
        let challenge = store.issue_challenge(user_id(), commitment, 100).unwrap();
        let session = store
            .authenticate(
                &challenge.id,
                user_id(),
                commitment,
                public_key,
                key.sign(challenge.message),
                100,
            )
            .unwrap();
        let connection = store.connection().unwrap();
        let stored: Vec<u8> = connection
            .query_row("SELECT token_commitment FROM auth_sessions", [], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(stored.len(), 32);
        assert_ne!(stored, session.bearer_token.as_bytes());
        assert_eq!(
            stored,
            word_bytes(token_commitment(&session.bearer_token)).to_vec()
        );
    }

    #[test]
    fn revoked_session_is_immediately_unusable() {
        let store = store(30, 60);
        let key = AuthSecretKey::new_ecdsa_k256_keccak();
        let public_key = key.public_key();
        let commitment = Word::from(public_key.to_commitment());
        let challenge = store.issue_challenge(user_id(), commitment, 100).unwrap();
        let session = store
            .authenticate(
                &challenge.id,
                user_id(),
                commitment,
                public_key,
                key.sign(challenge.message),
                100,
            )
            .unwrap();

        assert!(store.revoke_session(&session.bearer_token, 101).unwrap());
        assert!(!store.revoke_session(&session.bearer_token, 102).unwrap());
        assert!(
            store
                .lookup_session(&session.bearer_token, 101)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn bearer_parsing_is_strict_and_case_insensitive() {
        let mut headers = HeaderMap::new();
        assert_eq!(parse_bearer(&headers), Err(BearerParseError::Missing));
        headers.insert(header::AUTHORIZATION, "bearer abc_123".parse().unwrap());
        assert_eq!(parse_bearer(&headers).unwrap().as_str(), "abc_123");
        headers.insert(header::AUTHORIZATION, "Basic abc".parse().unwrap());
        assert_eq!(parse_bearer(&headers), Err(BearerParseError::Invalid));
        headers.insert(header::AUTHORIZATION, "Bearer one two".parse().unwrap());
        assert_eq!(parse_bearer(&headers), Err(BearerParseError::Invalid));
    }
}
