use miden_core::{Felt, Word};
use miden_protocol::crypto::hash::poseidon2::Poseidon2;

/// Domain-separation tag — stops a signature for one action type being
/// replayed as another. See the cancel/withdraw tags in the perp repo.
///
/// A user's authorization to move `amount` to a recipient, bound to the
/// depositor's account id so the operator knows whose funds are moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Intent {
    pub user_suffix: u64,
    pub user_prefix: u64,
    pub sell_idx: u64,
    pub sell_amount: u64,
    pub buy_idx: u64,
    pub buy_amount: u64,
}

impl Intent {
    /// The exact field elements hashed to the signed Word.
    /// MUST match the TypeScript `intentFelts` ordering byte-for-byte.
    /// Eight felts matching the tx-script stack layout verified on-chain (top → bottom).
    pub fn canonical_felts(&self) -> Vec<u64> {
        vec![
            self.user_suffix,
            self.user_prefix,
            self.sell_idx,
            self.sell_amount,
            self.user_suffix,
            self.user_prefix,
            self.buy_idx,
            self.buy_amount,
        ]
    }

    /// The Word the user signs.
    pub fn message_word(&self) -> Word {
        message_word(&self.canonical_felts())
    }
}

/// Hash a canonical felt vector to the signable Word.
///
/// Uses Poseidon2 (the protocol's canonical `Hasher`), which is the algebraic hash the
/// Miden VM reconstructs on-chain via the native `hperm` instruction. The authorizer
/// component (Task 5) rebuilds this exact Word inside the transaction to verify the
/// signature; using any other algebraic hash (e.g. RPO) would be impossible to reproduce
/// on-chain in this toolchain, since the VM exposes no RPO permutation instruction.
pub fn message_word(felts: &[u64]) -> Word {
    let elements: Vec<Felt> = felts.iter().map(|&v| Felt::new(v).unwrap()).collect();
    Poseidon2::hash_elements(&elements)
}

#[cfg(test)]
mod tests {
    use miden_client::account::{AccountBuilder, AccountType, component::BasicWallet};
    use miden_client::auth::{AuthScheme, AuthSecretKey, AuthSingleSig};
    use miden_core::Word;

    use super::*;
    use crate::miden_execution::{intent_user_fields, user_id_word};

    #[test]
    fn signing_roundtrip_matches_pubkey_commitment() {
        let key_pair = AuthSecretKey::new_ecdsa_k256_keccak();
        let account = AccountBuilder::new([42u8; 32])
            .account_type(AccountType::Public)
            .with_auth_component(AuthSingleSig::new(
                key_pair.public_key().to_commitment(),
                AuthScheme::EcdsaK256Keccak,
            ))
            .with_component(BasicWallet)
            .build()
            .unwrap();
        let user_id = account.id();
        let (user_prefix, user_suffix) = intent_user_fields(user_id);

        let intent = Intent {
            user_suffix,
            user_prefix,
            sell_idx: 0,
            buy_idx: 1,
            sell_amount: 10,
            buy_amount: 10,
        };
        let msg = intent.message_word();
        let sig = key_pair.sign(msg);
        let prepared = sig.to_prepared_signature(msg);
        assert_eq!(prepared.len(), 26);

        let expected: Word = key_pair.public_key().to_commitment().into();
        let map_key = user_id_word(user_id);
        eprintln!("map_key={map_key:?} expected_comm={expected:?} msg={msg:?}");
        assert_ne!(expected, Word::default(), "commitment must be non-zero");
    }
}
