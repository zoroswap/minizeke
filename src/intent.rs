use miden_client::auth::PublicKeyCommitment;
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
    pub pubkey_commitment: PublicKeyCommitment,
}

impl Intent {
    /// The exact field elements hashed to the signed Word.
    /// MUST match the TypeScript `intentFelts` ordering byte-for-byte.
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

    pub fn tx_script_string(&self) -> String {
        let pubkey_commitment: Word = self.pubkey_commitment.into();
        let pubkey_committment_a = pubkey_commitment.a;
        let pubkey_committment_b = pubkey_commitment.b;
        let pubkey_committment_c = pubkey_commitment.c;
        let pubkey_committment_d = pubkey_commitment.d;
        let msg = self.message_word();
        format!(
            "push.{}.{}.{}.{}.{pubkey_committment_d}.{pubkey_committment_c}.{pubkey_committment_b}.{pubkey_committment_a}",
            msg[3], msg[2], msg[1], msg[0]
        )
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
