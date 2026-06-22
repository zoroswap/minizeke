//! The transfer intent and its canonical, signable encoding.

use miden_protocol::crypto::hash::poseidon2::Poseidon2;
use miden_protocol::{Felt, Word};

/// Domain-separation tag — stops a signature for one action type being
/// replayed as another. See the cancel/withdraw tags in the perp repo.

/// A user's authorization to move `amount` to a recipient, bound to the
/// depositor's account id so the operator knows whose funds are moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Intent {
    pub sell_amount: u64,
    pub buy_amount: u64,
    pub user_suffix: u64,
    /// Depositor (User Account) id, low word.
    pub user_prefix: u64,
    pub sell_suffix: u64,
    pub sell_prefix: u64,
    pub buy_suffix: u64,
    pub buy_prefix: u64,
}

impl Intent {
    /// The exact field elements hashed to the signed Word.
    /// MUST match the TypeScript `intentFelts` ordering byte-for-byte.
    pub fn canonical_felts(&self) -> Vec<u64> {
        vec![
            self.sell_amount,
            self.buy_amount,
            self.user_suffix,
            self.user_prefix,
            self.sell_suffix,
            self.sell_prefix,
            self.buy_suffix,
            self.buy_prefix,
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
    let elements: Vec<Felt> = felts.iter().map(|&v| Felt::new(v)).collect();
    Poseidon2::hash_elements(&elements)
}
