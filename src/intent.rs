use miden_client::account::AccountId;
use miden_core::{Felt, Word};
use miden_protocol::crypto::hash::poseidon2::Poseidon2;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::nonce_window::nonce_from_client_order_id;

/// Breaking v3 wire/on-chain intent constants. These values are also asserted by
/// `pool.masm`; changing any of them requires deploying fresh pool accounts.
pub const INTENT_VERSION: u8 = 3;
pub const SWAP_PURPOSE_TAG: u64 = u64::from_be_bytes(*b"ZKSWPV3\0");
pub const INTENT_DOMAIN_TAG: u64 = u64::from_be_bytes(*b"minizeke");
pub const TESTNET_NETWORK_TAG: u64 = u64::from_be_bytes(*b"testnet\0");
pub const INTENT_FELT_COUNT: usize = 16;

/// Returns whether a Unix-seconds intent expiry has elapsed. Equality is expired.
pub const fn is_expired_at(expires_at: u64, unix_timestamp_seconds: u64) -> bool {
    unix_timestamp_seconds >= expires_at
}

/// The exact v3 authorization consumed by a pool. The server-issued client UUID is
/// represented as four 32-bit limbs so every value is a valid Miden field element.
/// Limb 0 carries the per-user order nonce; limbs 1..=3 remain random for API
/// idempotency.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Intent {
    pub purpose: u64,
    pub domain: u64,
    pub network: u64,
    pub user_suffix: u64,
    pub user_prefix: u64,
    pub sell_asset_suffix: u64,
    pub sell_asset_prefix: u64,
    pub sell_amount: u64,
    pub buy_asset_suffix: u64,
    pub buy_asset_prefix: u64,
    pub buy_amount: u64,
    pub client_order_id: [u64; 4],
    pub expires_at: u64,
}

impl Intent {
    pub fn new_swap(
        user_id: AccountId,
        sell_asset: AccountId,
        sell_amount: u64,
        buy_asset: AccountId,
        buy_amount: u64,
        client_order_id: Uuid,
        expires_at: u64,
    ) -> Self {
        Self {
            purpose: SWAP_PURPOSE_TAG,
            domain: INTENT_DOMAIN_TAG,
            network: TESTNET_NETWORK_TAG,
            user_suffix: user_id.suffix().as_canonical_u64(),
            user_prefix: user_id.prefix().as_u64(),
            sell_asset_suffix: sell_asset.suffix().as_canonical_u64(),
            sell_asset_prefix: sell_asset.prefix().as_u64(),
            sell_amount,
            buy_asset_suffix: buy_asset.suffix().as_canonical_u64(),
            buy_asset_prefix: buy_asset.prefix().as_u64(),
            buy_amount,
            client_order_id: uuid_felts(client_order_id),
            expires_at,
        }
    }

    /// The exact field elements hashed to the signed Word.
    /// MUST match the TypeScript `intentFelts` ordering byte-for-byte.
    pub fn canonical_felts(&self) -> [u64; INTENT_FELT_COUNT] {
        [
            self.purpose,
            self.domain,
            self.network,
            self.user_suffix,
            self.user_prefix,
            self.sell_asset_suffix,
            self.sell_asset_prefix,
            self.sell_amount,
            self.buy_asset_suffix,
            self.buy_asset_prefix,
            self.buy_amount,
            self.client_order_id[0],
            self.client_order_id[1],
            self.client_order_id[2],
            self.client_order_id[3],
            self.expires_at,
        ]
    }

    /// The Word the user signs.
    pub fn message_word(&self) -> Word {
        message_word(&self.canonical_felts())
    }

    pub fn client_order_uuid(&self) -> Uuid {
        uuid_from_felts(self.client_order_id)
    }

    /// Server-allocated per-user order nonce encoded in UUID limb 0.
    pub fn order_nonce(&self) -> u32 {
        nonce_from_client_order_id(self.client_order_uuid())
    }
}

/// Hash a canonical felt vector to the signable Word.
///
/// Uses Poseidon2 (the protocol's canonical `Hasher`), which is the algebraic hash the
/// Miden VM reconstructs on-chain via the native `hperm` instruction. The authorizer
/// component rebuilds this exact Word inside the transaction to verify the signature;
/// using any other algebraic hash (e.g. RPO) would be impossible to reproduce on-chain
/// in this toolchain, since the VM exposes no RPO permutation instruction.
pub fn message_word(felts: &[u64]) -> Word {
    let elements: Vec<Felt> = felts.iter().map(|&v| Felt::new(v).unwrap()).collect();
    Poseidon2::hash_elements(&elements)
}

pub fn uuid_felts(id: Uuid) -> [u64; 4] {
    let bytes = id.as_bytes();
    [
        u64::from(u32::from_be_bytes(bytes[0..4].try_into().unwrap())),
        u64::from(u32::from_be_bytes(bytes[4..8].try_into().unwrap())),
        u64::from(u32::from_be_bytes(bytes[8..12].try_into().unwrap())),
        u64::from(u32::from_be_bytes(bytes[12..16].try_into().unwrap())),
    ]
}

pub fn uuid_from_felts(limbs: [u64; 4]) -> Uuid {
    let mut bytes = [0_u8; 16];
    for (index, limb) in limbs.into_iter().enumerate() {
        let limb = u32::try_from(limb)
            .expect("v3 UUID limbs are always unsigned 32-bit integers")
            .to_be_bytes();
        bytes[index * 4..index * 4 + 4].copy_from_slice(&limb);
    }
    Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod tests {
    use miden_assembly::Assembler;
    use miden_core_lib::CoreLibrary;
    use miden_processor::{
        DefaultHost, ExecutionOptions, StackInputs, advice::AdviceInputs, execute_sync,
    };

    use super::*;

    fn golden_intent() -> Intent {
        Intent {
            purpose: SWAP_PURPOSE_TAG,
            domain: INTENT_DOMAIN_TAG,
            network: TESTNET_NETWORK_TAG,
            user_suffix: 11,
            user_prefix: 12,
            sell_asset_suffix: 21,
            sell_asset_prefix: 22,
            sell_amount: 23,
            buy_asset_suffix: 31,
            buy_asset_prefix: 32,
            buy_amount: 33,
            // Limb 0 is the order nonce; remaining limbs stay random/idempotent.
            client_order_id: [0x0000_0007, 0x4455_6677, 0x8899_aabb, 0xccdd_eeff],
            expires_at: 1_800_000_000,
        }
    }

    #[test]
    fn v3_intent_encoding_and_message_are_golden() {
        let intent = golden_intent();
        assert_eq!(intent.order_nonce(), 7);
        assert_eq!(
            intent.canonical_felts(),
            [
                6_506_385_721_141_900_032, // ZKSWPV3\0
                7_883_954_021_992_852_325,
                8_387_236_824_952_960_000,
                11,
                12,
                21,
                22,
                23,
                31,
                32,
                33,
                0x0000_0007,
                0x4455_6677,
                0x8899_aabb,
                0xccdd_eeff,
                1_800_000_000,
            ]
        );
        let message = intent.message_word();
        let elements = message.as_elements();
        let actual = [
            elements[0].as_canonical_u64(),
            elements[1].as_canonical_u64(),
            elements[2].as_canonical_u64(),
            elements[3].as_canonical_u64(),
        ];
        // Captured once from Poseidon2; keep in sync with TypeScript golden vectors.
        assert_eq!(
            actual,
            [
                14_679_449_024_156_102_030,
                11_802_393_995_137_049_376,
                13_991_692_530_890_841_766,
                14_202_507_866_894_551_169,
            ]
        );
    }

    #[test]
    fn masm_v3_hash_matches_rust_golden() {
        let operator_source = include_str!("../masm/accounts/operator.masm");
        assert!(operator_source.contains("loc_storew_le.0 dropw"));
        assert!(!operator_source.contains("\n    loc_storew_be."));

        let intent = golden_intent();
        let felts = intent.canonical_felts();
        let reversed = felts
            .iter()
            .rev()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(".");
        let source = format!(
            r#"
            use miden::core::crypto::hashes::poseidon2
            use miden::core::sys

            @locals(16)
            proc hash_intent
                loc_storew_le.0 dropw
                loc_storew_le.4 dropw
                loc_storew_le.8 dropw
                loc_storew_le.12 dropw
                push.16 locaddr.0
                exec.poseidon2::hash_elements
            end

            begin
                push.{reversed}
                exec.hash_intent
                exec.sys::truncate_stack
            end
            "#
        );
        let mut assembler = Assembler::default();
        assembler
            .link_static_library(CoreLibrary::default().library())
            .unwrap();
        let program = assembler.assemble_program(source).unwrap();
        let output = execute_sync(
            &program,
            StackInputs::default(),
            AdviceInputs::default(),
            &mut DefaultHost::default(),
            ExecutionOptions::default(),
        )
        .unwrap();

        assert_eq!(output.stack.get_word(0).unwrap(), intent.message_word());
    }

    #[test]
    fn uuid_limb_encoding_is_big_endian_and_lossless() {
        let id = Uuid::parse_str("00112233-4455-6677-8899-aabbccddeeff").unwrap();
        let limbs = [0x0011_2233, 0x4455_6677, 0x8899_aabb, 0xccdd_eeff];
        assert_eq!(uuid_felts(id), limbs);
        assert_eq!(uuid_from_felts(limbs), id);
    }

    #[test]
    fn expiry_uses_strict_unix_seconds_ordering_on_and_off_chain() {
        assert!(!is_expired_at(1_800_000_001, 1_800_000_000));
        assert!(is_expired_at(1_800_000_000, 1_800_000_000));
        assert!(is_expired_at(1_799_999_999, 1_800_000_000));

        // Miden block timestamps are Unix seconds. With stack [timestamp, expiry], the swap makes
        // `lt` evaluate timestamp < expiry; omitting it reverses the comparison.
        let pool_source = include_str!("../masm/accounts/pool.masm");
        assert!(pool_source.contains("exec.tx::get_block_timestamp\n    # => [block_timestamp_unix_seconds, expires_at_unix_seconds]\n"));
        assert!(pool_source.contains("swap lt assert.err=ERR_INTENT_EXPIRED"));
    }

    #[test]
    fn purpose_tag_is_v3() {
        assert_eq!(&SWAP_PURPOSE_TAG.to_be_bytes(), b"ZKSWPV3\0");
        assert_eq!(INTENT_VERSION, 3);
    }
}
