//! Per-user 96-slot sliding nonce window used on-chain by pools and mirrored in Rust
//! for unit tests and diagnostics.
//!
//! Storage word layout (elements `[0..4]`):
//! `[base_nonce, bitmap0, bitmap1, bitmap2]` where bit `i` of the concatenated 96-bit
//! bitmap means nonce `base_nonce + i` has been consumed.

use anyhow::{Result, anyhow};
use miden_core::{Felt, Word};

/// Inclusive-exclusive window length retained per user.
pub const NONCE_WINDOW_SIZE: u32 = 96;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct NonceWindow {
    pub base_nonce: u32,
    pub bitmap0: u32,
    pub bitmap1: u32,
    pub bitmap2: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NonceWindowError {
    Stale,
    AlreadyConsumed,
}

impl NonceWindow {
    pub fn from_word(word: Word) -> Result<Self> {
        let e = word.as_elements();
        Ok(Self {
            base_nonce: felt_u32(e[0], "base_nonce")?,
            bitmap0: felt_u32(e[1], "bitmap0")?,
            bitmap1: felt_u32(e[2], "bitmap1")?,
            bitmap2: felt_u32(e[3], "bitmap2")?,
        })
    }

    pub fn to_word(self) -> Word {
        Word::new([
            Felt::new(u64::from(self.base_nonce)).unwrap(),
            Felt::new(u64::from(self.bitmap0)).unwrap(),
            Felt::new(u64::from(self.bitmap1)).unwrap(),
            Felt::new(u64::from(self.bitmap2)).unwrap(),
        ])
    }

    pub fn is_empty(self) -> bool {
        self == Self::default()
    }

    /// Marks `nonce` consumed, sliding the window forward when needed.
    pub fn consume(mut self, nonce: u32) -> Result<Self, NonceWindowError> {
        if nonce < self.base_nonce {
            return Err(NonceWindowError::Stale);
        }
        let mut offset = nonce - self.base_nonce;
        if offset >= NONCE_WINDOW_SIZE {
            let new_base = nonce - (NONCE_WINDOW_SIZE - 1);
            let shift = new_base - self.base_nonce;
            self.shift_right(shift);
            self.base_nonce = new_base;
            offset = nonce - self.base_nonce;
        }
        if self.bit_set(offset) {
            return Err(NonceWindowError::AlreadyConsumed);
        }
        self.set_bit(offset);
        Ok(self)
    }

    fn bit_set(self, offset: u32) -> bool {
        debug_assert!(offset < NONCE_WINDOW_SIZE);
        let (limb, bit) = match offset {
            0..=31 => (self.bitmap0, offset),
            32..=63 => (self.bitmap1, offset - 32),
            _ => (self.bitmap2, offset - 64),
        };
        (limb & (1_u32 << bit)) != 0
    }

    fn set_bit(&mut self, offset: u32) {
        let (limb_ref, bit) = match offset {
            0..=31 => (&mut self.bitmap0, offset),
            32..=63 => (&mut self.bitmap1, offset - 32),
            _ => (&mut self.bitmap2, offset - 64),
        };
        *limb_ref |= 1_u32 << bit;
    }

    fn shift_right(&mut self, shift: u32) {
        if shift == 0 {
            return;
        }
        if shift >= NONCE_WINDOW_SIZE {
            self.bitmap0 = 0;
            self.bitmap1 = 0;
            self.bitmap2 = 0;
            return;
        }
        let (b0, b1, b2) = (self.bitmap0, self.bitmap1, self.bitmap2);
        let (n0, n1, n2) = if shift >= 64 {
            let s = shift - 64;
            (b2 >> s, 0, 0)
        } else if shift >= 32 {
            let s = shift - 32;
            let n0 = (b1 >> s) | b2.checked_shl(32 - s).unwrap_or(0);
            let n1 = b2 >> s;
            (n0, n1, 0)
        } else {
            let s = shift;
            let n0 = (b0 >> s) | b1.checked_shl(32 - s).unwrap_or(0);
            let n1 = (b1 >> s) | b2.checked_shl(32 - s).unwrap_or(0);
            let n2 = b2 >> s;
            (n0, n1, n2)
        };
        self.bitmap0 = n0;
        self.bitmap1 = n1;
        self.bitmap2 = n2;
    }
}

fn felt_u32(felt: Felt, label: &str) -> Result<u32> {
    let value = felt.as_canonical_u64();
    u32::try_from(value).map_err(|_| anyhow!("{label} does not fit in u32: {value}"))
}

/// Builds a v3 client-order UUID whose first big-endian u32 limb is `nonce`.
pub fn client_order_id_for_nonce(nonce: u32, random_96_bits: [u8; 12]) -> uuid::Uuid {
    let mut bytes = [0_u8; 16];
    bytes[0..4].copy_from_slice(&nonce.to_be_bytes());
    bytes[4..16].copy_from_slice(&random_96_bits);
    uuid::Uuid::from_bytes(bytes)
}

/// Extracts the server-allocated order nonce from a v3 client-order UUID.
pub fn nonce_from_client_order_id(client_order_id: uuid::Uuid) -> u32 {
    let bytes = client_order_id.as_bytes();
    u32::from_be_bytes(bytes[0..4].try_into().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_duplicate_inside_window() {
        let window = NonceWindow::default()
            .consume(3)
            .unwrap()
            .consume(3)
            .unwrap_err();
        assert_eq!(window, NonceWindowError::AlreadyConsumed);
    }

    #[test]
    fn two_independent_users_share_nonce_values() {
        let a = NonceWindow::default().consume(7).unwrap();
        let b = NonceWindow::default().consume(7).unwrap();
        assert_eq!(a.bitmap0 & (1 << 7), 1 << 7);
        assert_eq!(b.bitmap0 & (1 << 7), 1 << 7);
    }

    #[test]
    fn accepts_out_of_order_within_window() {
        let window = NonceWindow::default()
            .consume(5)
            .unwrap()
            .consume(1)
            .unwrap()
            .consume(4)
            .unwrap();
        assert_eq!(window.base_nonce, 0);
        assert_eq!(window.bitmap0 & (1 << 1), 1 << 1);
        assert_eq!(window.bitmap0 & (1 << 4), 1 << 4);
        assert_eq!(window.bitmap0 & (1 << 5), 1 << 5);
    }

    #[test]
    fn slides_and_rejects_old_nonce() {
        let window = NonceWindow::default().consume(100).unwrap();
        assert_eq!(window.base_nonce, 100 - (NONCE_WINDOW_SIZE - 1));
        assert_eq!(
            window.consume(window.base_nonce - 1).unwrap_err(),
            NonceWindowError::Stale
        );
        let again = window.consume(100).unwrap_err();
        assert_eq!(again, NonceWindowError::AlreadyConsumed);
    }

    #[test]
    fn window_boundaries() {
        let mut window = NonceWindow::default();
        for nonce in 0..NONCE_WINDOW_SIZE {
            window = window.consume(nonce).unwrap();
        }
        assert_eq!(window.base_nonce, 0);
        assert_eq!(window.bitmap0, u32::MAX);
        assert_eq!(window.bitmap1, u32::MAX);
        assert_eq!(window.bitmap2, u32::MAX);

        window = window.consume(NONCE_WINDOW_SIZE).unwrap();
        assert_eq!(window.base_nonce, 1);
        assert!(window.consume(0).is_err());
    }

    #[test]
    fn many_orders_keep_constant_size_word() {
        let mut window = NonceWindow::default();
        for nonce in 0..10_000_u32 {
            window = window.consume(nonce).unwrap();
        }
        let word = window.to_word();
        let round_trip = NonceWindow::from_word(word).unwrap();
        assert_eq!(round_trip, window);
        assert_eq!(window.base_nonce, 10_000 - NONCE_WINDOW_SIZE);
    }

    #[test]
    fn client_order_id_embeds_nonce() {
        let id = client_order_id_for_nonce(0xAABB_CCDD, [1; 12]);
        assert_eq!(nonce_from_client_order_id(id), 0xAABB_CCDD);
        assert_eq!(&id.as_bytes()[4..], &[1; 12]);
    }
}
