use std::{fs::read_to_string, path::PathBuf};

use anyhow::{Result, anyhow};
use miden_client::{
    Felt, Word,
    account::AccountId,
    assembly::CodeBuilder,
    asset::FungibleAsset,
    note::{
        NetworkAccountTarget, Note, NoteAssets, NoteAttachment, NoteAttachments, NoteExecutionHint,
        NoteRecipient, NoteStorage, NoteTag, NoteType, PartialNoteMetadata,
    },
};
use miden_protocol::note::NoteScript;
use rand::{Rng, SeedableRng, rngs::StdRng};
use tracing::info;

use crate::{assembly_utils::link_all_note_libraries, asset_utils::asset_to_word};

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum NoteKind {
    Register,
    Fund,
    InitRedeem,
    Redeem,
    Withdraw,
    Deposit,
    AddPool,
    Checkpoint,
}

impl NoteKind {
    pub const NETWORK_KINDS: [Self; 8] = [
        Self::Register,
        Self::Fund,
        Self::InitRedeem,
        Self::Redeem,
        Self::Withdraw,
        Self::Deposit,
        Self::AddPool,
        Self::Checkpoint,
    ];

    pub fn masm_name(&self) -> &str {
        match self {
            NoteKind::Register => "REGISTER.masm",
            NoteKind::Fund => "FUND.masm",
            NoteKind::InitRedeem => "INIT_REDEEM.masm",
            NoteKind::Redeem => "REDEEM.masm",
            NoteKind::Deposit => "DEPOSIT.masm",
            NoteKind::Withdraw => "WITHDRAW.masm",
            NoteKind::AddPool => "ADD_POOL.masm",
            NoteKind::Checkpoint => "CHECKPOINT.masm",
        }
    }
}

#[derive(Clone, Debug)]
pub struct ZekeNote {
    note: Note,
    note_kind: NoteKind,
    serial_number: Word,
}

pub enum ZekeNoteInstructions {
    Register(RegisterInstructions),
    Fund(FundInstructions),
    InitRedeem(InitRedeemInstructions),
    Redeem(RedeemInstructions),
    Deposit(DepositInstructions),
    Withdraw(WithdrawInstructions),
    AddPool(AddPoolInstructions),
    Checkpoint(CheckpointInstructions),
}

pub struct RegisterInstructions {
    pub user_id: AccountId,
    pub vault_id: AccountId,
    /// Commitment of the user's trading pubkey, stored in the vault's registration map.
    pub pubkey_commitment: Word,
}

pub struct FundInstructions {
    pub user_id: AccountId,
    pub vault_id: AccountId,
    pub note_assets: Vec<FungibleAsset>,
}

pub struct InitRedeemInstructions {
    pub user_id: AccountId,
    pub vault_id: AccountId,
    pub min_expected_asset: FungibleAsset,
}

pub struct RedeemInstructions {
    pub user_id: AccountId,
    pub vault_id: AccountId,
    pub min_expected_asset: FungibleAsset,
}

/// LP liquidity deposit: the note carries `asset` into the vault; the vault credits the
/// LP's entitlement and the server mints LP shares off-chain.
pub struct DepositInstructions {
    pub lp_id: AccountId,
    pub vault_id: AccountId,
    pub asset: FungibleAsset,
}

/// Self-custodial LP liquidity withdrawal: no assets attached; the vault checks
/// `amount <= entitlement - withdrawn` and pays out `asset_out` via P2ID to the LP.
pub struct WithdrawInstructions {
    pub lp_id: AccountId,
    pub vault_id: AccountId,
    pub asset_out: FungibleAsset,
}

pub struct AddPoolInstructions {
    pub operator_id: AccountId,
    pub vault_id: AccountId,
    pub pool_id: AccountId,
}

pub struct CheckpointInstructions {
    pub operator_id: AccountId,
    pub vault_id: AccountId,
    pub asset_id: AccountId,
    pub lp_id: AccountId,
    pub new_entitlement: u64,
}

impl ZekeNote {
    pub fn get_note_script(code_builder: CodeBuilder, note_file_name: &str) -> Result<NoteScript> {
        let manifest_dir = env!("CARGO_MANIFEST_DIR");
        let note_path = PathBuf::from_iter(&[manifest_dir, "masm", "notes", note_file_name]);
        // let pool_path = PathBuf::from_iter(&[manifest_dir, "masm", "accounts", "zoropool.masm"]);
        let note_code = read_to_string(&note_path)
            .map_err(|e| anyhow!("Error parsing note code at path {note_path:?}: {e:?}"))?;

        let code_builder = link_all_note_libraries(code_builder.clone())?;
        code_builder
            .compile_note_script(note_code)
            .map_err(|e| anyhow!("Failed to compile note script: {}", e))
    }

    pub fn new(note_instructions: ZekeNoteInstructions, code_builder: CodeBuilder) -> Result<Self> {
        let note_kind;
        let vault_id;
        let sender_id;
        let mut note_assets = None;
        let mut note_storage_builder = NoteStorageBuilder::default();
        match note_instructions {
            ZekeNoteInstructions::Register(instructions) => {
                vault_id = instructions.vault_id;
                sender_id = instructions.user_id;
                note_kind = NoteKind::Register;
                note_storage_builder = note_storage_builder
                    .with_asset_compact(instructions.pubkey_commitment)
                    .with_beneficiary(instructions.user_id);
            }
            ZekeNoteInstructions::Fund(instructions) => {
                vault_id = instructions.vault_id;
                sender_id = instructions.user_id;
                note_assets = Some(instructions.note_assets);
                note_kind = NoteKind::Fund;
                note_storage_builder = note_storage_builder.with_beneficiary(instructions.user_id);
            }
            ZekeNoteInstructions::InitRedeem(instructions) => {
                vault_id = instructions.vault_id;
                sender_id = instructions.user_id;
                note_kind = NoteKind::InitRedeem;
                note_storage_builder =
                    note_storage_builder.with_asset(instructions.min_expected_asset);
                note_storage_builder = note_storage_builder.with_beneficiary(instructions.user_id);
            }
            ZekeNoteInstructions::Redeem(instructions) => {
                vault_id = instructions.vault_id;
                sender_id = instructions.user_id;
                note_storage_builder =
                    note_storage_builder.with_asset(instructions.min_expected_asset);
                note_kind = NoteKind::Redeem;
                note_storage_builder = note_storage_builder
                    .with_beneficiary(instructions.user_id)
                    .with_p2id_tag(NoteTag::with_account_target(instructions.user_id));
            }
            ZekeNoteInstructions::Deposit(instructions) => {
                vault_id = instructions.vault_id;
                sender_id = instructions.lp_id;
                note_assets = Some(vec![instructions.asset]);
                note_kind = NoteKind::Deposit;
                note_storage_builder = note_storage_builder.with_beneficiary(instructions.lp_id);
            }
            ZekeNoteInstructions::Withdraw(instructions) => {
                vault_id = instructions.vault_id;
                sender_id = instructions.lp_id;
                note_kind = NoteKind::Withdraw;
                note_storage_builder = note_storage_builder
                    .with_asset(instructions.asset_out)
                    .with_beneficiary(instructions.lp_id)
                    .with_p2id_tag(NoteTag::with_account_target(instructions.lp_id));
            }
            ZekeNoteInstructions::AddPool(instructions) => {
                vault_id = instructions.vault_id;
                sender_id = instructions.operator_id;
                note_kind = NoteKind::AddPool;
                note_storage_builder = note_storage_builder
                    .with_asset_compact(Word::from([
                        instructions.pool_id.suffix(),
                        instructions.pool_id.prefix().as_felt(),
                        Felt::ZERO,
                        Felt::ZERO,
                    ]))
                    .with_beneficiary(instructions.operator_id);
            }
            ZekeNoteInstructions::Checkpoint(instructions) => {
                vault_id = instructions.vault_id;
                sender_id = instructions.operator_id;
                note_kind = NoteKind::Checkpoint;
                note_storage_builder = note_storage_builder
                    .with_asset_compact(Word::from([
                        instructions.asset_id.suffix(),
                        instructions.asset_id.prefix().as_felt(),
                        Felt::ZERO,
                        Felt::new(instructions.new_entitlement)?,
                    ]))
                    .with_metadata(Word::from([
                        instructions.lp_id.suffix(),
                        instructions.lp_id.prefix().as_felt(),
                        Felt::ZERO,
                        Felt::ZERO,
                    ]))
                    .with_beneficiary(instructions.operator_id);
            }
        }
        let note_storage = note_storage_builder.build()?;
        let serial_number = random_word();
        let note_script = Self::get_note_script(code_builder, note_kind.masm_name())?;
        let note_metadata = PartialNoteMetadata::new(sender_id, NoteType::Public)
            .with_tag(NoteTag::with_account_target(vault_id));
        let recipient = NoteRecipient::new(serial_number, note_script, note_storage);
        let note_assets = NoteAssets::new(
            note_assets
                .unwrap_or(Vec::new())
                .into_iter()
                .map(FungibleAsset::into)
                .collect(),
        )?;
        let network_target = NetworkAccountTarget::new(vault_id, NoteExecutionHint::Always)?;
        let note = Note::with_attachments(
            note_assets,
            note_metadata,
            recipient,
            NoteAttachments::from(NoteAttachment::from(network_target)),
        );

        Ok(Self {
            note,
            serial_number,
            note_kind,
        })
    }

    pub fn note(&self) -> &Note {
        &self.note
    }

    pub fn print_note_info(&self) {
        info!(
            "View note on MidenScan: https://testnet.midenscan.com/note/{}",
            self.note.id().to_hex()
        );
    }
}

fn random_word() -> Word {
    let mut rng = StdRng::from_os_rng();
    let felts = [
        Felt::new_unchecked(rng.random::<u64>() >> 1),
        Felt::new_unchecked(rng.random::<u64>() >> 1),
        Felt::new_unchecked(rng.random::<u64>() >> 1),
        Felt::new_unchecked(rng.random::<u64>() >> 1),
    ];
    Word::new(felts)
}

#[derive(Clone, Debug, Default)]
pub struct NoteStorageBuilder {
    beneficiary: Option<AccountId>,
    asset: Option<Word>,
    metadata: Option<Word>,
}

impl NoteStorageBuilder {
    pub fn with_asset(mut self, asset: FungibleAsset) -> Self {
        self.asset = Some(asset_to_word(asset));
        self
    }
    pub fn with_asset_compact(mut self, asset: Word) -> Self {
        self.asset = Some(asset);
        self
    }

    pub fn with_metadata(mut self, metadata: Word) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_deadline(mut self, deadline: u64) -> Result<Self> {
        if let Some(metadata) = self.metadata {
            self.metadata =
                Some([deadline.try_into()?, metadata[1], metadata[2], metadata[3]].into())
        } else {
            self.metadata = Some([deadline.try_into()?, Felt::ZERO, Felt::ZERO, Felt::ZERO].into())
        }
        Ok(self)
    }

    pub fn with_p2id_tag(mut self, tag: NoteTag) -> Self {
        if let Some(metadata) = self.metadata {
            self.metadata = Some([metadata[0], tag.into(), metadata[2], metadata[3]].into())
        } else {
            self.metadata = Some([Felt::ZERO, tag.into(), Felt::ZERO, Felt::ZERO].into())
        }
        self
    }

    pub fn with_min_amount(mut self, min_amount: u64) -> Result<Self> {
        if let Some(metadata) = self.metadata {
            self.metadata = Some(
                [
                    metadata[0],
                    metadata[1],
                    min_amount.try_into()?,
                    metadata[3],
                ]
                .into(),
            )
        } else {
            self.metadata =
                Some([Felt::ZERO, Felt::ZERO, min_amount.try_into()?, Felt::ZERO].into())
        }
        Ok(self)
    }

    pub fn with_beneficiary(mut self, beneficiary: AccountId) -> Self {
        self.beneficiary = Some(beneficiary);
        self
    }

    pub fn build(self) -> Result<NoteStorage> {
        let asset = self.asset.unwrap_or(Word::new([Felt::ZERO; 4]));
        let metadata = self.metadata.unwrap_or(Word::new([Felt::ZERO; 4]));
        let beneficiary = self
            .beneficiary
            .ok_or_else(|| anyhow!("Note builder missing beneficiary."))?;

        Ok(NoteStorage::new(vec![
            asset[0],
            asset[1],
            asset[2],
            asset[3],
            metadata[0],
            metadata[1],
            metadata[2],
            metadata[3],
            beneficiary.suffix(),
            beneficiary.prefix().into(),
            Felt::ZERO,
            Felt::ZERO,
        ])?)
    }
}

#[cfg(test)]
mod tests {
    use miden_client::testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE,
        ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2,
    };

    use super::*;

    #[tokio::test]
    async fn test_build_register_note() -> Result<()> {
        let user_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE)?;
        let vault_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2)?;
        let code_builder = CodeBuilder::new();
        ZekeNote::new(
            ZekeNoteInstructions::Register(RegisterInstructions {
                user_id,
                vault_id,
                pubkey_commitment: Word::new([Felt::new(7).unwrap(); 4]),
            }),
            code_builder.clone(),
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn test_build_fund_note() -> Result<()> {
        let user_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE)?;
        let vault_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2)?;
        let faucet_id = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
        let code_builder = CodeBuilder::new();
        ZekeNote::new(
            ZekeNoteInstructions::Fund(FundInstructions {
                user_id,
                vault_id,
                note_assets: vec![FungibleAsset::new(faucet_id, 199)?],
            }),
            code_builder.clone(),
        )?;
        Ok(())
    }
    #[tokio::test]
    async fn test_build_init_redeem() -> Result<()> {
        let user_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE)?;
        let vault_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2)?;
        let faucet_id = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
        let code_builder = CodeBuilder::new();
        ZekeNote::new(
            ZekeNoteInstructions::InitRedeem(InitRedeemInstructions {
                user_id,
                vault_id,
                min_expected_asset: FungibleAsset::new(faucet_id, 100)?,
            }),
            code_builder.clone(),
        )?;
        Ok(())
    }
    #[tokio::test]
    async fn test_build_redeem() -> Result<()> {
        let user_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE)?;
        let vault_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2)?;
        let faucet_id = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
        let code_builder = CodeBuilder::new();
        ZekeNote::new(
            ZekeNoteInstructions::Redeem(RedeemInstructions {
                user_id,
                vault_id,
                min_expected_asset: FungibleAsset::new(faucet_id, 100)?,
            }),
            code_builder.clone(),
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn test_build_deposit() -> Result<()> {
        let lp_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE)?;
        let vault_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2)?;
        let faucet_id = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
        let code_builder = CodeBuilder::new();
        ZekeNote::new(
            ZekeNoteInstructions::Deposit(DepositInstructions {
                lp_id,
                vault_id,
                asset: FungibleAsset::new(faucet_id, 500)?,
            }),
            code_builder.clone(),
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn test_build_withdraw() -> Result<()> {
        let lp_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE)?;
        let vault_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2)?;
        let faucet_id = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
        let code_builder = CodeBuilder::new();
        ZekeNote::new(
            ZekeNoteInstructions::Withdraw(WithdrawInstructions {
                lp_id,
                vault_id,
                asset_out: FungibleAsset::new(faucet_id, 100)?,
            }),
            code_builder.clone(),
        )?;
        Ok(())
    }

    #[tokio::test]
    async fn test_build_add_pool_network_note() -> Result<()> {
        let operator_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE)?;
        let vault_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2)?;
        let note = ZekeNote::new(
            ZekeNoteInstructions::AddPool(AddPoolInstructions {
                operator_id,
                vault_id,
                pool_id: operator_id,
            }),
            CodeBuilder::new(),
        )?;
        assert_eq!(
            NetworkAccountTarget::try_from(note.note().attachments())?.target_id(),
            vault_id
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_build_checkpoint_network_note() -> Result<()> {
        let operator_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE)?;
        let vault_id = AccountId::try_from(ACCOUNT_ID_REGULAR_PUBLIC_ACCOUNT_IMMUTABLE_CODE_2)?;
        let asset_id = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
        let note = ZekeNote::new(
            ZekeNoteInstructions::Checkpoint(CheckpointInstructions {
                operator_id,
                vault_id,
                asset_id,
                lp_id: operator_id,
                new_entitlement: 123,
            }),
            CodeBuilder::new(),
        )?;
        assert_eq!(
            NetworkAccountTarget::try_from(note.note().attachments())?.target_id(),
            vault_id
        );
        Ok(())
    }
}
