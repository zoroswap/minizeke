use std::{fs::read_to_string, path::PathBuf};

use anyhow::{Result, anyhow};
use miden_client::{
    Client,
    account::{
        Account, AccountBuilder, AccountComponent, AccountId, AccountType, StorageMap,
        StorageMapKey, StorageSlot, StorageSlotName, component::BasicWallet,
    },
    assembly::CodeBuilder,
    auth::{AuthScheme, AuthSecretKey, AuthSingleSig},
    keystore::{FilesystemKeyStore, Keystore},
    rpc::Endpoint,
    testing::account_id::{
        ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1, ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2,
    },
};
use miden_core::{Felt, Word, ZERO};
use miden_protocol::account::AccountComponentMetadata;
use rand::RngCore;

use miden_protocol::crypto::hash::poseidon2::Poseidon2;

/// Domain-separation tag — stops a signature for one action type being
/// replayed as another. See the cancel/withdraw tags in the perp repo.

/// A user's authorization to move `amount` to a recipient, bound to the
/// depositor's account id so the operator knows whose funds are moved.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Intent {
    pub user_suffix: u64,
    pub user_prefix: u64,
    pub sell_idx: u64,
    pub sell_amount: u64,
    pub user_suffix_2: u64,
    pub user_prefix_2: u64,
    pub buy_idx: u64,
    pub buy_amount: u64,
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
            self.user_prefix_2,
            self.user_suffix_2,
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

/// Build the operator account and register it in the genesis state of `chain`.
///
/// Because `MockChain` does not support adding accounts after genesis, this function rebuilds
/// the chain from scratch with the operator account included. Any previous state in `chain`
/// is replaced.
///
/// Slot 0 = `StorageMap` keyed by `user_id_word` → pubkey commitment (one entry per depositor).
/// Slots 1 and 2 are zeroed value slots (last_nonce, last_authorized).
///
/// `depositors` is a slice of `(user_id_word, pubkey_commitment)` pairs — one per depositor.
pub fn deploy_operator(chain: &mut MockChain, depositors: &[(Word, Word)]) -> DeployedOperator {
    let library = CodeBuilder::default()
        .compile_component_code("signed_intents::operator", OPERATOR_MASM)
        .expect("operator.masm must assemble");

    let keys_slot = StorageSlotName::new(DEPOSITOR_KEYS_SLOT).expect("slot name must parse");
    let nonce_slot = StorageSlotName::new(LAST_NONCE_SLOT).expect("slot name must parse");
    let auth_slot = StorageSlotName::new(LAST_AUTH_SLOT).expect("slot name must parse");

    let map = StorageMap::with_entries(
        depositors
            .iter()
            .map(|(uid, comm)| (StorageMapKey::new(*uid), *comm)),
    )
    .expect("depositor map must build");

    let component = AccountComponent::new(
        library,
        vec![
            StorageSlot::with_map(keys_slot, map),
            StorageSlot::with_value(nonce_slot, Word::from([0u32, 0, 0, 0])),
            StorageSlot::with_value(auth_slot, Word::from([0u32, 0, 0, 0])),
        ],
        AccountComponentMetadata::mock("signed_intents::operator"),
    )
    .expect("operator component must build");

    // Build the account via MockChainBuilder using Auth::IncrNonce, which gives each
    // transaction a nonce delta so the kernel does not reject it as a no-op.
    // We pre-build the Account with IncrNonce auth component.
    let account = {
        let (auth_component, _authenticator) = Auth::IncrNonce.build_component();
        AccountBuilder::new(rand::random())
            .storage_mode(AccountStorageMode::Public)
            .with_auth_component(auth_component)
            .with_component(component)
            .build_existing()
            .expect("operator account must build")
    };

    let account_id = account.id();

    // Rebuild the chain with the operator account in genesis state.
    let mut builder = MockChain::builder();
    builder
        .add_account(account)
        .expect("add account to builder must succeed");
    *chain = builder
        .build()
        .expect("chain must build with operator account");
}

pub fn build_pool_component(
    pool_0_balance: u64,
    pool_1_balance: u64,
    users: Vec<AccountId>,
    cb: CodeBuilder,
) -> Result<AccountComponent> {
    let code = read_masm_file(&["accounts", "pool.masm"])?;
    let cb = link_storage_utils(cb)?;
    let lib = cb.compile_component_code("zoro_miden::pool", &code)?;

    let asset0 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_1)?;
    let asset1 = AccountId::try_from(ACCOUNT_ID_PUBLIC_FUNGIBLE_FAUCET_2)?;
    let faucets = [asset0, asset1];
    let user_amount = 1_000;

    let pool_balance_0: Word = [
        Felt::new(pool_0_balance).unwrap(),
        Felt::ZERO,
        Felt::ZERO,
        Felt::ZERO,
    ]
    .into();

    let pool_balance_1: Word = [
        Felt::new(pool_1_balance).unwrap(),
        Felt::ZERO,
        Felt::ZERO,
        Felt::ZERO,
    ]
    .into();

    let user_balance: Word = [
        Felt::new(user_amount).unwrap(),
        Felt::new(user_amount).unwrap(),
        Felt::ZERO,
        Felt::ZERO,
    ]
    .into();

    let slot_names = get_user_balance_storage_slot_names();

    let component = AccountComponent::new(
        lib,
        slot_names[..users.len()]
            .iter()
            .map(|name| StorageSlot::with_value(name.clone(), user_balance.into()))
            .collect(),
        AccountComponentMetadata::new("zoro_miden::pool"),
    )?;

    Ok(component)
}

pub fn read_masm_file(path_steps: &[&str]) -> Result<String> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from_iter(
        [manifest_dir, "masm"]
            .into_iter()
            .chain(path_steps.iter().copied()),
    );
    read_to_string(&path).map_err(|e| anyhow!("Error reading MASM file at path {path:?}: {e:?}"))
}

fn n(name: &str) -> StorageSlotName {
    let name = StorageSlotName::new(name).expect("valid slot name");
    // println!("Slot name: {:?}, id: {:?}", name, name.id());
    name
}

pub fn link_pool(mut code_builder: CodeBuilder) -> Result<CodeBuilder> {
    //let mut code_builder = link_storage_utils(code_builder)?;
    let pool_code = read_masm_file(&["accounts", "pool.masm"])?;
    code_builder.link_module("zoro_miden::pool", &pool_code)?;
    Ok(code_builder)
}

pub fn link_storage_utils(code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let mut code_builder = link_math(code_builder)?;
    let storage_utils_code = read_masm_file(&["lib", "storage_utils.masm"])?;
    code_builder.link_module("zoro_miden::lib::storage_utils", &storage_utils_code)?;
    Ok(code_builder)
}

pub fn link_math(mut code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let math_code = read_masm_file(&["lib", "math.masm"])?;
    code_builder.link_module("zoro_miden::lib::math", &math_code)?;
    Ok(code_builder)
}

fn map_from(entries: &[(Word, u64)]) -> StorageMap {
    let mut map = StorageMap::new();
    for (k, v) in entries {
        map.insert(
            StorageMapKey::new(*k),
            [Felt::new(*v).unwrap(), ZERO, ZERO, ZERO].into(),
        )
        .expect("insert into map");
    }
    map
}

pub fn print_contract_procedures(pool_contract: &Account) {
    println!("+++++Pool contract procedures");
    pool_contract.code().procedures().iter().for_each(|proc| {
        println!("Proc root: {:?} ", proc.mast_root().to_hex());
    });
}

pub fn print_library_exports(masm_lib: &miden_assembly::Library) {
    println!("+++++Masm lib exports:");
    masm_lib.exports().for_each(|export| {
        let path = export.path();
        if let Some(root) = masm_lib.get_procedure_root_by_path(&path) {
            println!("Export: {:?} {:?} {:?}", path, root, root.to_hex());
        } else {
            println!("Export: {:?} (no procedure root)", path);
        }
    });
}
