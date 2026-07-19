use std::{fs::read_to_string, path::PathBuf};

use anyhow::{Result, anyhow};
use miden_assembly::Library;
use miden_client::{
    account::{Account, StorageSlotName},
    assembly::CodeBuilder,
};
use miden_core::Word;
use miden_protocol::account::AccountComponentCode;

pub fn read_masm_file(path_steps: &[&str]) -> Result<String> {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let path = PathBuf::from_iter(
        [manifest_dir, "masm"]
            .into_iter()
            .chain(path_steps.iter().copied()),
    );
    read_to_string(&path).map_err(|e| anyhow!("Error reading MASM file at path {path:?}: {e:?}"))
}

pub fn storage_slot_name(name: &str) -> StorageSlotName {
    StorageSlotName::new(name).expect("valid slot name")
}

pub fn link_pool(mut code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let pool_code = read_masm_file(&["accounts", "pool.masm"])?;
    code_builder.link_module("zoro_miden::pool", &pool_code)?;
    Ok(code_builder)
}

pub fn link_vault(mut code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let vault_code = read_masm_file(&["accounts", "vault.masm"])?;
    code_builder.link_module("zoro_miden::vault", &vault_code)?;
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

pub fn link_operator(mut code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let math_code = read_masm_file(&["accounts", "operator.masm"])?;
    code_builder.link_module("zoro_miden::operator", &math_code)?;
    Ok(code_builder)
}

pub fn link_note_common_lib(code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let mut code_builder = code_builder.clone();
    let note_common_lib_code = read_masm_file(&["lib", "common.masm"])?;
    code_builder.link_module("zoro_miden::note::common", &note_common_lib_code)?;
    Ok(code_builder)
}

pub fn link_asset_utils_lib(code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let mut code_builder = code_builder.clone();
    let lib_code = read_masm_file(&["lib", "asset_utils.masm"])?;
    code_builder.link_module("zoro_miden::lib::asset_utils", &lib_code)?;
    Ok(code_builder)
}

pub fn link_output_note_utils_lib(code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let mut code_builder = code_builder.clone();
    let lib_code = read_masm_file(&["lib", "output_note_utils.masm"])?;
    code_builder.link_module("zoro_miden::lib::output_note_utils", &lib_code)?;
    Ok(code_builder)
}

pub fn link_all_libraries_for_vault(code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let code_builder = link_storage_utils(code_builder)?;
    let code_builder = link_asset_utils_lib(code_builder)?;
    let code_builder = link_note_common_lib(code_builder)?;
    let code_builder = link_output_note_utils_lib(code_builder)?;
    Ok(code_builder)
}

pub fn link_all_note_libraries(code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let code_builder = link_all_libraries_for_vault(code_builder)?;
    let code_builder = link_vault(code_builder)?;
    Ok(code_builder)
}

/// Compiles the vault component code (no storage). Used both to build the actual vault
/// component and to extract FPI proc roots for the pool without a circular dependency
/// (MAST roots do not depend on storage).
pub fn compile_vault_code(code_builder: CodeBuilder) -> Result<AccountComponentCode> {
    let code = read_masm_file(&["accounts", "vault.masm"])?;
    let cb = link_all_libraries_for_vault(code_builder)?;
    Ok(cb.compile_component_code("zoro_miden::vault", code)?)
}

/// Compiles the pool component code (no storage). See [`compile_vault_code`].
pub fn compile_pool_code(code_builder: CodeBuilder) -> Result<AccountComponentCode> {
    let code = read_masm_file(&["accounts", "pool.masm"])?;
    let cb = link_math(code_builder)?;
    let cb = link_operator(cb)?;
    Ok(cb.compile_component_code("zoro_miden::pool", code)?)
}

fn proc_root(lib: &Library, path: &str) -> Result<Word> {
    lib.get_procedure_root_by_path(path)
        .ok_or_else(|| anyhow!("procedure {path} not found in compiled library"))
}

/// MAST root of `VAULT::get_user_trading_details`, the per-trader FPI getter the pool calls
/// during swaps. Stored in the pool's `zoropool::user_trading_details_proc_root` slot.
pub fn vault_trading_details_proc_root(code_builder: CodeBuilder) -> Result<Word> {
    let code = compile_vault_code(code_builder)?;
    proc_root(
        code.as_library(),
        "zoro_miden::vault::get_user_trading_details",
    )
}

/// MAST root of `POOL::get_user_asset_balance_details_with_vault_values`, the getter the
/// vault FPIs into during redeem flows. Stored in the vault's
/// `zorovault::user_pool_balance_details_proc_root` slot.
pub fn pool_balance_details_proc_root(code_builder: CodeBuilder) -> Result<Word> {
    let code = compile_pool_code(code_builder)?;
    proc_root(
        code.as_library(),
        "zoro_miden::pool::get_user_asset_balance_details_with_vault_values",
    )
}

pub fn print_contract_procedures(pool_contract: &Account) {
    println!("+++++Pool contract procedures");
    pool_contract.code().procedures().iter().for_each(|proc| {
        println!("Proc root: {:?} ", proc.mast_root().to_hex());
    });
}

pub fn print_library_exports(masm_lib: &Library) {
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
