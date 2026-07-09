use std::{fs::read_to_string, path::PathBuf, sync::OnceLock};

use anyhow::{Result, anyhow};
use miden_client::{
    account::{Account, StorageSlotName},
    assembly::CodeBuilder,
};
use miden_protocol::account::component::AccountComponentCode;

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

static VAULT_COMPONENT_CODE: OnceLock<AccountComponentCode> = OnceLock::new();

/// Canonical vault component library — shared by account deploy and note scripts.
pub fn vault_component_code() -> &'static AccountComponentCode {
    VAULT_COMPONENT_CODE.get_or_init(|| {
        let code = read_masm_file(&["accounts", "vault.masm"]).expect("vault.masm");
        let cb = link_all_libraries_for_vault(CodeBuilder::new()).expect("vault libs");
        cb.compile_component_code("zoro_miden::vault", &code)
            .expect("vault component")
    })
}

pub fn link_all_note_libraries(code_builder: CodeBuilder) -> Result<CodeBuilder> {
    let mut code_builder = link_all_libraries_for_vault(code_builder)?;
    code_builder.link_static_library(vault_component_code().as_library())?;
    Ok(code_builder)
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
