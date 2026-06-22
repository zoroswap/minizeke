# Background and Motivation

The current request is a barebones POC for performance measurement. `asm/pool.masm` should be standalone, with no FPI/vault synchronization references, and pool state should be reduced to balances only.

# Key Challenges and Analysis

- Keep the edit scoped to `asm/pool.masm`.
- Remove vault/FPI and lazy accounting paths so balances are local-only.
- Preserve simple raw balance and swap behavior for the POC.

# High-level Task Breakdown

- [x] Update `asm/pool.masm` to remove vault/FPI references and lazy accounting.
  - Success criteria: no FPI/vault symbols remain in `asm/pool.masm`; user balance reads use raw local state.
- [x] Reduce pool state storage to balances only.
  - Success criteria: pool balance map stores `[poolBalance, 0, 0, 0]` and no `poolState` field remains.
- [x] Run lightweight verification.
  - Success criteria: search confirms removed symbols are gone and lints/tests are checked where available.

# Project Status Board

- [x] Executor: Apply barebones pool POC changes.

# Executor's Feedback or Assistance Requests

Implemented the barebones standalone pool POC changes in `asm/pool.masm`.

Verification:
- `rg` found no remaining FPI/vault/accounting/pool-state symbols in `asm/pool.masm`.
- IDE lints report no issues for `asm/pool.masm`.
- `cargo test` passes, but it only ran the default Rust target with 0 tests.

# Lessons

- Read the file before editing it.
