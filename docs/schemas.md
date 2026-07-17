# Schemas

Words and stacks below are written left to right with the top element first. Account ID words use suffix before prefix unless stated otherwise.

## Key conventions

- Account key: `[account_suffix, account_prefix, 0, 0]`.
- User key: `[user_suffix, user_prefix, 0, 0]`.
- Pool key: `[pool_suffix, pool_prefix, 0, 0]`.
- Asset-user key: `[asset_suffix, asset_prefix, user_suffix, user_prefix]`.
- Asset-LP key: `[asset_suffix, asset_prefix, lp_suffix, lp_prefix]`.
- Compact fungible asset: `[faucet_suffix, faucet_prefix, callback_flag, amount]`.
- Slot ID word: `[slot_id_suffix, slot_id_prefix, 0, 0]`.
- Map counters use `[amount, 0, 0, 0]`.

Named storage slots use `StorageSlotName` IDs derived from the exact names below. MASM `word("name")` and Rust `storage_slot_name("name")` refer to the same ID.

## Vault storage

The vault component defines 11 slots.

### User accounting maps

- `zorovault::user_asset_total_funding`
  - key: asset-user key
  - value: `[cumulative_funding, 0, 0, 0]`
- `zorovault::user_asset_total_redeems`
  - key: asset-user key
  - value: `[cumulative_completed_redeems, 0, 0, 0]`
- `zorovault::user_asset_total_initiated_redeems`
  - key: asset-user key
  - value: `[cumulative_initiated_redeems, 0, 0, 0]`

`pending_redeem = cumulative_initiated_redeems - cumulative_completed_redeems`.

### Registration and placement

- `zorovault::user_pubkeys`
  - key: user key
  - value: four-felt ECDSA public-key commitment
  - zero word means unregistered
- `zorovault::authorized_pools`
  - key: pool key
  - value: `[1, 0, 0, 0]` when authorized
- `zorovault::user_pool`
  - key: user key
  - value: pool key
  - zero word means no assignment
- `zorovault::active_pool`
  - value: pool key used by later registrations
  - initial value: zero word

### Configuration

- `zorovault::operator_account_id`
  - value: `[operator_suffix, operator_prefix, 0, 0]`
- `zorovault::user_pool_balance_details_proc_root`
  - value: MAST root of `pool::get_user_asset_balance_details_with_vault_values`

### LP accounting maps

- `zorovault::lp_entitlements`
  - key: asset-LP key
  - value: `[cumulative_entitlement, 0, 0, 0]`
- `zorovault::lp_withdrawn`
  - key: asset-LP key
  - value: `[cumulative_withdrawn, 0, 0, 0]`

`withdrawable = cumulative_entitlement - cumulative_withdrawn`.

The account also includes `AuthNetworkAccount`, configured with the roots of all eight note scripts, and `BasicWallet`. Those are standard components, not part of the `zoro_miden::vault` slot list.

## Pool storage

The pool component defines 253 slots. `AuthSingleSig` adds two more, so the account uses all 255 slots. `BasicWallet` adds none.

### Generic cells

There are 247 value slots:

`zoropool::cell_0` through `zoropool::cell_246`

Each starts as `[0, 0, 0, 0]` and later stores:

`[bought, sold, 0, 0]`

### Metadata slots

- `zoropool::cell_slot_ids`
  - key: `[index, 0, 0, 0]`, where `0 <= index < 247`
  - value: slot ID word for `zoropool::cell_<index>`
  - fully populated at deployment
- `zoropool::cell_index`
  - key: asset-user key
  - value: allocated cell's slot ID word
  - missing key means no allocated cell and reads as zero counters
- `zoropool::next_cell`
  - value: `[next_index, 0, 0, 0]`
  - starts at zero
- `zoropool::vault_account_id`
  - value: `[vault_suffix, vault_prefix, 0, 0]`
- `zoropool::user_trading_details_proc_root`
  - value: MAST root of `vault::get_user_trading_details`
- `zoropool::consumed_orders`
  - authenticated map from the four-limb client UUID to `[1, 0, 0, 0]`

Allocation uses `cell_slot_ids[next_index]`, writes `cell_index[asset-user key]`, then increments `next_cell`. It requires `next_index < 247`.

The slot count is:

`247 cells + 6 pool metadata + 2 AuthSingleSig = 255`

A new swap can allocate its sell cell, buy cell, or both. The two asset-user keys are in the same shard.

## Note storage

Every note has exactly three storage words:

1. asset or action word
2. metadata word
3. beneficiary word

The beneficiary word is always `[beneficiary_suffix, beneficiary_prefix, 0, 0]`.

### REGISTER

- word 0: `PK_COMM = [pk0, pk1, pk2, pk3]`
- word 1: `[0, 0, 0, 0]`
- word 2: user ID word
- sender: user
- attached assets: none

### FUND

- word 0: `[0, 0, 0, 0]`
- word 1: `[0, 0, 0, 0]`
- word 2: user ID word
- sender: user
- attached assets: the fungible assets received by the vault; the script reads the first input asset

### INIT_REDEEM

- word 0: compact fungible asset for the requested amount
- word 1: `[0, 0, 0, 0]`
- word 2: user ID word
- sender: user
- attached assets: none

### REDEEM

- word 0: compact fungible payout asset
- word 1: `[0, p2id_tag, 0, 0]`
- word 2: user ID word
- sender: user
- attached assets: none
- output: P2ID note carrying word 0's asset

### WITHDRAW

- word 0: compact fungible payout asset
- word 1: `[0, p2id_tag, 0, 0]`
- word 2: LP ID word
- sender: LP
- attached assets: none
- output: P2ID note carrying word 0's asset

### DEPOSIT

- word 0: `[0, 0, 0, 0]`
- word 1: `[0, 0, 0, 0]`
- word 2: LP ID word
- sender: LP
- attached assets: one fungible asset received by the vault

### ADD_POOL

- word 0: pool key
- word 1: `[0, 0, 0, 0]`
- word 2: operator ID word
- sender: operator
- attached assets: none

### CHECKPOINT

- word 0: `[asset_suffix, asset_prefix, 0, new_entitlement]`
- word 1: LP ID word
- word 2: operator ID word
- sender: operator
- attached assets: none

All eight are public notes targeted at the vault with `NoteExecutionHint::Always`. Their script roots are allow-listed in the vault's network-account auth component.

## Runtime LP journal

Runtime LP accounting is stored in `lp.<network>.sqlite3` by default:

- `lp_operations`: one row per consumed DEPOSIT or WITHDRAW note, uniquely keyed by
  `note_id` and `nullifier`. Status advances from `confirmed` to `applied`, or `failed`.
- `lp_positions`: current shares plus the most recent checkpoint's shares, asset value,
  and cumulative withdrawn amount for each `(lp_id, faucet_id)`.
- `lp_meta.sync_cursor`: last consumed-note block scanned by the LP worker.

The note is the chain commit point. Replaying a confirmed note is safe because both the
journal and Processing deduplicate it before changing shares or curve state.

## Finalized execution, fees, and analytics

`execution.<network>.sqlite3` contains:

- `swap_accounting`: proposed/submitted/confirmed/failed swaps, oracle marks, quoted and credited output,
  retained surplus, fee version, and exact LP/backstop/protocol/volatility components.
- `pool_snapshots`: the latest finalized curve state per faucet, restored after restart.
- `execution_submissions`: node-accepted transaction IDs, shard/order membership,
  submission/expiration heights, expected initial/final account commitments, serialized Miden
  local-store updates, retry metadata, and submitted/confirmed/failed state.

`fees.<network>.sqlite3` contains versioned fee batches and their per-asset expansion. An
automatic or manual batch is atomic and idempotent by `batch_id`. The public fee precision is
`1_000_000`; one basis point is 100 units. Every volatility fee has `valid_until`. Expired state
is represented as zero volatility fee while static base fields remain unchanged.

`analytics.<network>.sqlite3` is an append-only idempotent event journal with projections for:

- weighted-average-cost user positions and realized/unrealized marked PnL;
- completed funding/redeem cash flows (`INIT_REDEEM` affects pending totals only);
- finalized swap volume, fills, and fees;
- LP deposits/withdrawals, pool NAV/TVL, inventory PnL, and fee totals;
- event-time oracle marks and explicit history coverage.

## Wallet session schema

`auth.<network>.sqlite3` stores one-time challenges and opaque sessions. The challenge message is
Poseidon2 over a domain-separation tag, length-prefixed domain and network, user suffix/prefix,
the vault pubkey commitment, a random 32-byte nonce, issue time, and expiry. Login accepts only
ECDSA k256-keccak keys whose commitment equals the user's current vault registration.

Bearer tokens are random 32-byte values returned once. Only a domain-separated Poseidon2 token
commitment is persisted. Private WebSocket subscriptions use an `Authenticate` client frame and
are bound to the session user.

`POST /lp/deposits/note` accepts:

```json
{"lp_id":"0x...","faucet_id":"0x...","amount":100000000}
```

It returns the note ID, a base64-encoded serialized public note, an informational expected
share amount, the configured minimum deposit, and `pricing: \"execution_time\"`.

## Signed intent

Intent v1 is rejected. The canonical testnet intent v2 is exactly sixteen felts:

1. `purpose = u64::from_be_bytes("ZKSWPV2\0")`
2. `domain = u64::from_be_bytes("minizeke")`
3. `network = u64::from_be_bytes("testnet\0")`
4. `user_suffix`
5. `user_prefix`
6. `sell_asset_suffix`
7. `sell_asset_prefix`
8. `sell_amount`
9. `buy_asset_suffix`
10. `buy_asset_prefix`
11. `buy_amount`
12-15. the signed client UUID as four big-endian `u32` limbs
16. `expires_at`, Unix seconds

Rust and MASM hash the same sixteen felts with `Poseidon2::hash_elements`. The API verifies
the signature before atomically reserving the client UUID in `execution.<network>.sqlite3`.
An identical retry returns the original server lifecycle ID; rebinding the UUID returns HTTP
409. Processing checks expiry again before quoting.

The pool verifier receives:

`[m0, ..., m15, PK_COMM]`

Its advice data for each order is:

`[PK[9], SIG[17]]`

The transaction script pushes:

`[purpose, domain, network, user_suffix, user_prefix, sell_asset_suffix, sell_asset_prefix, sell_amount, buy_asset_suffix, buy_asset_prefix, buy_amount, uuid0, uuid1, uuid2, uuid3, expires_at]`

and calls `pool::execute_swap`. The pool asserts the fixed purpose/domain/network, checks the
chain timestamp, and rejects a UUID already present in its authenticated
`zoropool::consumed_orders` map. The UUID is consumed atomically with the balance-counter writes.

## FPI interfaces

FPI calls use a 16-felt argument/result area. Padding fills the unused elements.

### Pool native to vault foreign

Procedure: `vault::get_user_trading_details`

Input:

`[asset_suffix, asset_prefix, user_suffix, user_prefix, pad(12)]`

Output:

`[total_funding, total_redeemed, pending_redeem, PK_COMM[4], pad(9)]`

The pool calls this through the root in `zoropool::user_trading_details_proc_root` and the account ID in `zoropool::vault_account_id`.

### Vault native to pool foreign

Procedure: `pool::get_user_asset_balance_details_with_vault_values`

Input:

`[user_suffix, user_prefix, asset_suffix, asset_prefix, total_funding, total_redeemed, pending_redeem, pad(9)]`

Output:

`[balance, available_balance, pad(14)]`

The vault resolves the foreign pool from `zorovault::user_pool`, checks `zorovault::authorized_pools`, and calls the root in `zorovault::user_pool_balance_details_proc_root`.

The pool computes:

- `balance = total_funding + bought - sold - total_redeemed`
- `available_balance = balance - pending_redeem`

## Deployment schema v3

Deploy-time assets come from `assets.toml`:

```toml
[[assets]]
symbol = "BTC"
decimals = 8
max_supply = 1_000_000_000_000_000_000
initial_liquidity = 10
```

`symbol` must be unique and present as `attributes.base` in `ORACLE_URL/v1/price_feeds`.
`initial_liquidity` is expressed in whole tokens and is scaled by `decimals` when seeding the
pool. `oracle_feed_id` is resolved from the oracle catalog; it is not copied into the TOML file.

```json
{
  "schema_version": 3,
  "network": "testnet",
  "operator_account_id": "0x...",
  "vault_id": "0x...",
  "assets": [
    {
      "faucet_id": "0x...",
      "symbol": "BTC",
      "decimals": 8,
      "oracle_feed_id": "..."
    },
    {"faucet_id": "0x...", "symbol": "ETH", "decimals": 8, "oracle_feed_id": "..."},
    {"faucet_id": "0x...", "symbol": "USDC", "decimals": 8, "oracle_feed_id": "..."}
  ],
  "pools": ["0x..."],
  "lp_account_id": "0x...",
  "deposits": [
    {
      "faucet_id": "0x...",
      "amount": 100000000
    }
  ]
}
```

- Account IDs serialize as hex strings.
- `schema_version` must equal `3`.
- `network` must equal the active network name.
- `assets` must contain at least two entries.
- `pools` must contain at least one account ID.
- `lp_account_id` is nullable and defaults to `null` when absent.
- `deposits` defaults to an empty array when absent.
- Deposit order is significant because startup replays it through curve deposit math.
