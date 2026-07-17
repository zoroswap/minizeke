# Architecture

## Accounts and processes

### Vault

The vault is one public Miden network account. It is the custody boundary.

It holds all user and LP assets. It records cumulative user funding, initiated redeems, completed redeems, LP entitlements, and LP withdrawals. It also stores user public-key commitments and user-to-pool placement.

The vault accepts only the eight note scripts in `masm/notes/`. It does not use an operator key for normal user funding, redeem, or LP withdrawal. The `ADD_POOL` and `CHECKPOINT` scripts check that the note sender matches the configured operator account.

### Pool shards

Each pool shard is a public account controlled by an ECDSA single-signature key held by the server. A shard stores `bought` and `sold` counters for each allocated `(asset, user)` pair plus an authenticated map of consumed client order UUIDs. It does not hold trading custody or curve state.

A swap runs with the shard as the native account. The transaction calls the vault as a foreign account to read fresh funding, redeem, pending-redeem, and public-key values. Both the sell and buy counters are changed in the same native account transaction.

### Operator

The operator is a server-controlled account. It sends two maintenance notes:

- `ADD_POOL` authorizes a pool and makes it active for later registrations.
- `CHECKPOINT` raises an LP's cumulative on-chain entitlement.

The operator cannot lower an entitlement. User and LP payout limits are enforced by vault code.

### Faucets

Each listed asset has a public fungible faucet account. `faucet_server` owns an independent Miden client and SQLite store. It shares the deployment keystore so it can sign faucet transactions.

Public `POST /mint` is disabled by default. If explicitly enabled, the main server
proxies only to a loopback faucet and authenticates with a scoped primary/next service
credential; direct unauthenticated mint calls are rejected. The faucet process limits
accepted requests by `(recipient, faucet)` in memory. The default amount is `10000000`;
the default cooldown is 240 seconds.

Automatic fee updates and manual fee administration use separate primary/next
credentials on a second loopback-only listener. The public listener does not install
those routes.

### Oracle

`assets.toml` defines deployable symbols, decimals, and max supplies. Deployment fetches `ORACLE_URL/v1/price_feeds` and resolves each symbol from the feed's `attributes.base`. Assets missing from the oracle are rejected.

The resolved feed IDs are stored in deployment JSON. Server startup checks that every stored ID is still listed for the same symbol. It loads initial prices from `/v1/updates/price/latest`, then subscribes to `/v1/updates/price/stream` for only those IDs.

## Why the pool is sharded

A Miden account can use at most 255 storage slots. One pool shard uses:

- 247 generic asset-user cell slots.
- Six pool metadata slots, including the consumed-order map.
- Two `AuthSingleSig` slots.
- No `BasicWallet` storage slots.

This is exactly `247 + 6 + 2 = 255`.

One swap must update both the sell and buy legs in one native account. A user is therefore assigned to one shard, and all asset cells for that user are allocated there. Splitting the two legs across accounts would lose the single native-account transaction boundary.

## Registration and placement

`spawn` deploys and authorizes the first shard. `spawn_pool` deploys another shard, authorizes it, and replaces the vault's active-pool value.

The `REGISTER` note stores the user's public-key commitment and copies the current active pool into the user's placement map. Registration fails if the user already exists or the active pool is not authorized.

Placement is permanent in the current code. Adding a shard does not move existing users. Later registrations go to the new active shard. There is no load balancer or migration procedure.

`GET /users/{id}/placement` reads public vault storage from RPC, validates the assigned pool against the authorization map, then reads each configured asset's cell allocation from that shard. A `null` cell means that no swap has allocated it yet.

## User flows

### Fund

1. A user creates a public `FUND` network note carrying one asset.
2. The vault receives the note asset.
3. The vault adds its amount to the user's cumulative funding map.

The derived balance is:

`funding + bought - sold - redeemed`

Available balance subtracts pending redeems:

`balance - (initiated_redeems - redeemed)`

### Swap

1. The client submits a v2 order with a signed client UUID and expiry, serialized public key, and signature.
2. The API verifies the purpose/domain/network/user/assets/amounts/UUID/expiry signature and durably reserves the client UUID before publishing a separate server lifecycle ID. Identical retries return that ID; conflicting reuse returns 409.
3. The server quotes against in-memory curve state and current oracle prices after rechecking expiry.
4. The server checks a lazy local balance mirror. This is only a pre-flight check.
5. The execution worker resolves the user's shard from public vault storage.
6. The shard transaction fetches fresh vault values by FPI.
7. The pool verifies the canonical sixteen-felt intent, chain-time expiry, and unused UUID, then checks the sell balance.
8. The pool increments `sold` and `bought` and consumes the UUID atomically.

The on-chain FPI check and signature check are authoritative. The server quote determines `buy_amount`; the signed intent binds that amount.

### Redeem

Redeem has two vault-native notes.

1. `INIT_REDEEM` checks the pool-derived available balance by FPI and increments cumulative initiated redeems.
2. `REDEEM` checks the pending amount and pool-derived balance, increments cumulative completed redeems, and creates a P2ID payout note.

Pending redeems reduce the amount available to later swaps before the payout is completed.

## LP flows

### Deposit

A permissionless `DEPOSIT` note carries an asset into the vault. The LP creates and
submits the note from its own account; the server never holds LP keys. The vault increases
the LP's entitlement by the principal.

The dedicated LP worker discovers consumed DEPOSIT notes, journals each note ID/nullifier,
and sends a short accounting event to Processing. Processing prices the deposit against
the curve state at that point and mints shares exactly once. Quotes returned before note
submission are informational execution-time quotes. Deposits below
`LP_MIN_DEPOSIT_AMOUNT` receive no shares and remain recoverable through the vault
entitlement.

`deposit_pools` records seed deposits in deployment JSON. Startup replays those records,
then the applied LP journal, to rebuild LP state.

### Checkpoint

The LP worker periodically values each persisted LP position. If `withdrawn + current
value` is above the on-chain entitlement, the operator sends a `CHECKPOINT` note. The
vault only accepts non-decreasing values. The worker stores the checkpoint's share count,
value, and withdrawn counter for offline partial-withdrawal recovery.

### Withdraw

The LP submits `WITHDRAW` directly. The vault permits at most
`entitlement - withdrawn`, increments `withdrawn`, and creates a P2ID payout. This remains
available while every server process is offline.

After restart, the LP worker syncs consumed WITHDRAW notes in chain order. Processing
burns the checkpoint-equivalent shares only after the withdrawal is confirmed. The
durable journal deduplicates replay by note ID/nullifier, and vault counters are the final
consistency check.

LP RPC synchronization and operator checkpoint waits run on a dedicated current-thread
runtime with `lp.<network-store>` and never run in the swap execution loop.

## Batches and failures

The processor handles one logical batch at a time. It quotes orders sequentially and applies each accepted quote to its in-memory balances and curve state before processing the next order.

The execution worker groups accepted orders by assigned shard. It submits shard groups sequentially in deployment order. Every order in one shard is a call in one transaction, so that shard group succeeds atomically or fails as a unit.

A failed placement lookup fails only that order. A failed shard transaction marks every order in that shard failed. Earlier shard transactions stay submitted, and later shard groups are still attempted. There is no cross-shard atomic transaction.

Accepted swaps are journaled as proposed and become `Submitted` only after the node accepts the
transaction and its transaction ID, submission/expiration heights, expected pool-account
commitments, and serialized local-store update have been persisted. A submitted transaction is
`Confirmed` only after `miden-client` synchronization reports that exact transaction record as
`Committed` and its account ID plus initial/final account commitments match the persisted
submission. This is chain-observed inclusion according to the Miden 0.15 client API, not a claim
of a separate confirmation depth or stronger probabilistic finality.

Local-store apply is retried from the durable serialized update before each sync. A client-reported
discard is terminal. A transaction that remains pending past its expiration-height grace, the
configured wall-clock timeout, or the configured retry limit is terminally failed. Until
`Confirmed`, order states are owner-private and no public trade, trade candle, trading statistic,
trade WebSocket event, analytics swap, or finalized pool snapshot is produced. Failed shards
reverse their recorded balance and curve deltas. Once the logical batch resolves, finalized pool
snapshots are committed to `execution.<network>.sqlite3`; restart overlays these snapshots on the
liquidity-derived baseline.

## Oracle delivery and volatility fees

The oracle broker remains full-rate for quoting, history, depth, and volatility estimation.
Only WebSocket forwarding is coalesced per asset: one update per second by default, with an
immediate bypass at a 20-basis-point movement and a trailing flush of the newest pending tick.

Dynamic risk pricing runs in the standalone `fee_updater` binary. It ports the reference
EWMA/log-fee policy and sends authenticated, idempotent fee batches to the server. Fee state is
durable, versioned, applied between logical batches, and always has an expiry (600 seconds by
default). Expiry removes volatility fee in/out but never removes static swap, backstop, or
protocol fees. `/pools/info` and `pool_state` WebSockets publish the full fee state.

## Authentication and analytics

The vault-registered ECDSA trading key is also the HTTP identity root. A one-time
domain/network/user-bound Poseidon2 challenge creates a short-lived opaque session. The server
stores only token commitments and enforces ownership on private order, trade, LP, and analytics
routes. Order intents are verified off-chain before queue admission and again on-chain.

The analytics worker uses its own Miden client/store and scans consumed `FUND`,
`INIT_REDEEM`, and `REDEEM` notes idempotently. Finalized swaps, event-time oracle marks, LP cash
flows, pool snapshots, and exact fee components feed a SQLite WAC ledger. Responses include
coverage metadata so opening snapshots or missing historical marks are visible rather than
reported as exact history.

## Trust and verification

Users trust the server to quote fairly, choose the signed `buy_amount`, retain LP share state, checkpoint LP fees, and keep off-chain history and curve state available.

Users do not need to trust the server for these checks:

- The pool verifies the user's signature against the public-key commitment registered in the vault.
- The pool reads fresh vault totals and enforces available sell balance.
- The vault enforces pending redeem and LP withdrawal limits.
- Pool placement and trade counters are in public account storage.
- Vault funding, redeem, entitlement, and withdrawal counters are in public account storage.

Users can query `GET /users/{id}/placement`, then verify the returned vault assignment, pool cell IDs, `bought`, and `sold` against RPC account state. The endpoint is a convenience reader, not a proof source.

## Capacity and compatibility

Each new `(asset, user)` pair touched by a swap consumes one of 247 cells. A swap can allocate two cells. Reusing an existing pair consumes no new cell. At capacity, allocation raises `POOL: maximum asset-user cells reached`, which fails the whole shard transaction.

Capacity does not trigger automatic shard creation or user migration. Operators must add a shard before assigning more users, based on expected asset use.

Pool and vault FPI procedure roots are stored when accounts are built. Allowed vault note roots are also fixed at deployment. Changes to account MASM or note scripts are incompatible with already deployed accounts. Create a fresh deployment after such changes. A schema-v3 JSON file alone does not upgrade on-chain code.
