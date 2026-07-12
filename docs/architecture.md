# Architecture

## Accounts and processes

### Vault

The vault is one public Miden network account. It is the custody boundary.

It holds all user and LP assets. It records cumulative user funding, initiated redeems, completed redeems, LP entitlements, and LP withdrawals. It also stores user public-key commitments and user-to-pool placement.

The vault accepts only the eight note scripts in `masm/notes/`. It does not use an operator key for normal user funding, redeem, or LP withdrawal. The `ADD_POOL` and `CHECKPOINT` scripts check that the note sender matches the configured operator account.

### Pool shards

Each pool shard is a public account controlled by an ECDSA single-signature key held by the server. A shard stores `bought` and `sold` counters for each allocated `(asset, user)` pair. It does not hold trading custody or curve state.

A swap runs with the shard as the native account. The transaction calls the vault as a foreign account to read fresh funding, redeem, pending-redeem, and public-key values. Both the sell and buy counters are changed in the same native account transaction.

### Operator

The operator is a server-controlled account. It sends two maintenance notes:

- `ADD_POOL` authorizes a pool and makes it active for later registrations.
- `CHECKPOINT` raises an LP's cumulative on-chain entitlement.

The operator cannot lower an entitlement. User and LP payout limits are enforced by vault code.

### Faucets

Each listed asset has a public fungible faucet account. `faucet_server` owns an independent Miden client and SQLite store. It shares the deployment keystore so it can sign faucet transactions.

The main server only proxies `POST /mint`. The faucet process limits requests by `(recipient, faucet)` in memory. The default amount is `10000000`; the default cooldown is 240 seconds.

### Oracle

`assets.toml` defines deployable symbols, decimals, and max supplies. Deployment fetches `ORACLE_URL/v1/price_feeds` and resolves each symbol from the feed's `attributes.base`. Assets missing from the oracle are rejected.

The resolved feed IDs are stored in deployment JSON. Server startup checks that every stored ID is still listed for the same symbol. It loads initial prices from `/v1/updates/price/latest`, then subscribes to `/v1/updates/price/stream` for only those IDs.

## Why the pool is sharded

A Miden account can use at most 255 storage slots. One pool shard uses:

- 248 generic asset-user cell slots.
- Five pool metadata slots.
- Two `AuthSingleSig` slots.
- No `BasicWallet` storage slots.

This is exactly `248 + 5 + 2 = 255`.

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

1. The client submits an order, serialized public key, and signature.
2. The server quotes against in-memory curve state and current oracle prices.
3. The server checks a lazy local balance mirror. This is only a pre-flight check.
4. The execution worker resolves the user's shard from public vault storage.
5. The shard transaction fetches fresh vault values by FPI.
6. The pool verifies the canonical eight-felt intent and checks the sell balance.
7. The pool increments `sold` for the sell asset and `bought` for the buy asset.

The on-chain FPI check and signature check are authoritative. The server quote determines `buy_amount`; the signed intent binds that amount.

### Redeem

Redeem has two vault-native notes.

1. `INIT_REDEEM` checks the pool-derived available balance by FPI and increments cumulative initiated redeems.
2. `REDEEM` checks the pending amount and pool-derived balance, increments cumulative completed redeems, and creates a P2ID payout note.

Pending redeems reduce the amount available to later swaps before the payout is completed.

## LP flows

### Deposit

A `DEPOSIT` note carries an asset into the vault. The vault increases the LP's entitlement by the principal. The server applies curve deposit math and mints LP shares in an in-memory ledger.

`deposit_pools` records each successful seed deposit in deployment JSON. Startup replays those records to rebuild initial curve state and LP shares for the deployment LP.

### Checkpoint

The server periodically values each in-memory LP position. If `withdrawn + current value` is above the on-chain entitlement, the operator sends a `CHECKPOINT` note. The vault only accepts non-decreasing values.

### Withdraw

The server validates and burns LP shares, checkpoints first if needed, then sends a `WITHDRAW` note. The vault permits at most `entitlement - withdrawn`, increments `withdrawn`, and creates a P2ID payout.

The on-chain entitlement counters preserve the last checkpointed withdrawal right. LP share ownership and current curve valuation remain server-side.

## Batches and failures

The processor handles one logical batch at a time. It quotes orders sequentially and applies each accepted quote to its in-memory balances and curve state before processing the next order.

The execution worker groups accepted orders by assigned shard. It submits shard groups sequentially in deployment order. Every order in one shard is a call in one transaction, so that shard group succeeds atomically or fails as a unit.

A failed placement lookup fails only that order. A failed shard transaction marks every order in that shard failed. Earlier shard transactions stay submitted, and later shard groups are still attempted. There is no cross-shard atomic transaction.

The current processor does not roll back its precomputed in-memory balance or curve changes when a later shard submission fails. Pool curve state also is not persisted after swaps. Restart reconstructs it from deployment deposit records, not past swaps.

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

Each new `(asset, user)` pair touched by a swap consumes one of 248 cells. A swap can allocate two cells. Reusing an existing pair consumes no new cell. At capacity, allocation raises `POOL: maximum asset-user cells reached`, which fails the whole shard transaction.

Capacity does not trigger automatic shard creation or user migration. Operators must add a shard before assigning more users, based on expected asset use.

Pool and vault FPI procedure roots are stored when accounts are built. Allowed vault note roots are also fixed at deployment. Changes to account MASM or note scripts are incompatible with already deployed accounts. Create a fresh deployment after such changes. A schema-v2 JSON file alone does not upgrade on-chain code.
