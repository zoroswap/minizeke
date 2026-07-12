# minizeke

minizeke is a Miden spot-swap prototype. A public network vault holds assets and user registrations. Public pool accounts hold per-user trade counters. The server quotes orders from oracle prices and an off-chain curve, then submits signed swaps to each user's assigned pool account.

## Trade flow

1. Deploy faucets, the vault, and the first pool shard.
2. Seed each asset through the vault.
3. Register a user and fund the vault with public network notes.
4. Submit an order with the user's public key and signature.
5. The server quotes the order and assigns it to the user's shard.
6. The pool verifies the eight-felt intent, reads fresh vault totals by FPI, and updates both swap legs in one transaction.
7. Redeem in two notes: initiate the redeem, then consume the redeem note to receive a P2ID payout.

## Prerequisites

- Rust with the 2024 edition toolchain
- Access to a Miden localhost, devnet, or testnet node
- A working oracle HTTPS endpoint and SSE stream
- `curl` for API checks

Copy the environment template:

```sh
cp .env.example .env
```

Required server values:

```dotenv
SERVER_URL=127.0.0.1:7799
FAUCET_SERVER_URL=127.0.0.1:7800
ORACLE_URL=https://oracle.zoroswap.com
MIDEN_NETWORK=testnet
```

`MIDEN_NETWORK` accepts `localhost`, `devnet`, or `testnet`. It defaults to `testnet`. `DEPLOYMENT_FILE` overrides `deployment.<network>.json`.

Optional values:

- `TX_PROVER_URL`: remote transaction prover. Devnet and testnet use the network prover by default.
- `TX_PROVER_TIMEOUT_SECS`: prover timeout. Default: `30`.
- `NETWORK_NOTE_TIMEOUT_SECS`: network-note wait timeout.
- `HISTORY_DB_PATH`: history SQLite path. Default: `history.<network>.sqlite3`.
- `LOG_DIR`: daily diagnostic log directory. Default: `logs`.
- `LP_CHECKPOINT_INTERVAL_SECS`: LP entitlement checkpoint interval. Default: `600`.
- `FAUCET_MINT_AMOUNT`: amount minted per request. Default: `10000000`.
- `FAUCET_MINT_COOLDOWN_SECS`: cooldown per recipient and faucet. Default: `240`.
- `ASSETS_FILE`: deploy-time asset config. Default: `assets.toml`.

## Deploy

`spawn` creates the operator, configured faucets, vault, first pool shard, authorization note, and schema-v2 deployment file:

```sh
cargo run --bin spawn
```

It refuses to overwrite an existing deployment file. To replace it:

```sh
SPAWN_FORCE=1 cargo run --bin spawn
```

Assets are defined in `assets.toml`. `ASSETS_FILE` can point to another file. The default config deploys BTC, ETH, and USDC:

```toml
[[assets]]
symbol = "BTC"
decimals = 8
max_supply = 1_000_000_000_000_000_000
initial_liquidity = 10
```

`spawn` fetches `ORACLE_URL/v1/price_feeds` and resolves each symbol to one feed ID. Deployment stops if the oracle does not list an asset. The resolved IDs are saved in the deployment file.

Seed every configured asset. The default amount is `100000000` base units per asset:

```sh
cargo run --bin deposit_pools
DEPOSIT_AMOUNT=500000000 cargo run --bin deposit_pools
```

`deposit_pools` creates or reuses the deployment LP account. Each asset is seeded with
`initial_liquidity * 10^decimals` base units. `DEPOSIT_AMOUNT` overrides that value in base
units for every asset. One deposit record is appended after each successful on-chain deposit.

## Run

Run the dedicated faucet process:

```sh
cargo run --bin faucet_server
```

Run the API, order processor, oracle client, and Miden execution worker:

```sh
cargo run
```

The main process requires a valid schema-v2 deployment file and does not deploy accounts.

## Extend a deployment

Deploy, seed, and record another asset:

```sh
ASSET_SYMBOL="$NEW_SYMBOL" \
DEPOSIT_AMOUNT=100000000 \
cargo run --bin add_asset
```

Add the asset to `assets.toml` first. `add_asset` reads its decimals and max supply from the file and resolves its feed from the oracle. The symbol must be offered by the configured oracle and must not already exist in the deployment.

The deposit is skipped when the deployment has no `lp_account_id`.

Deploy and authorize another pool shard:

```sh
cargo run --bin spawn_pool
```

This shard becomes the vault's active pool for later registrations. Existing users keep their recorded shard.

## Deployment file

Schema version 2 has this shape:

```json
{
  "schema_version": 2,
  "network": "testnet",
  "operator_account_id": "0x...",
  "vault_id": "0x...",
  "assets": [
    {
      "faucet_id": "0x...",
      "symbol": "ASTA",
      "decimals": 8,
      "oracle_feed_id": "..."
    },
    {"faucet_id": "0x...", "symbol": "ASTB", "decimals": 8, "oracle_feed_id": "..."}
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

- `schema_version`: must be `2`.
- `network`: must equal the active `MIDEN_NETWORK`.
- `operator_account_id`: account allowed to authorize pools and checkpoint LP entitlements.
- `vault_id`: public network account holding assets and custody counters.
- `assets`: at least two faucet descriptors.
- `assets[].faucet_id`: fungible faucet account ID in hex.
- `assets[].symbol`: display symbol.
- `assets[].decimals`: base-unit decimals used by the curve.
- `assets[].oracle_feed_id`: resolved from `/v1/price_feeds` at deploy time and used by both price update endpoints.
- `pools`: at least one authorized pool account ID.
- `lp_account_id`: seeding LP account ID or `null`.
- `deposits`: successful liquidity deposits in chain order.
- `deposits[].faucet_id`: deposited faucet ID.
- `deposits[].amount`: deposited base units.

Legacy and unversioned deployment files are rejected. Account code and stored FPI roots are fixed at deployment, so MASM changes require a fresh deployment.

## HTTP API

The main server exposes:

- `GET /health`: process health and timestamp.
- `GET /pools/info`: listed assets, curve state, and pool shard IDs.
- `GET /stats`: order and trading totals.
- `GET /candles`: candles by `pair`, `source`, interval, time range, and limit.
- `GET /trades`: trades by `pair`, cursor, and limit.
- `GET /orders`: order history filtered by user, status, cursor, and limit.
- `GET /orders/{id}`: one order.
- `GET /users/{id}/placement`: the user's vault-assigned shard and allocated asset cells.
- `GET /depth`: derived depth for hex `base` and `quote` faucet IDs.
- `GET /ws`: order, pool, oracle, user, and stats events.
- `POST /orders/new`: submit an order, base64 signature, and base64 public key.
- `POST /mint`: proxy a mint request to the faucet process.

The faucet process also exposes `GET /health` and `POST /mint`. A mint body is:

```json
{"address":"mtst1...","faucet_id":"0x..."}
```

## Checks

```sh
cargo fmt --check
cargo test
cargo clippy --all-targets -- -D warnings
```

```sh
curl http://127.0.0.1:7799/health
curl http://127.0.0.1:7800/health
```

## Layout

- `src/`: server, deployment, execution, storage readers, and curve code.
- `src/bin/`: deployment, liquidity, faucet, asset, and shard commands.
- `assets.toml`: deploy-time asset symbols, decimals, and max supplies.
- `masm/accounts/`: vault, pool, and signature-verifier components.
- `masm/notes/`: eight network-account note scripts.
- `masm/lib/`: shared MASM helpers.
- `tests/`: swap, vault, and vault-pool integration tests.
- `docs/architecture.md`: account roles, flows, sharding, failure behavior, and trust.
- `docs/schemas.md`: account storage, note storage, intents, and FPI stack contracts.
