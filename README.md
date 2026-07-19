# Bounded ingress and proxy policy

The server applies the same exact-match origin allowlist to HTTP and WebSocket
requests. Configure it with `CORS_ALLOWED_ORIGINS` (default:
`http://localhost:3000,http://localhost:5173`). Requests without an `Origin`
header remain valid for non-browser clients.

Client IPs come from the TCP peer by default. `X-Forwarded-For` is accepted only
when both `TRUST_PROXY_HEADERS=true` and
`TRUST_PROXY_TLS_TERMINATED=true`. Enable both only when the server is reachable
exclusively through a trusted TLS-terminating reverse proxy that overwrites
forwarded headers; never enable them on a directly reachable listener.

Ingress defaults are:

- `HTTP_MAX_BODY_BYTES=1048576`, `HTTP_REQUEST_TIMEOUT_SECS=15`
- `RATE_LIMIT_PUBLIC_PER_MINUTE=240`, `RATE_LIMIT_AUTH_PER_MINUTE=20`,
  `RATE_LIMIT_MUTATION_PER_MINUTE=60`, `RATE_LIMIT_MAX_KEYS=100000`
- `DB_MAX_CONCURRENCY=8`, `MIDEN_RPC_MAX_CONCURRENCY=8`
- `WS_QUEUE_CAPACITY=128`, `WS_GLOBAL_CONNECTION_CAP=2000`,
  `WS_PER_IP_CONNECTION_CAP=256`, `WS_MAX_MESSAGE_BYTES=65536`,
  `WS_MAX_SUBSCRIPTIONS=64`
- `WS_SESSION_RECHECK_SECS=30`, `WS_PING_INTERVAL_SECS=20`,
  `WS_PONG_TIMEOUT_SECS=60`, `WS_WRITE_TIMEOUT_SECS=10`,
  `AUTH_PURGE_INTERVAL_SECS=300`

Saturated rate limits return `429`; saturated bounded workers and WebSocket
capacity return `503`. Both include `Retry-After`.
# minizeke

minizeke is a Miden spot-swap prototype. A public network vault holds assets and user registrations. Public pool accounts hold per-user trade counters. The server quotes orders from oracle prices and an off-chain curve, then submits signed swaps to each user's assigned pool account.

## Trade flow

1. Deploy faucets, the vault, and the first pool shard.
2. Seed each asset through the vault.
3. Register a user and fund the vault with public network notes.
4. Authenticate with the vault-registered trading key, then submit a signed order with its Bearer session.
5. The server quotes the order and assigns it to the user's shard.
6. The pool verifies the sixteen-felt v2 intent, expiry, and unused client UUID, reads fresh vault totals by FPI, and updates both swap legs in one transaction.
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
ADMIN_SERVER_URL=127.0.0.1:7801
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
- `LP_DB_PATH`: durable LP journal path. Default: `lp.<network>.sqlite3`.
- `LOG_DIR`: daily diagnostic log directory. Default: `logs`.
- `LP_CHECKPOINT_INTERVAL_SECS`: LP entitlement checkpoint interval. Default: `600`.
- `LP_SYNC_INTERVAL_SECS`: interval for discovering consumed LP notes. Default: `2`.
- `LP_MIN_DEPOSIT_AMOUNT`: minimum permissionless deposit in base units. Default: `1`.
- `EXECUTION_DB_PATH`: finalized swap and pool-state journal. Default: `execution.<network>.sqlite3`.
- `MAX_ADMITTED_ORDERS`: max pre-execution backlog (`admitted` + `processing_claimed`); excess admits get `503 execution queue is full`. Default: `64`.
- `MAX_ADMITTED_AGE_MS`: max age of an admitted order before quote fails it as `stale_queue`. Default: `60000`.
- `MAX_ORDERS_PER_SHARD_TX`: soft cap on swaps packed into one pool transaction. Default: `16`.
- `FINALITY_TIMEOUT_SECS`: terminal timeout for a submitted transaction that remains unconfirmed. Default: `1800`.
- `FINALITY_RETRY_MS`: minimum interval between finality reconciliation attempts. Default: `500`.
- `FINALITY_RETRY_SECS`: legacy fallback for finality retry cadence (`secs * 1000`) when `FINALITY_RETRY_MS` is unset.
- `FINALITY_MAX_ATTEMPTS`: terminal reconciliation-attempt limit. Default: `900`.
- `MIDEN_DEBUG_MODE`: opt-in Miden VM debug execution (`1|true|yes`). Default: off (much faster).
- `ANALYTICS_DB_PATH`: WAC user and pool analytics journal. Default: `analytics.<network>.sqlite3`.
- `AUTH_DB_PATH`: wallet challenges and hashed opaque sessions. Default: `auth.<network>.sqlite3`.
- `FEE_DB_PATH`: volatility-fee batches and validity state. Default: `fees.<network>.sqlite3`.
- `ORACLE_WS_MIN_INTERVAL_MS` / `ORACLE_WS_BPS_THRESHOLD`: frontend oracle coalescing. Defaults: `1000` / `20`.
- `AUTH_DOMAIN`, `AUTH_CHALLENGE_TTL_SECS`, `AUTH_SESSION_TTL_SECS`: signed login domain and TTLs.
- `CORS_ALLOWED_ORIGINS`: comma-separated frontend origins.
- `PUBLIC_MINT_ENABLED`: public `/mint` proxy switch. Default: `false`.
- `FAUCET_SERVICE_TOKEN` / `FAUCET_SERVICE_TOKEN_NEXT`: scoped, rotatable credential
  used only from the main server to the loopback faucet.
- `FEE_UPDATER_TOKEN` / `FEE_UPDATER_TOKEN_NEXT`: automatic fee-updater credential.
- `FEE_ADMIN_TOKEN` / `FEE_ADMIN_TOKEN_NEXT`: separate manual override credential.
- `ADMIN_SERVER_URL`: private fee/admin listener. Default: `127.0.0.1:7801`; do not
  expose it through the public reverse proxy.
- `FAUCET_MINT_AMOUNT`: amount minted per request. Default: `10000000`.
- `FAUCET_MINT_COOLDOWN_SECS`: cooldown per recipient and faucet. Default: `240`.
- `FAUCET_BATCH_SIZE`: max concurrent mint requests packed into one faucet
  transaction per faucet id (standard multi-note `send_notes` mint). Default: `32`.
- `ASSETS_FILE`: deploy-time asset config. Default: `assets.toml`.

`simulate_traders` takes two numbers: `START` and `MAX` (defaults `20` `100`).
It pre-stages all `MAX` traders, starts `START`, then activates about eight equal
stages at one-minute intervals. Every trader submits every 10 seconds with 20%
jitter and tracks up to two unsettled orders over one persistent WebSocket. The
server default supports 256 same-host trader connections; set
`WS_PER_IP_CONNECTION_CAP` only when testing a larger `MAX`.

Completed cohorts are reused on later runs when the network, vault, and requested
`MAX` match. Simulator state, keys, and SQLite stores live under
`simulation_stores/`; delete that directory to intentionally create a fresh cohort.

```sh
cargo run --bin simulate_traders -- 20 100
```

## Deploy

`spawn` creates the operator, configured faucets, vault, first pool shard, authorization note, and schema-v3 deployment file:

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

The main process requires a valid schema-v3 deployment file and does not deploy accounts.

Run the isolated volatility-fee updater separately:

```sh
cargo run --bin fee_updater
```

Set `FEE_SERVER_URL=http://127.0.0.1:7801`. It uses the same EWMA, logarithmic fee curve, deadband, spike, and refresh policy as
`../fee_updater`, samples `FEE_ORACLE_FEED_ID`, and pushes one surge value to all configured
assets and both directions. Every update expires; `VALIDITY_PERIOD_SECS` defaults to `600`,
with refresh at `REFRESH_FRACTION=0.8`. If the updater stops, the server retains static base
fees and automatically clears expired surge fees.

## Authentication and private APIs

`POST /auth/challenge` accepts `{\"user_id\":\"0x...\"}`. The wallet signs the returned
Poseidon2 message with the trading key registered in the vault, then sends the base64 public
key and signature to `POST /auth/login`. Login returns a short-lived opaque Bearer token; only
its Poseidon2 commitment is stored.

Bearer authentication is required for order submission, private order/trade history, LP-owned
records, and `/users/me/*` analytics. Public market trades are redacted. WebSocket clients send
`{\"type\":\"Authenticate\",\"token\":\"...\"}` before subscribing to private `user`,
`order_updates`, or `analytics` channels.

Frontend analytics routes:

- `GET /users/me/analytics`, `/users/me/pnl`, `/users/me/positions`, `/users/me/events`
- `GET /pools/analytics`, `/pools/{faucet_id}/analytics`
- `GET /pools/info` and WebSocket `pool_state` include base fees, volatility fee in/out,
  source, version, precision, and `valid_until`.

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
  "schema_version": 3,
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

- `schema_version`: must be `3`.
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
- `POST /orders/new`: submit a v2 signed intent, base64 signature, and base64 public key.
- `POST /mint`: proxy a mint request to the faucet process.
- `POST /lp/deposits/note`: quote and build a self-custodial DEPOSIT note. The response
  contains a base64-encoded public note for the LP to submit from its own account.
- `GET /lp/operations/{note_id}`: confirmed/applied status from the durable LP journal.
- `GET /lp/positions/{lp_id}/{faucet_id}`: current shares and checkpoint snapshot.

LP shares use execution-time pricing. The quote returned while building a note is
informational; the confirmed note's chain order determines the minted shares.

Order intent v1 is rejected. A v2 request uses a client-signed UUID and a separate server
lifecycle ID:

```json
{"version":2,"client_order_id":"00112233-4455-6677-8899-aabbccddeeff","expires_at":1784304000,"details":{"asset_in":"0x...","amount_in":10,"asset_out":"0x...","min_amount_out":9},"order_type":"Spot","user_id":"0x...","signed_intent":"...","pubkey":"..."}
```

The signed Poseidon2 message binds the swap purpose, `minizeke` domain, `testnet` network,
user, both asset IDs, both amounts, client UUID, and expiry. An identical UUID retry returns
the original lifecycle ID; using that UUID for different signed fields returns HTTP 409.

The faucet process also exposes `GET /health` and `POST /mint`. A mint body is:

```json
{"address":"mtst1...","faucet_id":"0x..."}
```

`POST /mint` is disabled by default on the public API. When explicitly enabled, the
main server authenticates to the loopback faucet with `FAUCET_SERVICE_TOKEN`; direct
faucet mint calls require that same scoped Bearer credential. Primary and `_NEXT`
values are accepted during rotation. Fee updater and manual admin routes use different
credentials and are hosted only on `ADMIN_SERVER_URL`.

Consumed analytics and LP notes use durable `(block, note_id)` cursors and exact event
IDs, so each sync processes only the new ordered suffix while inserts remain idempotent.
Broker capacities are configurable with `BROKER_*_CAPACITY`. Order, LP, fee, and
execution notifications are durable-store wakeups; oracle, stats, and pool-state feeds
are best-effort latest-state updates. `/health` reports aggregate broker lag and
unobserved-send counters.

## Checks

```sh
cargo fmt --check
cargo check --all-targets
cargo test
cargo clippy --all-targets -- -D warnings
```

See [`docs/remediation-runbook.md`](docs/remediation-runbook.md) for the exact remediation
verification sequence, ignored load/remote-testnet harnesses, and orders/sec/socket metrics.

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
