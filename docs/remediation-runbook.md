# Remediation verification runbook

## Fast verification

Keep the existing dependency cache and target directory; these checks do not require a
deployment or network:

```sh
cargo fmt --check
cargo check --all-targets
cargo test --lib
cargo test intent::tests::masm_v3_hash_matches_rust_golden --lib -- --nocapture
cargo test --test remediation_harness -- --list
```

Run the local MASM integration suites when a funded local deployment is available:

```sh
MIDEN_NETWORK=localhost cargo test --test vault -- --nocapture
MIDEN_NETWORK=localhost cargo test --test vault_pool -- --ignored --nocapture
MIDEN_NETWORK=localhost cargo test --test swap -- --nocapture
```

The remote suites submit transactions and consume funded notes. Use a disposable deployment,
run one suite at a time, and preserve its logs and deployment schema version.

Pool/vault MASM changes (nonce windows, registration capacity) and deployment schema
bumps require a fresh deploy: `SPAWN_FORCE=1 cargo run --bin spawn`,
`cargo run --bin deposit_pools`, wipe local SQLite (`lp` / `execution` /
`pool_provision` / related `*.sqlite3`), and a server restart.

The server-owned pool provisioner journals work in `pool_provision.*.sqlite3`. After
restart it resumes in-flight provisions and never lets clients mutate topology.
`simulate_traders` must not call `spawn_pool` or rewrite `deployment.json`.

## Orders per second

Prepare an array of independently signed, unexpired v3 order requests. Every entry needs a
server-leased `client_order_id`; repeating an entry measures idempotent replay throughput rather
than new-order admission throughput.

```sh
LOAD_BASE_URL=http://127.0.0.1:7799 \
LOAD_ORDERS_JSON="$(cat /secure/path/signed-orders.json)" \
LOAD_REQUESTS=100 LOAD_CONCURRENCY=8 \
cargo test --test remediation_harness measure_v2_order_admissions_per_second \
  -- --ignored --nocapture
```

Record `orders/sec`, accepted, rate-limited, failed, attempted, and elapsed from the output.
Also capture `/health` before and after. Its `broker.lagged_messages` and
`broker.dropped_without_receivers` counters must not grow unexpectedly. A configured ingress
limit can intentionally produce `429`; bounded database/RPC saturation produces `503` with
`Retry-After`.

## Socket capacity

The lightweight harness holds TCP sockets open to expose listener/file-descriptor limits:

```sh
LOAD_SOCKET_TARGET=127.0.0.1:7799 LOAD_SOCKETS=2000 \
cargo test --test remediation_harness measure_concurrent_socket_capacity \
  -- --ignored --nocapture
```

Record `sockets_open`, process RSS, process open-file count, and the host file-descriptor limit.
For WebSocket policy verification, additionally open authenticated WebSockets from the load
driver and confirm the configured global/per-IP caps, bounded outbound queue, message-size
limit, subscription cap, session revalidation, ping/pong timeout, and exact origin allowlist.
Capacity rejection should be `503` with `Retry-After`.

## Remote testnet smoke

```sh
REMOTE_API_HEALTH_URL=https://example.invalid/health \
REMOTE_TESTNET_HEALTH_URL=https://testnet-health.example.invalid/health \
cargo test --test remediation_harness remote_testnet_health_smoke \
  -- --ignored --nocapture
```

This smoke test is deliberately read-only. Transactional testnet verification remains in the
`swap` and `vault_pool` suites and requires funded accounts, a matching schema-v3 deployment,
the configured prover, and reachable Miden/oracle services.

## Release evidence

Archive command lines, exit codes, elapsed times, Rust/Cargo versions, free disk space, network,
deployment schema, and ignored-test reasons. Treat disk quota, missing credentials/funds, and
remote service availability as environment blockers; do not mark a compile or deterministic
test failure as environmental.
