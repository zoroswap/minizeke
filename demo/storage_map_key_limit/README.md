# Demo: `storage_map_key` limit via FPI

```sh
cargo run --bin demo_storage_map_key_limit
```

Triggers the real FPI foreign-account prefetch (`ForeignAccount::public` → `execute_transaction` → `get_account` with `StorageMapFetch::Slots`):

1. **64 keys** → FPI fetch OK  
2. **65 keys** → `parameter storage_map_key exceeded limit 64: 65`
