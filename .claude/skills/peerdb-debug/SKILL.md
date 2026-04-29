---
name: peerdb-debug
description: Debug peerdb-playground CDC flows by querying the metastore, source, and destination peers. Use when investigating a stuck flow, verifying replication parity, inspecting peer config, or comparing row counts between source and destination.
---

# Debugging peerdb flows

The repo ships three `just` recipes that wrap `docker compose exec` for the relevant databases:

- `just psql-meta "<sql>"` — peerdb's own metastore (Postgres at `peerdb-postgres`, db `peerdb`)
- `just psql-source "<sql>"` — Postgres source peer (db `source`)
- `just ch-dest "<sql>"` — ClickHouse destination peer (db `destination`)

Omit the SQL arg to drop into an interactive shell. Multi-word SQL must be quoted.

## Metastore schema (what to query)

Tables you'll actually care about:

**`peers`** — registered source/destination peers
```
id UUID, name TEXT, type SMALLINT, config BYTEA, created_at, updated_at
```
`type` is the `PeerType` enum: `1=POSTGRES`, `2=CLICKHOUSE`, `3=MYSQL`. `config` is encrypted protobuf — don't try to decode it from SQL.

**`cdc_flows`** — registered CDC flows
```
id UUID, name TEXT, source UUID→peers, destination UUID→peers,
config BYTEA, internal_version INT, status SMALLINT, created_at, updated_at
```
`status` is the `CdcFlowStatus` enum: `1=CREATED`, `2=SETUP`, `3=SNAPSHOT`, `4=CDC`, `5=PAUSED`, `6=FAILED`.

**`cdc_flow_source_checkpoints`** — last replication checkpoint per flow
```
flow_id UUID PK→cdc_flows, checkpoint TEXT, updated_at
```
For Postgres sources `checkpoint` is an LSN string. A stale `updated_at` here is a strong signal the flow is stuck.

## Common debugging recipes

### Is a flow stuck?
```bash
just psql-meta "
  select f.name, f.status, c.checkpoint, c.updated_at,
         now() - c.updated_at as staleness
  from cdc_flows f
  left join cdc_flow_source_checkpoints c on c.flow_id = f.id
  order by staleness desc nulls last;
"
```
Status `6` (FAILED) is the obvious red flag. Status `4` (CDC) with a `staleness` of minutes means the worker isn't advancing — check worker logs (`docker compose logs -f worker`).

### List all flows with their peer names
```bash
just psql-meta "
  select f.name as flow, sp.name as source, dp.name as dest,
         f.status, f.internal_version
  from cdc_flows f
  join peers sp on sp.id = f.source
  join peers dp on dp.id = f.destination;
"
```

### Postgres source: replication slot / publication state

peerdb names slots `peerdb_slot_<flow-id-underscored>` and publications `peerdb_pub_<flow-id>`.

```bash
# All slots and how far behind they are
just psql-source "
  select slot_name, active, restart_lsn, confirmed_flush_lsn,
         pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) as lag_bytes
  from pg_replication_slots
  where slot_name like 'peerdb_%';
"

# Tables in a publication
just psql-source "select * from pg_publication_tables where pubname like 'peerdb_%';"
```
An `active=false` slot or rapidly-growing `lag_bytes` is the signature of a dead/lagging worker.

### ClickHouse destination: read current state correctly

Destination tables use `ENGINE = ReplacingMergeTree(_version, _is_deleted)`. Naively `SELECT *` returns *all* historical versions including soft-deleted rows. Always either use `FINAL` or filter explicitly:

```bash
# Correct current state (slower but accurate without background merges)
just ch-dest "select * from users FINAL where _is_deleted = 0"

# Same idea, manual version
just ch-dest "
  select * from (
    select *, row_number() over (partition by id order by _version desc) as rn
    from users
  ) where rn = 1 and _is_deleted = 0
"
```
Plain `select count() from users` will overcount until merges run. Use `count() FINAL` or the row-number trick above.

### Compare source vs destination row counts
```bash
just psql-source "select count(*) from users"
just ch-dest "select count() from users FINAL where _is_deleted = 0"
```
A mismatch during/right after snapshot is normal (snapshot still running, or background merges pending). A persistent mismatch with the flow in CDC status is a real bug.

### Verify a specific row replicated
```bash
just psql-source "select * from users where id = 42"
just ch-dest "select * from users FINAL where id = 42"
```

## Gotchas

- `psql-meta` requires the main stack up (`just peerdb-up`); the others need `just peers-up`. If a recipe says `service "X" is not running`, that's why.
- ClickHouse uses `count()` not `count(*)`. SQL syntax differs in other small ways (no `RETURNING`, different date functions) — don't paste Postgres queries verbatim.
- Don't try to read `peers.config` or `cdc_flows.config` directly — they're encrypted protobuf. Use the gRPC API (`grpcurl ... peerdb.PeerdbService/...`) for that.
- Relative dates in LSN comparisons require `pg_current_wal_lsn()` to be on the *source*, not the metastore.
