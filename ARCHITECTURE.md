# PeerDB Playground — Architecture

CDC (Change Data Capture) tool that moves data from sources (Postgres, MySQL) to destinations (ClickHouse).

## Current State

- **Snapshot pipeline**: fully working end-to-end (Postgres/MySQL -> ClickHouse)
- **CDC streaming**: working end-to-end for Postgres -> ClickHouse and MySQL -> ClickHouse
- **Supported sources**: Postgres (logical replication), MySQL 8 (GTID + row-based binlog)
- **Supported destinations**: ClickHouse (ReplacingMergeTree)

## Project Structure

```
cmd/
  api/          — HTTP/gRPC API server (Connect RPC)
  worker/       — Temporal worker process
config/         — Config loading + JSON schema validation
connectors/     — Source/destination connector interfaces and implementations
  types/        — QType (column types) and QValue (column values) — source-agnostic type system
  postgres/     — Postgres source connector (publication, slot, snapshot, schema discovery)
  mysql/        — MySQL source connector (snapshot, schema discovery, GTID/binlog CDC)
  clickhouse/   — ClickHouse destination connector (table creation, batch writes)
e2e/            — End-to-end tests with testcontainers
errs/           — Application error types (maps to Connect RPC codes)
gen/            — Generated protobuf + Connect RPC code
middleware/     — HTTP interceptors (request ID, logging, error handling)
migrations/     — SQL migrations for peerdb metadata database
pkg/
  postgres/     — Postgres utilities (connect, publications, replication slots)
  mysql/        — MySQL utilities (connect, GTID/prerequisite inspection)
  clickhouse/   — ClickHouse utilities (connect)
  crypto/       — AES encryption for peer configs
proto/
  peerdb.proto    — Public API messages and service definition
  internal.proto  — Internal messages for Temporal serialization (TableSchema, QType)
server/         — Connect RPC handler implementations
services/
  peers/        — Peer CRUD (create, get, validate connections)
  flows/        — CDC flow CRUD (create, get, validate)
workflows/      — Temporal workflows and activities
docker-compose.yml — Local dev: pg-source, ch-dest, peerdb-postgres, temporal, temporal-ui
```

## Key Design Decisions

### Connector Interfaces

```go
type SourceConnector interface {
    Setup(ctx) error
    Read(ctx, ch chan<- RecordBatch) error   // Long-running CDC stream with reconnect/resume
    IsCriticalError(err error) bool          // Marks unrecoverable source failures
    Ack(ctx, position string) error          // Confirm processed source position
    GetTableSchemas(ctx) ([]TableSchema, error)
    SnapshotTable(ctx, table TableSchema) (<-chan RecordBatch, error)
    Teardown(ctx) error
    Close(ctx) error
}

type DestinationConnector interface {
    Setup(ctx, tables []TableSchema) error
    WriteBatch(ctx, batch RecordBatch) error
    Write(ctx, ch <-chan RecordBatch) error
    Teardown(ctx) error
    Close(ctx) error
}
```

- **Connectors are pure** — no dependency on proto/generated code, no knowledge of flow config
- **Source and destination don't know about each other** — activities wire them together
- **Close vs Teardown**: Close releases connections (used after each activity). Teardown permanently removes resources (slot, publication, tables).

### Type System

- `QType` interface with concrete types per data type (`QTypeInt32`, `QTypeString`, `QTypeNumeric{Precision, Scale}`, etc.)
- `QValue` interface with concrete value wrappers (`QValueInt32{Val}`, `QValueString{Val}`, etc.)
- Source-agnostic: Postgres connector converts pgx types -> QValue, ClickHouse connector converts QValue -> ClickHouse native types
- `TableIdentifier{Schema, Name}` — proper struct instead of string, so destinations can use `.Name` without schema prefix

### Table Configuration

- `CdcFlowConfig.tables` — fully qualified names controlling source table scope. Postgres uses `schema.table`; MySQL uses `database.table`. Empty = all source tables for that peer.
- `CdcFlowConfig.table_mappings` — optional source->destination rename + column exclusion. Independent from publication.
- Postgres publication or MySQL table filtering is the source of truth for which tables are replicated.
- Column exclusion is applied at the activity layer (before calling `SnapshotTable`), not inside connectors.

### ClickHouse Destination

- Tables use `ReplacingMergeTree(_version)` engine for deduplication
- `ORDER BY` uses source primary key columns
- `_version` column (UInt64): `0` for snapshot, commit LSN for CDC
- Handles at-least-once delivery: duplicate writes with same PK + version are deduplicated during merges
- Query with `SELECT ... FINAL` for immediate deduplication at read time
- Schema prefix stripped from source table names (Postgres `public.users` -> ClickHouse `users`)
- CDC writes are coalesced in the activity layer before calling `WriteBatch`, so ClickHouse does not get one insert per source commit

### Postgres Source

- Creates publication (`FOR ALL TABLES` or `FOR TABLE ...`) and replication slot per flow
- Idempotent setup — checks existence before creating (safe for Temporal activity retries)
- Slot name uses underscores (Postgres restriction: no hyphens in slot names)
- `GetTableSchemas` queries `information_schema.columns` + `pg_index` for PK detection
- `Read` uses logical replication via `pgoutput`
- Reader resumes from the slot's server-tracked LSN (`confirmed_flush_lsn`, falling back to `restart_lsn`)
- CDC records are buffered per Postgres transaction and emitted as one `RecordBatch` per commit
- `RecordBatch.BatchId` is the commit LSN string and is the contract used by `Ack`
- `Ack` advances the connector's acknowledged LSN so standby status updates can move the slot forward
- Transient replication failures reconnect with backoff; critical failures such as a missing slot are surfaced as unrecoverable
- Inserts and updates are both materialized as insert-like records; deletes/truncates are not implemented yet

### MySQL Source

- Validates `log_bin=ON`, `gtid_mode=ON`, `binlog_format=ROW`, and `binlog_row_image=FULL`
- `Setup` captures the current executed GTID set as the initial CDC watermark
- `GetTableSchemas` queries `information_schema.columns` + `information_schema.statistics` for PK detection
- `Read` uses `COM_BINLOG_DUMP_GTID` via `go-mysql` and resumes from the last persisted GTID checkpoint
- CDC records are buffered per transaction and emitted as one `RecordBatch` per commit
- `RecordBatch.BatchId` is the cumulative GTID set string and is the contract used by `Ack`
- ClickHouse `_version` is derived from the current binlog file sequence plus end position
- Inserts and updates are materialized as insert-like records; deletes, truncates, and DDL are ignored in v1

### Workflow Architecture

```
CdcFlowWorkflow (main)
├── SetupActivity (5min timeout, 3 retries)
│   ├── Source: create publication + slot
│   ├── Source: get table schemas
│   └── Destination: create tables
├── SnapshotWorkflow (child workflow)
│   ├── SnapshotTableActivity per table (1hr timeout, heartbeat 30s)
│   └── Bounded parallelism via Selector (max 10 concurrent)
├── CdcStreamActivity (long-running, heartbeat 30s, unlimited retries)
│   ├── source.Read(ctx, ch) emits one batch per committed source transaction
│   ├── activity accumulates multiple source batches
│   ├── flushes to destination on size/time thresholds
│   └── source.Ack(ctx, highest_commit_lsn) after successful destination write
└── TeardownActivity (not yet implemented)
```

- **Per-activity timeouts**: Setup (5min), Snapshot (1hr per table), CDC (long-running with heartbeat)
- **CDC lifetime**: `CdcStreamActivity` uses a 1-year `StartToCloseTimeout` with unlimited retries, so it automatically restarts on timeout and resumes from the replication slot
- **Heartbeat recovery**: if the worker dies, Temporal times out the activity attempt and schedules a retry; when a worker returns, CDC resumes from the last acknowledged LSN
- **Fail-fast snapshot**: if any table fails, stop scheduling new ones, wait for in-flight to finish
- **Activity inputs use structs** (not positional args) for forward compatibility with Temporal serialization
- **Proto messages for Temporal payloads**: `TableSchema` defined in `internal.proto` since `QType` interface can't be JSON-serialized
- **Workflow logging**: use `workflow.GetLogger()` (replay-aware), not `slog`
- **Activity logging**: use `slog` (normal Go code)
- **Source-agnostic retry policy**: the workflow does not know Postgres-specific errors; it relies on `SourceConnector.IsCriticalError` to decide whether a source failure should be retried

### Serialization

- Peer configs (connection params) are encrypted with AES and stored as bytes in peerdb-postgres
- Flow configs are serialized as protobuf bytes
- Source checkpoints are persisted in peerdb-postgres so MySQL CDC can resume from the last acknowledged GTID set
- Table schemas cross the Temporal boundary as proto messages (`internal.proto`), converted to/from `connectors.TableSchema` via `schema_convert.go`

### Error Handling

- `errs.Error` type with Connect RPC codes (`BadRequest`, `NotFound`, `Internal`)
- `ErrorHandler` middleware converts `errs.Error` to `connect.Error` with proper HTTP status codes
- Connectors return plain errors; activity layer wraps with context

## Future Work

- **S3 staging**: Source -> S3 -> Destination for backpressure handling (decouple source ack from destination write)
- **S3 source connector**: poll S3 for new files, push to channel, checkpoint tracking in peerdb-postgres
- **Schema evolution**: detect new columns via Postgres relation messages
- **Dynamic table management**: add/remove tables from running flows
- **CDC deletes/truncates**: propagate non-insert change types
- **Configurable CDC batching**: move flush interval / max buffered records into flow config
- **Teardown**: clean up slots, publications, destination tables on flow deletion

## Local Development

```bash
docker compose up -d    # Start pg-source, ch-dest, peerdb-postgres, temporal, temporal-ui
go run ./cmd/api/       # Start API server on :8080
go run ./cmd/worker/    # Start Temporal worker

# Temporal UI: http://localhost:8233
```
