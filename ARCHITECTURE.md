# PeerDB Playground — Architecture

CDC (Change Data Capture) tool that moves data from sources (Postgres) to destinations (ClickHouse).

## Current State

- **Snapshot pipeline**: fully working end-to-end (Postgres -> ClickHouse)
- **CDC streaming**: interfaces defined, not yet implemented
- **Supported sources**: Postgres (logical replication)
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
  clickhouse/   — ClickHouse destination connector (table creation, batch writes)
docker-compose.yml — Local dev: pg-source, ch-dest, peerdb-postgres, temporal, temporal-ui
errs/           — Application error types (maps to Connect RPC codes)
gen/            — Generated protobuf + Connect RPC code
middleware/     — HTTP interceptors (request ID, logging, error handling)
migrations/     — SQL migrations for peerdb metadata database
pkg/
  postgres/     — Postgres utilities (connect, publications, replication slots)
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
```

## Key Design Decisions

### Connector Interfaces

```go
type SourceConnector interface {
    Setup(ctx) error              // Create publication + replication slot
    GetTableSchemas(ctx) ([], error) // Discover source schema
    SnapshotTable(ctx, table) (<-chan RecordBatch, error) // Stream full table
    Read(ctx, ch chan<- RecordBatch) error  // CDC stream (not yet implemented)
    Ack(ctx, position string) error         // Confirm processed LSN
    Teardown(ctx) error           // Drop slot + publication
    Close(ctx) error              // Close connections
}

type DestinationConnector interface {
    Setup(ctx, tables) error      // Create destination tables
    Write(ctx, <-chan RecordBatch) error // Consume and write batches
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

- `CdcFlowConfig.tables` — schema-qualified names controlling publication scope. Empty = all tables.
- `CdcFlowConfig.table_mappings` — optional source->destination rename + column exclusion. Independent from publication.
- Publication is the source of truth for which tables are replicated.
- Column exclusion is applied at the activity layer (before calling `SnapshotTable`), not inside connectors.

### ClickHouse Destination

- Tables use `ReplacingMergeTree(_version)` engine for deduplication
- `ORDER BY` uses source primary key columns
- `_version` column (UInt64): 0 for snapshot, LSN for CDC (future)
- Handles at-least-once delivery: duplicate writes with same PK + version are deduplicated during merges
- Query with `SELECT ... FINAL` for immediate deduplication at read time
- Schema prefix stripped from source table names (Postgres `public.users` -> ClickHouse `users`)

### Postgres Source

- Creates publication (`FOR ALL TABLES` or `FOR TABLE ...`) and replication slot per flow
- Idempotent setup — checks existence before creating (safe for Temporal activity retries)
- Slot name uses underscores (Postgres restriction: no hyphens in slot names)
- `GetTableSchemas` queries `information_schema.columns` + `pg_index` for PK detection

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
├── CdcStreamActivity (not yet implemented)
└── TeardownActivity (not yet implemented)
```

- **Per-activity timeouts**: Setup (5min), Snapshot (1hr per table), CDC (long-running with heartbeat)
- **Fail-fast snapshot**: if any table fails, stop scheduling new ones, wait for in-flight to finish
- **Activity inputs use structs** (not positional args) for forward compatibility with Temporal serialization
- **Proto messages for Temporal payloads**: `TableSchema` defined in `internal.proto` since `QType` interface can't be JSON-serialized
- **Workflow logging**: use `workflow.GetLogger()` (replay-aware), not `slog`
- **Activity logging**: use `slog` (normal Go code)

### Serialization

- Peer configs (connection params) are encrypted with AES and stored as bytes in peerdb-postgres
- Flow configs are serialized as protobuf bytes
- Table schemas cross the Temporal boundary as proto messages (`internal.proto`), converted to/from `connectors.TableSchema` via `schema_convert.go`

### Error Handling

- `errs.Error` type with Connect RPC codes (`BadRequest`, `NotFound`, `Internal`)
- `ErrorHandler` middleware converts `errs.Error` to `connect.Error` with proper HTTP status codes
- Connectors return plain errors; activity layer wraps with context

## Future Work

- **CDC streaming**: implement `Read` using pglogrepl, LSN-based versioning
- **S3 staging**: Source -> S3 -> Destination for backpressure handling (decouple source ack from destination write)
- **S3 source connector**: poll S3 for new files, push to channel, checkpoint tracking in peerdb-postgres
- **Schema evolution**: detect new columns via Postgres relation messages
- **Dynamic table management**: add/remove tables from running flows
- **Teardown**: clean up slots, publications, destination tables on flow deletion

## Local Development

```bash
docker compose up -d    # Start pg-source, ch-dest, peerdb-postgres, temporal, temporal-ui
go run ./cmd/api/       # Start API server on :8080
go run ./cmd/worker/    # Start Temporal worker

# Temporal UI: http://localhost:8233
```
