package connectors

import (
	"context"
	"peerdb-playground/connectors/types"
)

type Connector interface {
	// Teardown should clean up any resources used by the connector (e.g. close connections, delete slots, publications, etc.)
	// Should be called when cdc flow gets fully removed
	// IMPORTANT: never call Teardown unless you sure that connector must be fulle removed and all resources must be cleaned up.
	Teardown(ctx context.Context) error
	// Close cleans up tmp connections
	// Should be called once connector is no longer needed in current code execution (e.g. create and close connectors during setup phase)
	Close(ctx context.Context) error
}

type SourceConnector interface {
	Connector
	Setup(ctx context.Context) error
	// Read should continuously read changes from the source and send them to the provided channel until the context is canceled or unrecoverable error occurs.
	// The implementation should handle reconnections and resume from the last position in case of transient errors.
	Read(ctx context.Context, ch chan<- RecordBatch) error
	// IsCriticalError reports whether the source error should stop activity retries.
	IsCriticalError(err error) bool
	// Ack should acknowledge that the batch with the given ID has been successfully processed and can be marked as completed in the source.
	// For example, in case of postgres CDC we have to mark the corresponding LSN position as completed so that it could free up WAL logs.
	Ack(ctx context.Context, position string) error
	// Get table schemas
	GetTableSchemas(ctx context.Context) ([]TableSchema, error)
	// SnapshotTable returns a channel that will yield record batches for the table and close once the snapshot is complete.
	SnapshotTable(ctx context.Context, table TableSchema) (<-chan RecordBatch, error)
}

type DestinationConnector interface {
	Connector
	// Setup should prepare destination for incoming data (e.g. create tables if neccesary)
	Setup(ctx context.Context, tables []TableSchema) error
	// WriteBatch should apply a single batch to the destination.
	WriteBatch(ctx context.Context, batch RecordBatch) error
	// Write should consume record batches from the provided channel and apply them to the destination until the channel is closed or unrecoverable error occurs.
	Write(ctx context.Context, ch <-chan RecordBatch) error
}

type ColumnSchema struct {
	Name       string
	Type       types.QType
	Nullable   bool
	PrimaryKey bool
}

type TableIdentifier struct {
	Schema string
	Name   string
}

func (t TableIdentifier) String() string {
	if t.Schema == "" {
		return t.Name
	}
	return t.Schema + "." + t.Name
}

type TableSchema struct {
	Table   TableIdentifier
	Columns []ColumnSchema
}

type TableMapping struct {
	DestTableName  string
	ExcludeColumns []string
}

// TableMappings keys are source table names, values are respective mapping.
type TableMappings = map[string]TableMapping
