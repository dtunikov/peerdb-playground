package workflows

import (
	"context"
	"io"
	"log/slog"

	"peerdb-playground/connectors"
)

func makeTestRecordBatch(batchID string, recordCount int) connectors.RecordBatch {
	records := make([]connectors.Record, 0, recordCount)
	for i := range recordCount {
		records = append(records, connectors.InsertRecord{
			BaseRecord: connectors.BaseRecord{
				Table:   connectors.TableIdentifier{Schema: "public", Name: "users"},
				Version: uint64(i + 1),
			},
		})
	}
	return connectors.RecordBatch{
		BatchId: batchID,
		Records: records,
	}
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeSourceConnector struct {
	readFn            func(ctx context.Context, ch chan<- connectors.RecordBatch) error
	ackFn             func(ctx context.Context, position string) error
	isCriticalErrorFn func(err error) bool
	snapshotTableFn   func(ctx context.Context, table connectors.TableSchema) (<-chan connectors.RecordBatch, error)
	setupFn           func(ctx context.Context) (string, error)
}

func (f *fakeSourceConnector) Teardown(ctx context.Context) error {
	return nil
}

func (f *fakeSourceConnector) Close(ctx context.Context) error {
	return nil
}

func (f *fakeSourceConnector) Setup(ctx context.Context) (string, error) {
	if f.setupFn != nil {
		return f.setupFn(ctx)
	}
	return "", nil
}

func (f *fakeSourceConnector) Read(ctx context.Context, ch chan<- connectors.RecordBatch) error {
	return f.readFn(ctx, ch)
}

func (f *fakeSourceConnector) IsCriticalError(err error) bool {
	if f.isCriticalErrorFn == nil {
		return false
	}
	return f.isCriticalErrorFn(err)
}

func (f *fakeSourceConnector) Ack(ctx context.Context, position string) error {
	return f.ackFn(ctx, position)
}

func (f *fakeSourceConnector) GetTableSchemas(ctx context.Context) ([]connectors.TableSchema, error) {
	return nil, nil
}

func (f *fakeSourceConnector) SnapshotTable(ctx context.Context, table connectors.TableSchema) (<-chan connectors.RecordBatch, error) {
	if f.snapshotTableFn != nil {
		return f.snapshotTableFn(ctx, table)
	}
	return nil, nil
}

type fakeDestinationConnector struct {
	writeBatchFn func(ctx context.Context, batch connectors.RecordBatch) error
	writeFn      func(ctx context.Context, ch <-chan connectors.RecordBatch) error
}

func (f *fakeDestinationConnector) Teardown(ctx context.Context) error {
	return nil
}

func (f *fakeDestinationConnector) Close(ctx context.Context) error {
	return nil
}

func (f *fakeDestinationConnector) Setup(ctx context.Context, tables []connectors.TableSchema) error {
	return nil
}

func (f *fakeDestinationConnector) WriteBatch(ctx context.Context, batch connectors.RecordBatch) error {
	return f.writeBatchFn(ctx, batch)
}

func (f *fakeDestinationConnector) Write(ctx context.Context, ch <-chan connectors.RecordBatch) error {
	if f.writeFn != nil {
		return f.writeFn(ctx, ch)
	}
	for batch := range ch {
		if err := f.writeBatchFn(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}
