package workflows

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"peerdb-playground/connectors"

	"go.temporal.io/sdk/temporal"
)

func TestRunCdcStreamCoalescesBatchesAndAcksHighestLSN(t *testing.T) {
	restoreHeartbeat, restoreFlushInterval, restoreMaxBatchSize := overrideCdcStreamTestConfig()
	defer restoreHeartbeat()
	defer restoreFlushInterval()
	defer restoreMaxBatchSize()

	cdcStreamFlushInterval = time.Hour
	cdcStreamMaxBatchSize = 10

	var mu sync.Mutex
	writes := []connectors.RecordBatch{}
	acks := []string{}
	src := &fakeSourceConnector{
		readFn: func(ctx context.Context, ch chan<- connectors.RecordBatch) error {
			defer close(ch)
			ch <- makeTestRecordBatch("0/20", 1)
			ch <- makeTestRecordBatch("0/30", 2)
			return nil
		},
		ackFn: func(ctx context.Context, position string) error {
			mu.Lock()
			defer mu.Unlock()
			acks = append(acks, position)
			return nil
		},
		isCriticalErrorFn: func(err error) bool {
			return false
		},
	}
	dst := &fakeDestinationConnector{
		writeBatchFn: func(ctx context.Context, batch connectors.RecordBatch) error {
			mu.Lock()
			defer mu.Unlock()
			writes = append(writes, batch)
			return nil
		},
	}

	err := runCdcStream(context.Background(), src, dst, testLogger())
	if err != nil {
		t.Fatalf("runCdcStream returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(writes) != 1 {
		t.Fatalf("unexpected write count: got %d want 1", len(writes))
	}
	if got, want := writes[0].BatchId, "0/30"; got != want {
		t.Fatalf("unexpected flushed batch id: got %s want %s", got, want)
	}
	if got, want := len(writes[0].Records), 3; got != want {
		t.Fatalf("unexpected flushed record count: got %d want %d", got, want)
	}
	if len(acks) != 1 {
		t.Fatalf("unexpected ack count: got %d want 1", len(acks))
	}
	if got, want := acks[0], "0/30"; got != want {
		t.Fatalf("unexpected acked lsn: got %s want %s", got, want)
	}
}

func TestRunCdcStreamFlushesWhenBatchSizeReached(t *testing.T) {
	restoreHeartbeat, restoreFlushInterval, restoreMaxBatchSize := overrideCdcStreamTestConfig()
	defer restoreHeartbeat()
	defer restoreFlushInterval()
	defer restoreMaxBatchSize()

	cdcStreamFlushInterval = time.Hour
	cdcStreamMaxBatchSize = 2

	var mu sync.Mutex
	writeBatchIDs := []string{}
	ackBatchIDs := []string{}
	src := &fakeSourceConnector{
		readFn: func(ctx context.Context, ch chan<- connectors.RecordBatch) error {
			defer close(ch)
			ch <- makeTestRecordBatch("0/10", 1)
			ch <- makeTestRecordBatch("0/20", 1)
			ch <- makeTestRecordBatch("0/30", 1)
			return nil
		},
		ackFn: func(ctx context.Context, position string) error {
			mu.Lock()
			defer mu.Unlock()
			ackBatchIDs = append(ackBatchIDs, position)
			return nil
		},
		isCriticalErrorFn: func(err error) bool {
			return false
		},
	}
	dst := &fakeDestinationConnector{
		writeBatchFn: func(ctx context.Context, batch connectors.RecordBatch) error {
			mu.Lock()
			defer mu.Unlock()
			writeBatchIDs = append(writeBatchIDs, batch.BatchId)
			return nil
		},
	}

	err := runCdcStream(context.Background(), src, dst, testLogger())
	if err != nil {
		t.Fatalf("runCdcStream returned error: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if got, want := len(writeBatchIDs), 2; got != want {
		t.Fatalf("unexpected write count: got %d want %d", got, want)
	}
	if got, want := writeBatchIDs[0], "0/20"; got != want {
		t.Fatalf("unexpected first flushed batch id: got %s want %s", got, want)
	}
	if got, want := writeBatchIDs[1], "0/30"; got != want {
		t.Fatalf("unexpected second flushed batch id: got %s want %s", got, want)
	}
	if got, want := len(ackBatchIDs), 2; got != want {
		t.Fatalf("unexpected ack count: got %d want %d", got, want)
	}
	if got, want := ackBatchIDs[0], "0/20"; got != want {
		t.Fatalf("unexpected first ack: got %s want %s", got, want)
	}
	if got, want := ackBatchIDs[1], "0/30"; got != want {
		t.Fatalf("unexpected second ack: got %s want %s", got, want)
	}
}

func TestRunCdcStreamReturnsDestinationError(t *testing.T) {
	restoreHeartbeat, restoreFlushInterval, restoreMaxBatchSize := overrideCdcStreamTestConfig()
	defer restoreHeartbeat()
	defer restoreFlushInterval()
	defer restoreMaxBatchSize()

	cdcStreamFlushInterval = time.Hour
	cdcStreamMaxBatchSize = 1

	expectedErr := errors.New("write failed")
	src := &fakeSourceConnector{
		readFn: func(ctx context.Context, ch chan<- connectors.RecordBatch) error {
			defer close(ch)
			ch <- makeTestRecordBatch("0/30", 1)
			return nil
		},
		ackFn: func(ctx context.Context, position string) error {
			t.Fatalf("ack should not be called when destination write fails")
			return nil
		},
		isCriticalErrorFn: func(err error) bool {
			return false
		},
	}
	dst := &fakeDestinationConnector{
		writeBatchFn: func(ctx context.Context, batch connectors.RecordBatch) error {
			return expectedErr
		},
	}

	err := runCdcStream(context.Background(), src, dst, testLogger())
	if err == nil {
		t.Fatal("expected runCdcStream to return an error")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("unexpected error: got %v want %v", err, expectedErr)
	}
}

func TestRunCdcStreamReturnsNonRetryableOnCriticalSourceError(t *testing.T) {
	restoreHeartbeat, restoreFlushInterval, restoreMaxBatchSize := overrideCdcStreamTestConfig()
	defer restoreHeartbeat()
	defer restoreFlushInterval()
	defer restoreMaxBatchSize()

	cdcStreamFlushInterval = time.Hour
	cdcStreamMaxBatchSize = 10

	expectedErr := errors.New("critical source failure")
	src := &fakeSourceConnector{
		readFn: func(ctx context.Context, ch chan<- connectors.RecordBatch) error {
			defer close(ch)
			return expectedErr
		},
		ackFn: func(ctx context.Context, position string) error {
			t.Fatalf("ack should not be called for critical source failure")
			return nil
		},
		isCriticalErrorFn: func(err error) bool {
			return errors.Is(err, expectedErr)
		},
	}
	dst := &fakeDestinationConnector{
		writeBatchFn: func(ctx context.Context, batch connectors.RecordBatch) error {
			t.Fatalf("destination should not receive batches on critical source failure")
			return nil
		},
	}

	err := runCdcStream(context.Background(), src, dst, testLogger())
	if err == nil {
		t.Fatal("expected runCdcStream to return an error")
	}

	var appErr *temporal.ApplicationError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected temporal application error, got %T", err)
	}
	if !appErr.NonRetryable() {
		t.Fatal("expected critical source failure to be non-retryable")
	}
}

func overrideCdcStreamTestConfig() (func(), func(), func()) {
	restoreHeartbeat := func() {}
	restoreFlushInterval := func() {}
	restoreMaxBatchSize := func() {}

	prevHeartbeat := recordActivityHeartbeat
	recordActivityHeartbeat = func(context.Context, ...any) {}
	restoreHeartbeat = func() {
		recordActivityHeartbeat = prevHeartbeat
	}

	prevFlushInterval := cdcStreamFlushInterval
	restoreFlushInterval = func() {
		cdcStreamFlushInterval = prevFlushInterval
	}

	prevMaxBatchSize := cdcStreamMaxBatchSize
	restoreMaxBatchSize = func() {
		cdcStreamMaxBatchSize = prevMaxBatchSize
	}

	return restoreHeartbeat, restoreFlushInterval, restoreMaxBatchSize
}

func makeTestRecordBatch(batchID string, recordCount int) connectors.RecordBatch {
	records := make([]connectors.Record, 0, recordCount)
	for i := 0; i < recordCount; i++ {
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
}

func (f *fakeSourceConnector) Teardown(ctx context.Context) error {
	return nil
}

func (f *fakeSourceConnector) Close(ctx context.Context) error {
	return nil
}

func (f *fakeSourceConnector) Setup(ctx context.Context) error {
	return nil
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
	return nil, nil
}

type fakeDestinationConnector struct {
	writeBatchFn func(ctx context.Context, batch connectors.RecordBatch) error
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
	for batch := range ch {
		if err := f.writeBatchFn(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}
