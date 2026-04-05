package workflows

import (
	"context"
	"errors"
	"testing"
	"time"

	"peerdb-playground/connectors"

	"go.temporal.io/sdk/temporal"
)

func testCdcStreamConfig(flushInterval time.Duration, maxBatchSize int) cdcStreamConfig {
	return cdcStreamConfig{
		flushInterval:     flushInterval,
		maxBatchSize:      maxBatchSize,
		heartbeatInterval: time.Hour, // effectively disabled for tests
		recordHeartbeat:   func(context.Context, ...any) {},
	}
}

func TestRunCdcStreamCoalescesBatchesAndAcksHighestLSN(t *testing.T) {
	cfg := testCdcStreamConfig(time.Hour, 10)

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
			acks = append(acks, position)
			return nil
		},
		isCriticalErrorFn: func(err error) bool {
			return false
		},
	}
	dst := &fakeDestinationConnector{
		writeBatchFn: func(ctx context.Context, batch connectors.RecordBatch) error {
			writes = append(writes, batch)
			return nil
		},
	}

	err := runCdcStream(context.Background(), src, dst, cfg, testLogger())
	if err != nil {
		t.Fatalf("runCdcStream returned error: %v", err)
	}

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
	cfg := testCdcStreamConfig(time.Hour, 2)

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
			ackBatchIDs = append(ackBatchIDs, position)
			return nil
		},
		isCriticalErrorFn: func(err error) bool {
			return false
		},
	}
	dst := &fakeDestinationConnector{
		writeBatchFn: func(ctx context.Context, batch connectors.RecordBatch) error {
			writeBatchIDs = append(writeBatchIDs, batch.BatchId)
			return nil
		},
	}

	err := runCdcStream(context.Background(), src, dst, cfg, testLogger())
	if err != nil {
		t.Fatalf("runCdcStream returned error: %v", err)
	}

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
	cfg := testCdcStreamConfig(time.Hour, 1)

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

	err := runCdcStream(context.Background(), src, dst, cfg, testLogger())
	if err == nil {
		t.Fatal("expected runCdcStream to return an error")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("unexpected error: got %v want %v", err, expectedErr)
	}
}

func TestRunCdcStreamReturnsNonRetryableOnCriticalSourceError(t *testing.T) {
	cfg := testCdcStreamConfig(time.Hour, 10)

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

	err := runCdcStream(context.Background(), src, dst, cfg, testLogger())
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
