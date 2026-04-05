package workflows

import (
	"context"
	"errors"
	"testing"

	"peerdb-playground/connectors"
	"peerdb-playground/gen"
)

func TestRunSnapshotWritesBatchesToDestination(t *testing.T) {
	batches := []connectors.RecordBatch{
		makeTestRecordBatch("batch-1", 2),
		makeTestRecordBatch("batch-2", 3),
	}

	src := &fakeSourceConnector{
		snapshotTableFn: func(ctx context.Context, table connectors.TableSchema) (<-chan connectors.RecordBatch, error) {
			ch := make(chan connectors.RecordBatch, len(batches))
			for _, b := range batches {
				ch <- b
			}
			close(ch)
			return ch, nil
		},
	}

	var written []connectors.RecordBatch
	dst := &fakeDestinationConnector{
		writeFn: func(ctx context.Context, ch <-chan connectors.RecordBatch) error {
			for batch := range ch {
				written = append(written, batch)
			}
			return nil
		},
	}

	table := connectors.TableSchema{
		Table:   connectors.TableIdentifier{Schema: "public", Name: "users"},
		Columns: []connectors.ColumnSchema{{Name: "id"}, {Name: "name"}},
	}

	err := runSnapshot(context.Background(), src, dst, table, testLogger())
	if err != nil {
		t.Fatalf("runSnapshot returned error: %v", err)
	}

	if got, want := len(written), 2; got != want {
		t.Fatalf("unexpected batch count: got %d want %d", got, want)
	}
	if got, want := written[0].BatchId, "batch-1"; got != want {
		t.Fatalf("unexpected first batch id: got %s want %s", got, want)
	}
	if got, want := written[1].BatchId, "batch-2"; got != want {
		t.Fatalf("unexpected second batch id: got %s want %s", got, want)
	}
}

func TestRunSnapshotReturnsSourceError(t *testing.T) {
	expectedErr := errors.New("snapshot failed")
	src := &fakeSourceConnector{
		snapshotTableFn: func(ctx context.Context, table connectors.TableSchema) (<-chan connectors.RecordBatch, error) {
			return nil, expectedErr
		},
	}
	dst := &fakeDestinationConnector{}

	table := connectors.TableSchema{
		Table: connectors.TableIdentifier{Schema: "public", Name: "users"},
	}

	err := runSnapshot(context.Background(), src, dst, table, testLogger())
	if err == nil {
		t.Fatal("expected runSnapshot to return an error")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("unexpected error: got %v want %v", err, expectedErr)
	}
}

func TestRunSnapshotReturnsDestinationError(t *testing.T) {
	expectedErr := errors.New("write failed")
	src := &fakeSourceConnector{
		snapshotTableFn: func(ctx context.Context, table connectors.TableSchema) (<-chan connectors.RecordBatch, error) {
			ch := make(chan connectors.RecordBatch)
			close(ch)
			return ch, nil
		},
	}
	dst := &fakeDestinationConnector{
		writeFn: func(ctx context.Context, ch <-chan connectors.RecordBatch) error {
			return expectedErr
		},
	}

	table := connectors.TableSchema{
		Table: connectors.TableIdentifier{Schema: "public", Name: "users"},
	}

	err := runSnapshot(context.Background(), src, dst, table, testLogger())
	if err == nil {
		t.Fatal("expected runSnapshot to return an error")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("unexpected error: got %v want %v", err, expectedErr)
	}
}

func TestApplyColumnExclusions(t *testing.T) {
	table := &gen.TableSchema{
		Schema: "public",
		Name:   "users",
		Columns: []*gen.ColumnSchema{
			{Name: "id"},
			{Name: "name"},
			{Name: "password"},
			{Name: "email"},
		},
	}
	mappings := []*gen.TableMapping{
		{
			Source:         "public.users",
			Destination:    "users",
			ExcludeColumns: []string{"password", "email"},
		},
	}

	result := applyColumnExclusions(table, mappings)

	if got, want := len(result.Columns), 2; got != want {
		t.Fatalf("unexpected column count: got %d want %d", got, want)
	}
	if got, want := result.Columns[0].Name, "id"; got != want {
		t.Fatalf("unexpected first column: got %s want %s", got, want)
	}
	if got, want := result.Columns[1].Name, "name"; got != want {
		t.Fatalf("unexpected second column: got %s want %s", got, want)
	}
}

func TestApplyColumnExclusionsNoMapping(t *testing.T) {
	table := &gen.TableSchema{
		Schema: "public",
		Name:   "users",
		Columns: []*gen.ColumnSchema{
			{Name: "id"},
			{Name: "name"},
		},
	}

	result := applyColumnExclusions(table, nil)

	if got, want := len(result.Columns), 2; got != want {
		t.Fatalf("unexpected column count: got %d want %d", got, want)
	}
}
