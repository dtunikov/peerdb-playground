package postgres

import (
	"context"
	"testing"

	"peerdb-playground/connectors"
	"peerdb-playground/connectors/types"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestDecodeTupleColumns(t *testing.T) {
	testCases := []struct {
		name        string
		rel         *pglogrepl.RelationMessage
		tableSchema connectors.TableSchema
		tuple       *pglogrepl.TupleData
		expected    []any
		wantErr     bool
	}{
		{
			name: "text columns",
			rel: &pglogrepl.RelationMessage{
				Namespace:    "public",
				RelationName: "users",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "name", DataType: pgtype.TextOID},
					{Name: "active", DataType: pgtype.BoolOID},
				},
			},
			tableSchema: connectors.TableSchema{
				Table: connectors.TableIdentifier{Schema: "public", Name: "users"},
				Columns: []connectors.ColumnSchema{
					{Name: "id", Type: types.QTypeInt32{}},
					{Name: "name", Type: types.QTypeString{}},
					{Name: "active", Type: types.QTypeBool{}},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{DataType: pglogrepl.TupleDataTypeText, Data: []byte("42")},
					{DataType: pglogrepl.TupleDataTypeText, Data: []byte("alice")},
					{DataType: pglogrepl.TupleDataTypeText, Data: []byte("t")},
				},
			},
			expected: []any{int32(42), "alice", true},
		},
		{
			name: "null column",
			rel: &pglogrepl.RelationMessage{
				Namespace:    "public",
				RelationName: "users",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
					{Name: "name", DataType: pgtype.TextOID},
				},
			},
			tableSchema: connectors.TableSchema{
				Table: connectors.TableIdentifier{Schema: "public", Name: "users"},
				Columns: []connectors.ColumnSchema{
					{Name: "id", Type: types.QTypeInt32{}},
					{Name: "name", Type: types.QTypeString{}},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
					{DataType: pglogrepl.TupleDataTypeNull},
				},
			},
			expected: []any{int32(1), nil},
		},
		{
			name: "binary returns error",
			rel: &pglogrepl.RelationMessage{
				Namespace:    "public",
				RelationName: "users",
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int4OID},
				},
			},
			tableSchema: connectors.TableSchema{
				Table: connectors.TableIdentifier{Schema: "public", Name: "users"},
				Columns: []connectors.ColumnSchema{
					{Name: "id", Type: types.QTypeInt32{}},
				},
			},
			tuple: &pglogrepl.TupleData{
				Columns: []*pglogrepl.TupleDataColumn{
					{DataType: pglogrepl.TupleDataTypeBinary, Data: []byte{0x00, 0x01}},
				},
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			values, skip, err := decodeTupleColumns(tc.rel, tc.tableSchema, tc.tuple)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if skip {
				t.Fatal("unexpected skip")
			}
			if len(values) != len(tc.expected) {
				t.Fatalf("value count: got %d want %d", len(values), len(tc.expected))
			}
			for i, v := range values {
				if got, want := v.Value.Value(), tc.expected[i]; got != want {
					t.Fatalf("column %d: got %v want %v", i, got, want)
				}
			}
		})
	}
}

func TestSchemaForRelationUsesQualifiedName(t *testing.T) {
	rel := &pglogrepl.RelationMessage{
		Namespace:    "public",
		RelationName: "users",
	}
	index := map[string]connectors.TableSchema{
		"public.users": {
			Table: connectors.TableIdentifier{Schema: "public", Name: "users"},
		},
	}

	tableSchema, err := schemaForRelation(rel, index)
	if err != nil {
		t.Fatalf("schemaForRelation returned error: %v", err)
	}
	if got, want := tableSchema.Table.String(), "public.users"; got != want {
		t.Fatalf("unexpected table schema match: got %s want %s", got, want)
	}
}

func TestMakeInsertRecordForUpdateTuple(t *testing.T) {
	relations := map[uint32]*pglogrepl.RelationMessage{
		1: {
			RelationID:   1,
			Namespace:    "public",
			RelationName: "users",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: pgtype.Int4OID},
				{Name: "name", DataType: pgtype.TextOID},
			},
		},
	}
	tableSchemas := map[string]connectors.TableSchema{
		"public.users": {
			Table: connectors.TableIdentifier{Schema: "public", Name: "users"},
			Columns: []connectors.ColumnSchema{
				{Name: "id", Type: types.QTypeInt32{}},
				{Name: "name", Type: types.QTypeString{}},
			},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("7")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("new-name")},
		},
	}

	record, skip, err := makeInsertRecord(relations, tableSchemas, 1, tuple)
	if err != nil {
		t.Fatalf("makeInsertRecord returned error: %v", err)
	}
	if skip {
		t.Fatal("makeInsertRecord unexpectedly requested skip")
	}
	if got, want := record.Table.String(), "public.users"; got != want {
		t.Fatalf("unexpected table: got %s want %s", got, want)
	}
	if got, want := record.Values[1].Value.Value(), "new-name"; got != want {
		t.Fatalf("unexpected updated value: got %v want %v", got, want)
	}
}

func TestMakeInsertRecordSkipsUnchangedToast(t *testing.T) {
	relations := map[uint32]*pglogrepl.RelationMessage{
		1: {
			RelationID:   1,
			Namespace:    "public",
			RelationName: "users",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: pgtype.Int4OID},
				{Name: "payload", DataType: pgtype.TextOID},
			},
		},
	}
	tableSchemas := map[string]connectors.TableSchema{
		"public.users": {
			Table: connectors.TableIdentifier{Schema: "public", Name: "users"},
			Columns: []connectors.ColumnSchema{
				{Name: "id", Type: types.QTypeInt32{}},
				{Name: "payload", Type: types.QTypeString{}},
			},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("1")},
			{DataType: pglogrepl.TupleDataTypeToast},
		},
	}

	_, skip, err := makeInsertRecord(relations, tableSchemas, 1, tuple)
	if err != nil {
		t.Fatalf("makeInsertRecord returned error: %v", err)
	}
	if !skip {
		t.Fatal("expected unchanged TOAST tuple to be skipped")
	}
}

func TestMakeDeleteRecordPreservesPreImage(t *testing.T) {
	relations := map[uint32]*pglogrepl.RelationMessage{
		1: {
			RelationID:   1,
			Namespace:    "public",
			RelationName: "users",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: pgtype.Int4OID, Flags: 1},
				{Name: "name", DataType: pgtype.TextOID},
				{Name: "active", DataType: pgtype.BoolOID},
			},
		},
	}
	tableSchemas := map[string]connectors.TableSchema{
		"public.users": {
			Table: connectors.TableIdentifier{Schema: "public", Name: "users"},
			Columns: []connectors.ColumnSchema{
				{Name: "id", Type: types.QTypeInt32{}, PrimaryKey: true},
				{Name: "name", Type: types.QTypeString{}},
				{Name: "active", Type: types.QTypeBool{}},
			},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("42")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("alice")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("t")},
		},
	}

	record, err := makeDeleteRecord(relations, tableSchemas, 1, tuple)
	if err != nil {
		t.Fatalf("makeDeleteRecord returned error: %v", err)
	}
	if got, want := record.Table.String(), "public.users"; got != want {
		t.Fatalf("unexpected table: got %s want %s", got, want)
	}
	if got, want := len(record.Values), 3; got != want {
		t.Fatalf("expected %d values, got %d", want, got)
	}

	if got, want := record.Values[0].Name, "id"; got != want {
		t.Fatalf("unexpected pk column name: got %s want %s", got, want)
	}
	if got, want := record.Values[0].Value.Value(), int32(42); got != want {
		t.Fatalf("expected pk value preserved: got %v want %v", got, want)
	}

	if got, want := record.Values[1].Name, "name"; got != want {
		t.Fatalf("unexpected non-pk column name at idx 1: got %s want %s", got, want)
	}
	if got, want := record.Values[1].Value.Value(), "alice"; got != want {
		t.Fatalf("expected non-pk pre-image preserved: got %v want %v", got, want)
	}
	if got, want := record.Values[2].Name, "active"; got != want {
		t.Fatalf("unexpected non-pk column name at idx 2: got %s want %s", got, want)
	}
	if got, want := record.Values[2].Value.Value(), true; got != want {
		t.Fatalf("expected non-pk pre-image preserved: got %v want %v", got, want)
	}
}

func TestMakeDeleteRecordErrorsWhenNoPrimaryKey(t *testing.T) {
	relations := map[uint32]*pglogrepl.RelationMessage{
		1: {
			RelationID:   1,
			Namespace:    "public",
			RelationName: "events",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "payload", DataType: pgtype.TextOID},
			},
		},
	}
	tableSchemas := map[string]connectors.TableSchema{
		"public.events": {
			Table: connectors.TableIdentifier{Schema: "public", Name: "events"},
			Columns: []connectors.ColumnSchema{
				{Name: "payload", Type: types.QTypeString{}},
			},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("hi")},
		},
	}

	if _, err := makeDeleteRecord(relations, tableSchemas, 1, tuple); err == nil {
		t.Fatal("expected error for table without primary key, got nil")
	}
}

func TestMakeDeleteRecordErrorsWhenTupleMissing(t *testing.T) {
	relations := map[uint32]*pglogrepl.RelationMessage{
		1: {
			RelationID:   1,
			Namespace:    "public",
			RelationName: "users",
			Columns: []*pglogrepl.RelationMessageColumn{
				{Name: "id", DataType: pgtype.Int4OID, Flags: 1},
			},
		},
	}
	tableSchemas := map[string]connectors.TableSchema{
		"public.users": {
			Table: connectors.TableIdentifier{Schema: "public", Name: "users"},
			Columns: []connectors.ColumnSchema{
				{Name: "id", Type: types.QTypeInt32{}, PrimaryKey: true},
			},
		},
	}

	if _, err := makeDeleteRecord(relations, tableSchemas, 1, nil); err == nil {
		t.Fatal("expected error for nil tuple, got nil")
	}
}

func TestAckIsMonotonic(t *testing.T) {
	connector := &SourceConnector{}

	if err := connector.Ack(context.Background(), "0/20"); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}
	if got, want := pglogrepl.LSN(connector.lastAckedLSN.Load()).String(), "0/20"; got != want {
		t.Fatalf("unexpected acked lsn after first ack: got %s want %s", got, want)
	}

	if err := connector.Ack(context.Background(), "0/10"); err != nil {
		t.Fatalf("Ack returned error for stale lsn: %v", err)
	}
	if got, want := pglogrepl.LSN(connector.lastAckedLSN.Load()).String(), "0/20"; got != want {
		t.Fatalf("stale ack moved lsn backwards: got %s want %s", got, want)
	}

	if err := connector.Ack(context.Background(), "0/30"); err != nil {
		t.Fatalf("Ack returned error for newer lsn: %v", err)
	}
	if got, want := pglogrepl.LSN(connector.lastAckedLSN.Load()).String(), "0/30"; got != want {
		t.Fatalf("unexpected acked lsn after newer ack: got %s want %s", got, want)
	}
}
