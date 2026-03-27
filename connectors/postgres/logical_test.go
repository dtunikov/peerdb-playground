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
	rel := &pglogrepl.RelationMessage{
		Namespace:    "public",
		RelationName: "users",
		Columns: []*pglogrepl.RelationMessageColumn{
			{Name: "id", DataType: pgtype.Int4OID},
			{Name: "name", DataType: pgtype.TextOID},
			{Name: "active", DataType: pgtype.BoolOID},
		},
	}
	tableSchema := connectors.TableSchema{
		Table: connectors.TableIdentifier{Schema: "public", Name: "users"},
		Columns: []connectors.ColumnSchema{
			{Name: "id", Type: types.QTypeInt32{}},
			{Name: "name", Type: types.QTypeString{}},
			{Name: "active", Type: types.QTypeBool{}},
		},
	}
	tuple := &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("42")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("alice")},
			{DataType: pglogrepl.TupleDataTypeText, Data: []byte("t")},
		},
	}

	values, skip, err := decodeTupleColumns(pgtype.NewMap(), rel, tableSchema, tuple)
	if err != nil {
		t.Fatalf("decodeTupleColumns returned error: %v", err)
	}
	if skip {
		t.Fatal("decodeTupleColumns unexpectedly requested skip")
	}
	if len(values) != 3 {
		t.Fatalf("unexpected value count: got %d want 3", len(values))
	}

	if got, want := values[0].Value.Value(), int32(42); got != want {
		t.Fatalf("unexpected id value: got %v want %v", got, want)
	}
	if got, want := values[1].Value.Value(), "alice"; got != want {
		t.Fatalf("unexpected name value: got %v want %v", got, want)
	}
	if got, want := values[2].Value.Value(), true; got != want {
		t.Fatalf("unexpected active value: got %v want %v", got, want)
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

	record, skip, err := makeInsertRecord(pgtype.NewMap(), relations, tableSchemas, 1, tuple)
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

	_, skip, err := makeInsertRecord(pgtype.NewMap(), relations, tableSchemas, 1, tuple)
	if err != nil {
		t.Fatalf("makeInsertRecord returned error: %v", err)
	}
	if !skip {
		t.Fatal("expected unchanged TOAST tuple to be skipped")
	}
}

func TestAckIsMonotonic(t *testing.T) {
	connector := &SourceConnector{}

	if err := connector.Ack(context.Background(), "0/20"); err != nil {
		t.Fatalf("Ack returned error: %v", err)
	}
	if got, want := connector.lastAckedLSN.String(), "0/20"; got != want {
		t.Fatalf("unexpected acked lsn after first ack: got %s want %s", got, want)
	}

	if err := connector.Ack(context.Background(), "0/10"); err != nil {
		t.Fatalf("Ack returned error for stale lsn: %v", err)
	}
	if got, want := connector.lastAckedLSN.String(), "0/20"; got != want {
		t.Fatalf("stale ack moved lsn backwards: got %s want %s", got, want)
	}

	if err := connector.Ack(context.Background(), "0/30"); err != nil {
		t.Fatalf("Ack returned error for newer lsn: %v", err)
	}
	if got, want := connector.lastAckedLSN.String(), "0/30"; got != want {
		t.Fatalf("unexpected acked lsn after newer ack: got %s want %s", got, want)
	}
}
