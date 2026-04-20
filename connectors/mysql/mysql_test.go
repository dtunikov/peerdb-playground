package mysql

import (
	"context"
	"database/sql"
	"testing"

	"peerdb-playground/connectors"
	"peerdb-playground/connectors/types"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func TestMysqlTypeToQType(t *testing.T) {
	testCases := []struct {
		name       string
		dataType   string
		columnType string
		precision  sql.NullInt64
		scale      sql.NullInt64
		want       types.QType
	}{
		{
			name:       "tinyint one becomes bool",
			dataType:   "tinyint",
			columnType: "tinyint(1)",
			want:       types.QTypeBool{},
		},
		{
			name:       "varchar becomes string",
			dataType:   "varchar",
			columnType: "varchar(255)",
			want:       types.QTypeString{},
		},
		{
			name:       "datetime becomes timestamp",
			dataType:   "datetime",
			columnType: "datetime",
			want:       types.QTypeTimestamp{},
		},
		{
			name:       "json becomes json",
			dataType:   "json",
			columnType: "json",
			want:       types.QTypeJSON{},
		},
		{
			name:       "decimal keeps precision and scale",
			dataType:   "decimal",
			columnType: "decimal(10,2)",
			precision:  sql.NullInt64{Int64: 10, Valid: true},
			scale:      sql.NullInt64{Int64: 2, Valid: true},
			want:       types.QTypeNumeric{Precision: 10, Scale: 2},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := mysqlTypeToQType(tc.dataType, tc.columnType, tc.precision, tc.scale)
			if err != nil {
				t.Fatalf("mysqlTypeToQType returned error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("unexpected qtype: got %#v want %#v", got, tc.want)
			}
		})
	}
}

func TestMysqlTypeToQTypeRejectsUnsupportedType(t *testing.T) {
	if _, err := mysqlTypeToQType("enum", "enum('a','b')", sql.NullInt64{}, sql.NullInt64{}); err == nil {
		t.Fatal("expected unsupported mysql type to return an error")
	}
}

func TestAppendRowsEventUsesUpdateAfterImage(t *testing.T) {
	txn := transactionBuffer{batchID: "uuid:1-2"}
	tableSchema := connectors.TableSchema{
		Table: connectors.TableIdentifier{Schema: "source", Name: "users"},
		Columns: []connectors.ColumnSchema{
			{Name: "id", Type: types.QTypeInt64{}},
			{Name: "name", Type: types.QTypeString{}},
		},
	}
	rowsEvent := &replication.RowsEvent{
		Rows: [][]any{
			{int64(7), "old-name"},
			{int64(7), "new-name"},
		},
	}
	rowsEvent.Table = &replication.TableMapEvent{Schema: []byte("source"), Table: []byte("users")}

	if err := appendRowsEvent(&txn, rowsEvent, tableSchema, 123, replication.UPDATE_ROWS_EVENTv2); err != nil {
		t.Fatalf("appendRowsEvent returned error: %v", err)
	}
	if got, want := len(txn.records), 1; got != want {
		t.Fatalf("unexpected record count: got %d want %d", got, want)
	}
	rec := txn.records[0].(connectors.InsertRecord)
	if got, want := rec.Values[1].Value.Value(), "new-name"; got != want {
		t.Fatalf("unexpected updated value: got %v want %v", got, want)
	}
	if got, want := rec.Version, uint64(123); got != want {
		t.Fatalf("unexpected record version: got %d want %d", got, want)
	}
}

func TestAppendRowsEventDeleteNullsNonPkColumns(t *testing.T) {
	txn := transactionBuffer{batchID: "uuid:1-2"}
	tableSchema := connectors.TableSchema{
		Table: connectors.TableIdentifier{Schema: "source", Name: "users"},
		Columns: []connectors.ColumnSchema{
			{Name: "id", Type: types.QTypeInt64{}, PrimaryKey: true},
			{Name: "name", Type: types.QTypeString{}},
		},
	}
	rowsEvent := &replication.RowsEvent{
		Rows: [][]any{
			{int64(7), "alice"},
		},
	}
	rowsEvent.Table = &replication.TableMapEvent{Schema: []byte("source"), Table: []byte("users")}

	if err := appendRowsEvent(&txn, rowsEvent, tableSchema, 456, replication.DELETE_ROWS_EVENTv2); err != nil {
		t.Fatalf("appendRowsEvent returned error: %v", err)
	}
	if got, want := len(txn.records), 1; got != want {
		t.Fatalf("unexpected record count: got %d want %d", got, want)
	}
	rec, ok := txn.records[0].(connectors.DeleteRecord)
	if !ok {
		t.Fatalf("expected DeleteRecord, got %T", txn.records[0])
	}
	if got, want := rec.Values[0].Value.Value(), int64(7); got != want {
		t.Fatalf("expected pk preserved: got %v want %v", got, want)
	}
	if _, isNull := rec.Values[1].Value.(types.QValueNull); !isNull {
		t.Fatalf("expected non-pk column to be QValueNull, got %T", rec.Values[1].Value)
	}
	if got, want := rec.Version, uint64(456); got != want {
		t.Fatalf("unexpected record version: got %d want %d", got, want)
	}
}

func TestAppendRowsEventDeleteErrorsWithoutPrimaryKey(t *testing.T) {
	txn := transactionBuffer{batchID: "uuid:1-2"}
	tableSchema := connectors.TableSchema{
		Table: connectors.TableIdentifier{Schema: "source", Name: "events"},
		Columns: []connectors.ColumnSchema{
			{Name: "payload", Type: types.QTypeString{}},
		},
	}
	rowsEvent := &replication.RowsEvent{
		Rows: [][]any{{"hi"}},
	}
	rowsEvent.Table = &replication.TableMapEvent{Schema: []byte("source"), Table: []byte("events")}

	if err := appendRowsEvent(&txn, rowsEvent, tableSchema, 1, replication.DELETE_ROWS_EVENTv2); err == nil {
		t.Fatal("expected error for delete on table without primary key")
	}
}

func TestAckValidatesGTIDSet(t *testing.T) {
	connector := &SourceConnector{}
	valid := "3E11FA47-71CA-11E1-9E33-C80AA9429562:23-24"
	if _, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, valid); err != nil {
		t.Fatalf("test setup invalid gtid set: %v", err)
	}
	if err := connector.Ack(context.Background(), valid); err != nil {
		t.Fatalf("Ack returned error for valid gtid set: %v", err)
	}
	if err := connector.Ack(context.Background(), "not-a-gtid"); err == nil {
		t.Fatal("expected Ack to reject invalid gtid set")
	}
}

func TestMysqlVersionForPosition(t *testing.T) {
	got := mysqlVersionForPosition("mysql-bin.000123", 456)
	want := (uint64(123) << 32) | uint64(456)
	if got != want {
		t.Fatalf("unexpected mysql version: got %d want %d", got, want)
	}
}
