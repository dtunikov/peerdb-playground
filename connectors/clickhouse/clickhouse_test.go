package clickhouse

import (
	"reflect"
	"strings"
	"testing"

	"peerdb-playground/connectors"
	"peerdb-playground/connectors/types"
)

func TestColumnNames(t *testing.T) {
	table := connectors.TableIdentifier{Schema: "public", Name: "users"}

	testCases := []struct {
		name   string
		record connectors.Record
		want   []string
	}{
		{
			name: "insert record with multiple columns",
			record: connectors.InsertRecord{
				BaseRecord: connectors.BaseRecord{Table: table},
				Values: []connectors.ColumnValue{
					{Name: "id", Value: types.QValueInt32{Val: 1}},
					{Name: "name", Value: types.QValueString{Val: "alice"}},
				},
			},
			want: []string{`"id"`, `"name"`, `"_version"`, `"_is_deleted"`},
		},
		{
			name: "delete record with same shape as insert",
			record: connectors.DeleteRecord{
				BaseRecord: connectors.BaseRecord{Table: table},
				Values: []connectors.ColumnValue{
					{Name: "id", Value: types.QValueInt32{Val: 1}},
					{Name: "name", Value: types.QValueNull{}},
				},
			},
			want: []string{`"id"`, `"name"`, `"_version"`, `"_is_deleted"`},
		},
		{
			name: "record with no user columns still emits meta columns",
			record: connectors.InsertRecord{
				BaseRecord: connectors.BaseRecord{Table: table},
				Values:     nil,
			},
			want: []string{`"_version"`, `"_is_deleted"`},
		},
		{
			name: "column names with special characters are quoted",
			record: connectors.InsertRecord{
				BaseRecord: connectors.BaseRecord{Table: table},
				Values: []connectors.ColumnValue{
					{Name: "user id", Value: types.QValueInt32{Val: 1}},
					{Name: "Order", Value: types.QValueString{Val: "x"}},
				},
			},
			want: []string{`"user id"`, `"Order"`, `"_version"`, `"_is_deleted"`},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := columnNames(tc.record)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("columnNames mismatch\n got: %v\nwant: %v", got, tc.want)
			}
			for _, n := range got {
				if !strings.HasPrefix(n, `"`) || !strings.HasSuffix(n, `"`) {
					t.Errorf("column name %q is not double-quoted", n)
				}
			}
		})
	}
}
