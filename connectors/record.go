package connectors

import (
	"fmt"
	"peerdb-playground/connectors/types"
)

type ColumnValue struct {
	Name  string
	Value types.QValue
}

type BaseRecord struct {
	Table   TableIdentifier
	Version uint64
}

func (r BaseRecord) GetTable() TableIdentifier {
	return r.Table
}

type InsertRecord struct {
	BaseRecord
	Values []ColumnValue
}

func (r InsertRecord) isRecord() {}

type DeleteRecord struct {
	BaseRecord
	Values []ColumnValue // non-pk columns will be NULL
}

func (r DeleteRecord) isRecord() {}

type Record interface {
	isRecord()
	GetTable() TableIdentifier
}

type RecordBatch struct {
	BatchId string
	Records []Record
}

func RecordWithVersion(r Record, v uint64) (Record, error) {
	switch x := r.(type) {
	case InsertRecord:
		x.Version = v
		return x, nil
	case DeleteRecord:
		x.Version = v
		return x, nil
	}

	return nil, fmt.Errorf("unsupported record type %T", r)
}
