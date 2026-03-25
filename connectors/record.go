package connectors

import (
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

func (r InsertRecord) IsRecord() {}

type Record interface {
	IsRecord()
	GetTable() TableIdentifier
}

type RecordBatch struct {
	BatchId string
	Records []Record
}
