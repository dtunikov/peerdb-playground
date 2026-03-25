package connectors

import (
	"peerdb-playground/connectors/types"
	"peerdb-playground/gen"
)

func TableSchemaToProto(t TableSchema) *gen.TableSchema {
	cols := make([]*gen.ColumnSchema, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = columnSchemaToProto(c)
	}
	return &gen.TableSchema{
		Schema:  t.Table.Schema,
		Name:    t.Table.Name,
		Columns: cols,
	}
}

func TableSchemasToProto(tables []TableSchema) []*gen.TableSchema {
	result := make([]*gen.TableSchema, len(tables))
	for i, t := range tables {
		result[i] = TableSchemaToProto(t)
	}
	return result
}

func TableSchemaFromProto(t *gen.TableSchema) TableSchema {
	cols := make([]ColumnSchema, len(t.Columns))
	for i, c := range t.Columns {
		cols[i] = columnSchemaFromProto(c)
	}
	return TableSchema{
		Table: TableIdentifier{
			Schema: t.Schema,
			Name:   t.Name,
		},
		Columns: cols,
	}
}

func TableSchemasFromProto(tables []*gen.TableSchema) []TableSchema {
	result := make([]TableSchema, len(tables))
	for i, t := range tables {
		result[i] = TableSchemaFromProto(t)
	}
	return result
}

func columnSchemaToProto(c ColumnSchema) *gen.ColumnSchema {
	col := &gen.ColumnSchema{
		Name:       c.Name,
		Type:       qTypeToProto(c.Type),
		Nullable:   c.Nullable,
		PrimaryKey: c.PrimaryKey,
	}
	if n, ok := c.Type.(types.QTypeNumeric); ok {
		col.Precision = int32(n.Precision)
		col.Scale = int32(n.Scale)
	}
	if e, ok := c.Type.(types.QTypeEnum); ok {
		col.EnumValues = e.Values
	}
	return col
}

func columnSchemaFromProto(c *gen.ColumnSchema) ColumnSchema {
	return ColumnSchema{
		Name:       c.Name,
		Type:       qTypeFromProto(c.Type, int(c.Precision), int(c.Scale), c.EnumValues),
		Nullable:   c.Nullable,
		PrimaryKey: c.PrimaryKey,
	}
}

func qTypeToProto(t types.QType) gen.QType {
	switch t.(type) {
	case types.QTypeBool:
		return gen.QType_QTYPE_BOOL
	case types.QTypeInt16:
		return gen.QType_QTYPE_INT16
	case types.QTypeInt32:
		return gen.QType_QTYPE_INT32
	case types.QTypeInt64:
		return gen.QType_QTYPE_INT64
	case types.QTypeFloat32:
		return gen.QType_QTYPE_FLOAT32
	case types.QTypeFloat64:
		return gen.QType_QTYPE_FLOAT64
	case types.QTypeString:
		return gen.QType_QTYPE_STRING
	case types.QTypeBytes:
		return gen.QType_QTYPE_BYTES
	case types.QTypeDate:
		return gen.QType_QTYPE_DATE
	case types.QTypeTime:
		return gen.QType_QTYPE_TIME
	case types.QTypeTimestamp:
		return gen.QType_QTYPE_TIMESTAMP
	case types.QTypeTimestampTZ:
		return gen.QType_QTYPE_TIMESTAMPTZ
	case types.QTypeUUID:
		return gen.QType_QTYPE_UUID
	case types.QTypeJSON:
		return gen.QType_QTYPE_JSON
	case types.QTypeNumeric:
		return gen.QType_QTYPE_NUMERIC
	case types.QTypeEnum:
		return gen.QType_QTYPE_ENUM
	default:
		return gen.QType_QTYPE_UNSPECIFIED
	}
}

func qTypeFromProto(t gen.QType, precision, scale int, enumValues []string) types.QType {
	switch t {
	case gen.QType_QTYPE_BOOL:
		return types.QTypeBool{}
	case gen.QType_QTYPE_INT16:
		return types.QTypeInt16{}
	case gen.QType_QTYPE_INT32:
		return types.QTypeInt32{}
	case gen.QType_QTYPE_INT64:
		return types.QTypeInt64{}
	case gen.QType_QTYPE_FLOAT32:
		return types.QTypeFloat32{}
	case gen.QType_QTYPE_FLOAT64:
		return types.QTypeFloat64{}
	case gen.QType_QTYPE_STRING:
		return types.QTypeString{}
	case gen.QType_QTYPE_BYTES:
		return types.QTypeBytes{}
	case gen.QType_QTYPE_DATE:
		return types.QTypeDate{}
	case gen.QType_QTYPE_TIME:
		return types.QTypeTime{}
	case gen.QType_QTYPE_TIMESTAMP:
		return types.QTypeTimestamp{}
	case gen.QType_QTYPE_TIMESTAMPTZ:
		return types.QTypeTimestampTZ{}
	case gen.QType_QTYPE_UUID:
		return types.QTypeUUID{}
	case gen.QType_QTYPE_JSON:
		return types.QTypeJSON{}
	case gen.QType_QTYPE_NUMERIC:
		return types.QTypeNumeric{Precision: precision, Scale: scale}
	case gen.QType_QTYPE_ENUM:
		return types.QTypeEnum{Values: enumValues}
	default:
		return types.QTypeString{}
	}
}
