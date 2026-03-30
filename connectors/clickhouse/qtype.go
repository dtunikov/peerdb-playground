package clickhouse

import (
	"fmt"
	"peerdb-playground/connectors/types"
)

func qTypToClickhouseType(t types.QType) string {
	switch t := t.(type) {
	case types.QTypeBool:
		return "Bool"
	case types.QTypeInt16:
		return "Int16"
	case types.QTypeInt32:
		return "Int32"
	case types.QTypeInt64:
		return "Int64"
	case types.QTypeFloat32:
		return "Float32"
	case types.QTypeFloat64:
		return "Float64"
	case types.QTypeString:
		return "String"
	case types.QTypeBytes:
		return "String"
	case types.QTypeDate:
		return "Date"
	case types.QTypeTime:
		return "String"
	case types.QTypeTimestamp:
		return "DateTime64(6)"
	case types.QTypeTimestampTZ:
		return "DateTime64(6)"
	case types.QTypeUUID:
		return "UUID"
	case types.QTypeJSON:
		return "String"
	case types.QTypeNumeric:
		nt := t
		if nt.Precision > 0 {
			return fmt.Sprintf("Decimal(%d, %d)", nt.Precision, nt.Scale)
		}
		return "String"
	case types.QTypeEnum:
		return "String"
	default:
		return "String"
	}
}
