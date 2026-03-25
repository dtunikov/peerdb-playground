package postgres

import "peerdb-playground/connectors/types"

func pgTypeToQType(udtName string, precision, scale *int) types.QType {
	switch udtName {
	case "bool":
		return types.QTypeBool{}
	case "int2":
		return types.QTypeInt16{}
	case "int4":
		return types.QTypeInt32{}
	case "int8":
		return types.QTypeInt64{}
	case "float4":
		return types.QTypeFloat32{}
	case "float8":
		return types.QTypeFloat64{}
	case "numeric":
		p, s := 0, 0
		if precision != nil {
			p = *precision
		}
		if scale != nil {
			s = *scale
		}
		return types.QTypeNumeric{Precision: p, Scale: s}
	case "text", "varchar", "bpchar":
		return types.QTypeString{}
	case "bytea":
		return types.QTypeBytes{}
	case "date":
		return types.QTypeDate{}
	case "time", "timetz":
		return types.QTypeTime{}
	case "timestamp":
		return types.QTypeTimestamp{}
	case "timestamptz":
		return types.QTypeTimestampTZ{}
	case "uuid":
		return types.QTypeUUID{}
	case "json", "jsonb":
		return types.QTypeJSON{}
	default:
		// fallback to string for unknown types
		return types.QTypeString{}
	}
}
