package mysql

import (
	"database/sql"
	"fmt"
	"peerdb-playground/connectors/types"
	"strings"
)

func mysqlTypeToQType(
	dataType string,
	columnType string,
	numericPrecision sql.NullInt64,
	numericScale sql.NullInt64,
) (types.QType, error) {
	unsigned := strings.Contains(strings.ToLower(columnType), "unsigned")
	switch strings.ToLower(dataType) {
	case "tinyint":
		if strings.EqualFold(columnType, "tinyint(1)") {
			return types.QTypeBool{}, nil
		}
		if unsigned {
			return types.QTypeInt16{}, nil
		}
		return types.QTypeInt16{}, nil
	case "smallint":
		if unsigned {
			return types.QTypeInt32{}, nil
		}
		return types.QTypeInt16{}, nil
	case "mediumint", "int", "integer":
		if unsigned {
			return types.QTypeInt64{}, nil
		}
		return types.QTypeInt32{}, nil
	case "bigint":
		if unsigned {
			return types.QTypeNumeric{Precision: 20, Scale: 0}, nil
		}
		return types.QTypeInt64{}, nil
	case "decimal", "numeric":
		return types.QTypeNumeric{
			Precision: int(nullInt64OrZero(numericPrecision)),
			Scale:     int(nullInt64OrZero(numericScale)),
		}, nil
	case "float":
		return types.QTypeFloat32{}, nil
	case "double", "real":
		return types.QTypeFloat64{}, nil
	case "char", "varchar", "text", "tinytext", "mediumtext", "longtext":
		return types.QTypeString{}, nil
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bit":
		return types.QTypeBytes{}, nil
	case "date":
		return types.QTypeDate{}, nil
	case "time":
		return types.QTypeTime{}, nil
	case "datetime":
		return types.QTypeTimestamp{}, nil
	case "timestamp":
		return types.QTypeTimestampTZ{}, nil
	case "json":
		return types.QTypeJSON{}, nil
	case "year":
		return types.QTypeInt32{}, nil
	default:
		return nil, fmt.Errorf("unsupported mysql data type %s", dataType)
	}
}

func nullInt64OrZero(v sql.NullInt64) int64 {
	if !v.Valid {
		return 0
	}
	return v.Int64
}
