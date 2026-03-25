package postgres

import (
	"fmt"
	"peerdb-playground/connectors"
	"peerdb-playground/connectors/types"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

func convertValue(name string, qtype types.QType, value any) connectors.ColumnValue {
	if value == nil {
		return connectors.ColumnValue{Name: name, Value: types.QValueNull{}}
	}

	var qval types.QValue
	switch qtype.(type) {
	case types.QTypeBool:
		qval = types.QValueBool{Val: value.(bool)}
	case types.QTypeInt16:
		qval = types.QValueInt16{Val: value.(int16)}
	case types.QTypeInt32:
		qval = types.QValueInt32{Val: value.(int32)}
	case types.QTypeInt64:
		qval = types.QValueInt64{Val: value.(int64)}
	case types.QTypeFloat32:
		qval = types.QValueFloat32{Val: value.(float32)}
	case types.QTypeFloat64:
		qval = types.QValueFloat64{Val: value.(float64)}
	case types.QTypeString, types.QTypeEnum:
		qval = types.QValueString{Val: value.(string)}
	case types.QTypeBytes:
		qval = types.QValueBytes{Val: value.([]byte)}
	case types.QTypeDate:
		qval = types.QValueDate{Val: value.(time.Time)}
	case types.QTypeTime:
		qval = types.QValueTime{Val: value.(time.Time)}
	case types.QTypeTimestamp:
		qval = types.QValueTimestamp{Val: value.(time.Time)}
	case types.QTypeTimestampTZ:
		qval = types.QValueTimestampTZ{Val: value.(time.Time)}
	case types.QTypeUUID:
		// pgx returns UUID as [16]byte
		if uid, ok := value.([16]byte); ok {
			qval = types.QValueUUID{Val: fmt.Sprintf("%x-%x-%x-%x-%x", uid[0:4], uid[4:6], uid[6:8], uid[8:10], uid[10:16])}
		} else {
			qval = types.QValueUUID{Val: fmt.Sprintf("%v", value)}
		}
	case types.QTypeJSON:
		qval = types.QValueJSON{Val: fmt.Sprintf("%v", value)}
	case types.QTypeNumeric:
		if n, ok := value.(pgtype.Numeric); ok {
			var s string
			b, _ := n.MarshalJSON()
			s = string(b)
			qval = types.QValueNumeric{Val: s}
		} else {
			qval = types.QValueNumeric{Val: fmt.Sprintf("%v", value)}
		}
	default:
		qval = types.QValueString{Val: fmt.Sprintf("%v", value)}
	}

	return connectors.ColumnValue{Name: name, Value: qval}
}
