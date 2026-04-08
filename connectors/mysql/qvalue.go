package mysql

import (
	"fmt"
	"peerdb-playground/connectors"
	"peerdb-playground/connectors/types"
	"strconv"
	"time"
)

func mysqlColumnValue(name string, qtype types.QType, value any) (connectors.ColumnValue, error) {
	if value == nil {
		return connectors.ColumnValue{Name: name, Value: types.QValueNull{}}, nil
	}

	switch qtype.(type) {
	case types.QTypeBool:
		v, err := mysqlBoolValue(value)
		return connectors.ColumnValue{Name: name, Value: types.QValueBool{Val: v}}, err
	case types.QTypeInt16:
		v, err := mysqlInt64Value(value)
		return connectors.ColumnValue{Name: name, Value: types.QValueInt16{Val: int16(v)}}, err
	case types.QTypeInt32:
		v, err := mysqlInt64Value(value)
		return connectors.ColumnValue{Name: name, Value: types.QValueInt32{Val: int32(v)}}, err
	case types.QTypeInt64:
		v, err := mysqlInt64Value(value)
		return connectors.ColumnValue{Name: name, Value: types.QValueInt64{Val: v}}, err
	case types.QTypeFloat32:
		v, err := mysqlFloat64Value(value)
		return connectors.ColumnValue{Name: name, Value: types.QValueFloat32{Val: float32(v)}}, err
	case types.QTypeFloat64:
		v, err := mysqlFloat64Value(value)
		return connectors.ColumnValue{Name: name, Value: types.QValueFloat64{Val: v}}, err
	case types.QTypeString:
		return connectors.ColumnValue{Name: name, Value: types.QValueString{Val: mysqlStringValue(value)}}, nil
	case types.QTypeBytes:
		return connectors.ColumnValue{Name: name, Value: types.QValueBytes{Val: mysqlBytesValue(value)}}, nil
	case types.QTypeDate:
		v, err := mysqlDateTimeValue(value)
		return connectors.ColumnValue{Name: name, Value: types.QValueDate{Val: v}}, err
	case types.QTypeTime:
		v, err := mysqlTimeValue(value)
		return connectors.ColumnValue{Name: name, Value: types.QValueTime{Val: v}}, err
	case types.QTypeTimestamp:
		v, err := mysqlDateTimeValue(value)
		return connectors.ColumnValue{Name: name, Value: types.QValueTimestamp{Val: v}}, err
	case types.QTypeTimestampTZ:
		v, err := mysqlDateTimeValue(value)
		return connectors.ColumnValue{Name: name, Value: types.QValueTimestampTZ{Val: v}}, err
	case types.QTypeJSON:
		return connectors.ColumnValue{Name: name, Value: types.QValueJSON{Val: mysqlStringValue(value)}}, nil
	case types.QTypeNumeric:
		return connectors.ColumnValue{Name: name, Value: types.QValueNumeric{Val: mysqlNumericString(value)}}, nil
	default:
		return connectors.ColumnValue{Name: name, Value: types.QValueString{Val: mysqlStringValue(value)}}, nil
	}
}

func mysqlBoolValue(value any) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int8:
		return v != 0, nil
	case int16:
		return v != 0, nil
	case int32:
		return v != 0, nil
	case int64:
		return v != 0, nil
	case uint8:
		return v != 0, nil
	case uint16:
		return v != 0, nil
	case uint32:
		return v != 0, nil
	case uint64:
		return v != 0, nil
	case []byte:
		return string(v) != "0", nil
	case string:
		return v != "0", nil
	default:
		return false, fmt.Errorf("unsupported bool value type %T", value)
	}
}

func mysqlInt64Value(value any) (int64, error) {
	switch v := value.(type) {
	case int8:
		return int64(v), nil
	case int16:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case uint8:
		return int64(v), nil
	case uint16:
		return int64(v), nil
	case uint32:
		return int64(v), nil
	case uint64:
		if v > uint64(^uint64(0)>>1) {
			return 0, fmt.Errorf("uint64 value %d overflows int64", v)
		}
		return int64(v), nil
	case []byte:
		return strconv.ParseInt(string(v), 10, 64)
	case string:
		return strconv.ParseInt(v, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported integer value type %T", value)
	}
}

func mysqlFloat64Value(value any) (float64, error) {
	switch v := value.(type) {
	case float32:
		return float64(v), nil
	case float64:
		return v, nil
	case []byte:
		return strconv.ParseFloat(string(v), 64)
	case string:
		return strconv.ParseFloat(v, 64)
	default:
		return 0, fmt.Errorf("unsupported float value type %T", value)
	}
}

func mysqlStringValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(value)
	}
}

func mysqlBytesValue(value any) []byte {
	switch v := value.(type) {
	case []byte:
		return append([]byte(nil), v...)
	case string:
		return []byte(v)
	default:
		return []byte(fmt.Sprint(value))
	}
}

func mysqlDateTimeValue(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case []byte:
		return parseMySQLDateTimeString(string(v))
	case string:
		return parseMySQLDateTimeString(v)
	default:
		return time.Time{}, fmt.Errorf("unsupported datetime value type %T", value)
	}
}

func mysqlTimeValue(value any) (time.Time, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case []byte:
		return parseMySQLTimeString(string(v))
	case string:
		return parseMySQLTimeString(v)
	default:
		return time.Time{}, fmt.Errorf("unsupported time value type %T", value)
	}
}

func mysqlNumericString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(value)
	}
}

func parseMySQLDateTimeString(value string) (time.Time, error) {
	layouts := []string{
		"2006-01-02 15:04:05.999999",
		"2006-01-02 15:04:05",
		"2006-01-02",
		time.RFC3339Nano,
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported datetime value %q", value)
}

func parseMySQLTimeString(value string) (time.Time, error) {
	layouts := []string{
		"15:04:05.999999",
		"15:04:05",
	}
	for _, layout := range layouts {
		if t, err := time.Parse(layout, value); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unsupported time value %q", value)
}
