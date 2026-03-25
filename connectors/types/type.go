package types

type QType interface {
	IsQType()
}

type QTypeInt16 struct{}

func (t QTypeInt16) IsQType() {}

type QTypeInt32 struct{}

func (t QTypeInt32) IsQType() {}

type QTypeInt64 struct{}

func (t QTypeInt64) IsQType() {}

type QTypeFloat32 struct{}

func (t QTypeFloat32) IsQType() {}

type QTypeFloat64 struct{}

func (t QTypeFloat64) IsQType() {}

type QTypeString struct{}

func (t QTypeString) IsQType() {}

type QTypeBool struct{}

func (t QTypeBool) IsQType() {}

type QTypeBytes struct{}

func (t QTypeBytes) IsQType() {}

type QTypeDate struct{}

func (t QTypeDate) IsQType() {}

type QTypeTime struct{}

func (t QTypeTime) IsQType() {}

type QTypeTimestamp struct{}

func (t QTypeTimestamp) IsQType() {}

type QTypeTimestampTZ struct{}

func (t QTypeTimestampTZ) IsQType() {}

type QTypeUUID struct{}

func (t QTypeUUID) IsQType() {}

type QTypeJSON struct{}

func (t QTypeJSON) IsQType() {}

type QTypeNumeric struct {
	Precision int
	Scale     int
}

func (t QTypeNumeric) IsQType() {}

type QTypeEnum struct {
	Values []string
}

func (t QTypeEnum) IsQType() {}
