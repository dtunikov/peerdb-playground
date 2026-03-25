package types

import "time"

type QValue interface {
	Value() any
}

type QValueInt16 struct {
	Val int16
}

func (v QValueInt16) Value() any {
	return v.Val
}

type QValueInt32 struct {
	Val int32
}

func (v QValueInt32) Value() any {
	return v.Val
}

type QValueInt64 struct {
	Val int64
}

func (v QValueInt64) Value() any {
	return v.Val
}

type QValueFloat32 struct {
	Val float32
}

func (v QValueFloat32) Value() any {
	return v.Val
}

type QValueFloat64 struct {
	Val float64
}

func (v QValueFloat64) Value() any {
	return v.Val
}

type QValueString struct {
	Val string
}

func (v QValueString) Value() any {
	return v.Val
}

type QValueBool struct {
	Val bool
}

func (v QValueBool) Value() any {
	return v.Val
}

type QValueBytes struct {
	Val []byte
}

func (v QValueBytes) Value() any {
	return v.Val
}

type QValueDate struct {
	Val time.Time
}

func (v QValueDate) Value() any {
	return v.Val
}

type QValueTime struct {
	Val time.Time
}

func (v QValueTime) Value() any {
	return v.Val
}

type QValueTimestamp struct {
	Val time.Time
}

func (v QValueTimestamp) Value() any {
	return v.Val
}

type QValueTimestampTZ struct {
	Val time.Time
}

func (v QValueTimestampTZ) Value() any {
	return v.Val
}

type QValueUUID struct {
	Val string
}

func (v QValueUUID) Value() any {
	return v.Val
}

type QValueJSON struct {
	Val string
}

func (v QValueJSON) Value() any {
	return v.Val
}

type QValueNumeric struct {
	Val string
}

func (v QValueNumeric) Value() any {
	return v.Val
}

type QValueEnum struct {
	Val string
}

func (v QValueEnum) Value() any {
	return v.Val
}

type QValueNull struct{}

func (v QValueNull) Value() any {
	return nil
}
