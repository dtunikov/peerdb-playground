package errs

import (
	"fmt"

	"connectrpc.com/connect"
)

type Error struct {
	code    connect.Code
	message string
	detail  error
}

func (e Error) Error() string {
	return fmt.Sprintf("code=%s, message=%s, detail=%s", e.code, e.message, e.detail)
}

var (
	Internal = Error{
		code: connect.CodeInternal,
	}
	BadRequest = Error{
		code: connect.CodeInvalidArgument,
	}
	NotFound = Error{
		code: connect.CodeNotFound,
	}
)

func (e Error) Code() connect.Code {
	return e.code
}

func (e Error) WithMessage(msg string) Error {
	e.message = msg
	return e
}

func (e Error) WithDetail(err error) Error {
	e.detail = err
	return e
}
