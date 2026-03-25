package middleware

import (
	"context"
	"errors"
	"peerdb-playground/errs"

	"connectrpc.com/connect"
)

func ErrorHandler() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			resp, err := next(ctx, req)
			if err == nil {
				return resp, nil
			}

			var appErr errs.Error
			if errors.As(err, &appErr) {
				return nil, connect.NewError(appErr.Code(), err)
			}

			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}
}
