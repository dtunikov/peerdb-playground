package middleware

import (
	"context"

	"github.com/google/uuid"

	"connectrpc.com/connect"
)

type requestIDKey struct{}

func RequestIDFromContext(ctx context.Context) string {
	if id, ok := ctx.Value(requestIDKey{}).(string); ok {
		return id
	}
	return ""
}

func RequestID() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			id := uuid.NewString()
			ctx = context.WithValue(ctx, requestIDKey{}, id)
			return next(ctx, req)
		}
	}
}
