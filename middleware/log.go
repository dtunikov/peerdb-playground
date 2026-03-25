package middleware

import (
	"context"
	"log/slog"
	"time"

	"connectrpc.com/connect"
)

func LogRequest() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			method, reqId := req.Spec().Procedure, RequestIDFromContext(ctx)
			slog.Info("incoming request",
				slog.String("method", method),
				slog.String("request_id", reqId),
			)

			start := time.Now()
			resp, err := next(ctx, req)

			attrs := []slog.Attr{
				slog.String("request_id", reqId),
				slog.String("method", method),
				slog.Duration("duration", time.Since(start)),
			}
			lvl := slog.LevelInfo
			if err != nil {
				attrs = append(attrs, slog.String("error", err.Error()))
				lvl = slog.LevelError
			}
			slog.LogAttrs(ctx, lvl, "request completed", attrs...)

			return resp, err
		}
	}
}
