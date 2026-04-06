package sqlutil

import (
	"context"
	"database/sql"
)

type ExecContexter interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}
