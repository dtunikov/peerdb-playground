package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	mathrand "math/rand/v2"
	"reflect"
	"strings"
	"sync/atomic"
	"time"

	"peerdb-playground/gen"
	"peerdb-playground/internal/sqlutil"

	sq "github.com/Masterminds/squirrel"
	"github.com/shopspring/decimal"
)

const (
	encryptionKey      = "0123456789abcdef0123456789abcdef"
	clickhouseDatabase = "destination"
	clickhouseUser     = "default"
	clickhousePassword = "clickhouse"
	taskQueue          = "cdc-flow-e2e"
	postgresUser       = "postgres"
	postgresPassword   = "postgres"
	postgresDB         = "peerdb"
	mysqlUser          = "root"
	mysqlPassword      = "mysql"
	mysqlDatabase      = "source"
)

func (s *GRPCE2ESuite) createPostgresPeer(ctx context.Context) string {
	sourcePeer, err := s.env.APIClient.CreatePeer(ctx, &gen.CreatePeerRequest{
		Peer: &gen.Peer{
			Name: fmt.Sprintf("pg-source-%s", rand.Text()),
			Type: gen.PeerType_PEER_TYPE_POSTGRES,
			Config: &gen.Peer_PostgresConfig{
				PostgresConfig: &gen.PostgresConfig{
					Host:     s.env.PostgresHost,
					Port:     s.env.PostgresPort,
					User:     postgresUser,
					Password: postgresPassword,
					Database: postgresDB,
					SslMode:  "disable",
				},
			},
		},
	})
	s.Require().NoError(err)

	return sourcePeer.Peer.Id
}

func (s *GRPCE2ESuite) createMySQLPeer(ctx context.Context) string {
	sourcePeer, err := s.env.APIClient.CreatePeer(ctx, &gen.CreatePeerRequest{
		Peer: &gen.Peer{
			Name: fmt.Sprintf("mysql-source-%s", rand.Text()),
			Type: gen.PeerType_PEER_TYPE_MYSQL,
			Config: &gen.Peer_MysqlConfig{
				MysqlConfig: &gen.MysqlConfig{
					Host:     s.env.MySQLHost,
					Port:     s.env.MySQLPort,
					User:     mysqlUser,
					Password: mysqlPassword,
					Database: mysqlDatabase,
				},
			},
		},
	})
	s.Require().NoError(err)

	return sourcePeer.Peer.Id
}

func (s *GRPCE2ESuite) createClickHousePeer(ctx context.Context) string {
	destPeer, err := s.env.APIClient.CreatePeer(ctx, &gen.CreatePeerRequest{
		Peer: &gen.Peer{
			Name: fmt.Sprintf("destination-clickhouse-%s", rand.Text()),
			Type: gen.PeerType_PEER_TYPE_CLICKHOUSE,
			Config: &gen.Peer_ClickhouseConfig{
				ClickhouseConfig: &gen.ClickhouseConfig{
					Host:     s.env.ClickHouseHost,
					Port:     s.env.ClickHousePort,
					User:     clickhouseUser,
					Password: clickhousePassword,
					Database: clickhouseDatabase,
				},
			},
		},
	})
	s.Require().NoError(err)
	return destPeer.Peer.Id
}

var nextUserID atomic.Int64

func randomUserRow() userRow {
	id := nextUserID.Add(1)

	avatar := make([]byte, 2+mathrand.IntN(8))
	_, _ = rand.Read(avatar)

	balance := decimal.New(mathrand.Int64N(1_000_000), -2) // 0.00 – 9999.99

	bio := fmt.Sprintf(`{"key":"%s"}`, rand.Text()[:8])

	return userRow{
		ID:       id,
		Name:     strings.ToLower(rand.Text()[:10]),
		IsActive: mathrand.IntN(2) == 1,
		Age:      int16(18 + mathrand.IntN(62)),
		Rating:   int32(mathrand.IntN(10000)),
		Price:    float32(mathrand.IntN(10000)) / 100, // e.g. 12.34 – avoids float round-trip issues
		Score:    float64(mathrand.IntN(100000)) / 100,
		Birthday: time.Date(
			1970+mathrand.IntN(80),
			time.Month(1+mathrand.IntN(12)),
			1+mathrand.IntN(28),
			0, 0, 0, 0, time.UTC,
		),
		CreatedAt: time.Date(
			2020+mathrand.IntN(6),
			time.Month(1+mathrand.IntN(12)),
			1+mathrand.IntN(28),
			mathrand.IntN(24), mathrand.IntN(60), mathrand.IntN(60),
			0, time.UTC,
		),
		Balance: balance.StringFixed(2),
		Bio:     bio,
		Avatar:  avatar,
	}
}

func (s *GRPCE2ESuite) insertRows(ctx context.Context, db sqlutil.ExecContexter, dialect sourceDialect, qualifiedTableName string, rows []userRow) {
	cols := []string{"id", "name", "is_active", "age", "rating", "price", "score", "birthday", "created_at", "balance", "bio", "avatar"}
	insert := sq.StatementBuilder.
		PlaceholderFormat(dialect.PlaceholderFormat).
		Insert(qualifiedTableName).
		Columns(cols...)
	for _, r := range rows {
		insert = insert.Values(
			r.ID, r.Name, r.IsActive, r.Age, r.Rating,
			r.Price, r.Score, r.Birthday, r.CreatedAt,
			r.Balance, r.Bio, r.Avatar,
		)
	}
	seedSQL, seedArgs, err := insert.ToSql()
	s.Require().NoError(err)
	_, err = db.ExecContext(ctx, seedSQL, seedArgs...)
	s.Require().NoError(err)
}

func (s *GRPCE2ESuite) deleteRows(ctx context.Context, db sqlutil.ExecContexter, dialect sourceDialect, qualifiedTableName string, ids []int64) {
	if len(ids) == 0 {
		return
	}
	del := sq.StatementBuilder.
		PlaceholderFormat(dialect.PlaceholderFormat).
		Delete(qualifiedTableName).
		Where(sq.Eq{"id": ids})
	sqlStr, args, err := del.ToSql()
	s.Require().NoError(err)
	_, err = db.ExecContext(ctx, sqlStr, args...)
	s.Require().NoError(err)
}

func (s *GRPCE2ESuite) createAndSeedUsersTable(
	ctx context.Context,
	db sqlutil.ExecContexter,
	dialect sourceDialect,
) (tableName, qualifiedName string, seedRows []userRow) {
	tableName = fmt.Sprintf("users_%s", strings.ToLower(rand.Text()))
	qualifiedName = fmt.Sprintf("%s.%s", dialect.Schema, tableName)

	_, err := db.ExecContext(ctx, fmt.Sprintf(
		`CREATE TABLE IF NOT EXISTS %s (%s)`,
		qualifiedName,
		dialect.DDL,
	))
	s.Require().NoError(err)

	seedRows = []userRow{randomUserRow(), randomUserRow()}

	s.insertRows(ctx, db, dialect, qualifiedName, seedRows)
	s.T().Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, qualifiedName))
	})

	return tableName, qualifiedName, seedRows
}

type userRow struct {
	ID        int64
	Name      string
	IsActive  bool
	Age       int16
	Rating    int32
	Price     float32
	Score     float64
	Birthday  time.Time
	CreatedAt time.Time
	Balance   string
	Bio       string
	Avatar    []byte
}

type sourceDialect struct {
	Schema            string
	PlaceholderFormat sq.PlaceholderFormat
	// DDL is the CREATE TABLE column definitions (excluding the primary key column "id BIGINT PRIMARY KEY").
	DDL string
}

var postgresDialect = sourceDialect{
	Schema:            "public",
	PlaceholderFormat: sq.Dollar,
	DDL: `id BIGINT PRIMARY KEY,
		name TEXT NOT NULL,
		is_active BOOLEAN NOT NULL,
		age SMALLINT NOT NULL,
		rating INTEGER NOT NULL,
		price REAL NOT NULL,
		score DOUBLE PRECISION NOT NULL,
		birthday DATE NOT NULL,
		created_at TIMESTAMP NOT NULL,
		balance NUMERIC(10,2) NOT NULL,
		bio JSONB NOT NULL,
		avatar BYTEA NOT NULL`,
}

func mysqlDialect(database string) sourceDialect {
	return sourceDialect{
		Schema:            database,
		PlaceholderFormat: sq.Question,
		DDL: `id BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			is_active TINYINT(1) NOT NULL,
			age SMALLINT NOT NULL,
			rating INT NOT NULL,
			price FLOAT NOT NULL,
			score DOUBLE NOT NULL,
			birthday DATE NOT NULL,
			created_at DATETIME NOT NULL,
			balance DECIMAL(10,2) NOT NULL,
			bio JSON NOT NULL,
			avatar BLOB NOT NULL`,
	}
}

func userRowsEqual(a, b []userRow) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].ID != b[i].ID ||
			a[i].Name != b[i].Name ||
			a[i].IsActive != b[i].IsActive ||
			a[i].Age != b[i].Age ||
			a[i].Rating != b[i].Rating ||
			math.Float32bits(a[i].Price) != math.Float32bits(b[i].Price) ||
			math.Float64bits(a[i].Score) != math.Float64bits(b[i].Score) ||
			!a[i].Birthday.Equal(b[i].Birthday) ||
			!a[i].CreatedAt.Truncate(time.Second).Equal(b[i].CreatedAt.Truncate(time.Second)) ||
			a[i].Balance != b[i].Balance ||
			!jsonStringsEqual(a[i].Bio, b[i].Bio) ||
			!bytes.Equal(a[i].Avatar, b[i].Avatar) {
			return false
		}
	}
	return true
}

func jsonStringsEqual(a, b string) bool {
	if a == b {
		return true
	}

	var av any
	if err := json.Unmarshal([]byte(a), &av); err != nil {
		return false
	}

	var bv any
	if err := json.Unmarshal([]byte(b), &bv); err != nil {
		return false
	}

	return reflect.DeepEqual(av, bv)
}

func (s *GRPCE2ESuite) waitForEqualRows(ctx context.Context, expected []userRow, tableName string) {
	var got []userRow
	var err error
	ok := s.Assert().Eventuallyf(
		func() bool {
			got, err = s.loadUsersTableFromClickhouse(ctx, tableName)
			if err != nil {
				return false
			}
			return userRowsEqual(expected, got)
		},
		45*time.Second,
		1*time.Second,
		"waitForEqualRows table=%s expected=%v got=%v err=%v",
		tableName,
		expected,
		got,
		err,
	)
	if !ok {
		s.debugDumpClickhouse(ctx, tableName)
		s.FailNow("waitForEqualRows failed")
	}
}

func (s *GRPCE2ESuite) debugDumpClickhouse(ctx context.Context, tableName string) {
	type row struct{ DB, Table string }
	rows, err := s.env.ClickHouseConn.Query(ctx, `SELECT database, name FROM system.tables WHERE name LIKE 'users_%' ORDER BY database, name`)
	if err != nil {
		slog.Error("debug: list tables failed", "err", err)
		return
	}
	defer rows.Close() //nolint:errcheck
	var found []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.DB, &r.Table); err == nil {
			found = append(found, r)
		}
	}
	slog.Error("debug: existing users_* tables in ClickHouse", "expected_table", tableName, "found", found)

	countRows, err := s.env.ClickHouseConn.Query(ctx, fmt.Sprintf(`SELECT count() FROM "%s"`, tableName))
	if err != nil {
		slog.Error("debug: count query failed", "table", tableName, "err", err)
		return
	}
	defer countRows.Close() //nolint:errcheck
	for countRows.Next() {
		var c uint64
		_ = countRows.Scan(&c)
		slog.Error("debug: row count (no FINAL)", "table", tableName, "count", c)
	}

	finalCountRows, err := s.env.ClickHouseConn.Query(ctx, fmt.Sprintf(`SELECT count() FROM "%s" FINAL`, tableName))
	if err != nil {
		slog.Error("debug: count FINAL query failed", "table", tableName, "err", err)
	} else {
		for finalCountRows.Next() {
			var c uint64
			_ = finalCountRows.Scan(&c)
			slog.Error("debug: row count (FINAL)", "table", tableName, "count", c)
		}
		_ = finalCountRows.Close()
	}

	rawRows, err := s.env.ClickHouseConn.Query(ctx, fmt.Sprintf(`SELECT id, name, _version, _ingested_at FROM "%s" ORDER BY id, _version`, tableName))
	if err != nil {
		slog.Error("debug: raw query failed", "table", tableName, "err", err)
		return
	}
	defer rawRows.Close() //nolint:errcheck
	for rawRows.Next() {
		var id int64
		var name string
		var version uint64
		var ingestedAt time.Time
		_ = rawRows.Scan(&id, &name, &version, &ingestedAt)
		slog.Error("debug: raw row", "table", tableName, "id", id, "name", name, "_version", version, "_ingested_at", ingestedAt)
	}
}

func (s *GRPCE2ESuite) loadUsersTableFromClickhouse(ctx context.Context, tableName string) ([]userRow, error) {
	rows, err := s.env.ClickHouseConn.Query(ctx, fmt.Sprintf(
		`SELECT id, name, is_active, age, rating, price, score, birthday, created_at, balance, bio, avatar FROM "%s" FINAL WHERE _is_deleted = 0 ORDER BY id`,
		tableName,
	))
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var out []userRow
	for rows.Next() {
		var (
			row     userRow
			balance decimal.Decimal
			avatar  []byte
		)
		if err := rows.Scan(
			&row.ID, &row.Name, &row.IsActive, &row.Age, &row.Rating,
			&row.Price, &row.Score, &row.Birthday, &row.CreatedAt,
			&balance, &row.Bio, &avatar,
		); err != nil {
			return nil, err
		}
		row.Balance = balance.StringFixed(2)
		row.Avatar = append([]byte(nil), avatar...)
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}
