package e2e

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	"peerdb-playground/gen"
	chpkg "peerdb-playground/pkg/clickhouse"

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
			Type: gen.PeerType_POSTGRES,
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

	return sourcePeer.Id
}

func (s *GRPCE2ESuite) createMySQLPeer(ctx context.Context) string {
	sourcePeer, err := s.env.APIClient.CreatePeer(ctx, &gen.CreatePeerRequest{
		Peer: &gen.Peer{
			Name: fmt.Sprintf("mysql-source-%s", rand.Text()),
			Type: gen.PeerType_MYSQL,
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

	return sourcePeer.Id
}

func (s *GRPCE2ESuite) createClickHousePeer(ctx context.Context) string {
	destPeer, err := s.env.APIClient.CreatePeer(ctx, &gen.CreatePeerRequest{
		Peer: &gen.Peer{
			Name: fmt.Sprintf("destination-clickhouse-%s", rand.Text()),
			Type: gen.PeerType_CLICKHOUSE,
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
	return destPeer.Id
}

func (s *GRPCE2ESuite) createAndSeedUsersTable(
	ctx context.Context,
	db sqlExecContext,
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

	seedRows = []userRow{
		{
			ID: 1, Name: "alice", IsActive: true, Age: 30, Rating: 100,
			Price: 1.5, Score: 99.99,
			Birthday:  time.Date(1994, 1, 1, 0, 0, 0, 0, time.UTC),
			CreatedAt: time.Date(2025, 1, 1, 10, 0, 0, 0, time.UTC),
			Balance:   "1234.56",
			Bio:       `{"hobby":"chess"}`,
			Avatar:    []byte{0xDE, 0xAD},
		},
		{
			ID: 2, Name: "bob", IsActive: false, Age: 25, Rating: 200,
			Price: 2.5, Score: 50.01,
			Birthday:  time.Date(1999, 6, 15, 0, 0, 0, 0, time.UTC),
			CreatedAt: time.Date(2025, 3, 15, 8, 30, 0, 0, time.UTC),
			Balance:   "0.01",
			Bio:       `{"hobby":"go"}`,
			Avatar:    []byte{0xBE, 0xEF},
		},
	}

	cols := []string{"id", "name", "is_active", "age", "rating", "price", "score", "birthday", "created_at", "balance", "bio", "avatar"}
	insert := sq.StatementBuilder.
		PlaceholderFormat(dialect.PlaceholderFormat).
		Insert(qualifiedName).
		Columns(cols...)
	for _, r := range seedRows {
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

	s.T().Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = db.ExecContext(cleanupCtx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, qualifiedName))
	})

	return tableName, qualifiedName, seedRows
}

func (s *GRPCE2ESuite) loadUsersTableFromClickhouse(ctx context.Context, tableName string) ([]userRow, error) {
	conn, err := chpkg.Connect(ctx, chpkg.Config{
		Host:     s.env.ClickHouseHost,
		Port:     int(s.env.ClickHousePort),
		User:     clickhouseUser,
		Password: clickhousePassword,
		Database: clickhouseDatabase,
	})
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rows, err := conn.Query(ctx, fmt.Sprintf(
		`SELECT id, name, is_active, age, rating, price, score, birthday, created_at, balance, bio, avatar FROM "%s" FINAL ORDER BY id`,
		tableName,
	))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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
