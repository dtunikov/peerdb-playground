package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"log/slog"
	"math"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
	"time"

	"peerdb-playground/gen"
	"peerdb-playground/gen/genconnect"
	"peerdb-playground/server"

	sq "github.com/Masterminds/squirrel"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
)

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

type sqlExecContext interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
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

type GRPCE2ESuite struct {
	suite.Suite

	ctx context.Context

	pgContainer    *tcpg.PostgresContainer
	mysqlContainer testcontainers.Container
	chContainer    testcontainers.Container

	pgPool   *pgxpool.Pool
	pgDB     *sql.DB
	mysqlDB  *sql.DB
	chConn   driver.Conn
	worker   worker.Worker
	api      *httptest.Server
	temporal *testsuite.DevServer

	apiClient       genconnect.PeerdbServiceClient
	temporalClient  client.Client
	postgresHost    string
	postgresPort    uint32
	mysqlHost       string
	mysqlPort       uint32
	clickhouseHost  string
	clickhousePort  uint32
	lastWorkflowID  string
	projectRootPath string
}

func (s *GRPCE2ESuite) SetupSuite() {
	if testing.Short() {
		s.T().Skip("skipping e2e suite in short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(s.T())

	s.ctx = context.Background()
	s.projectRootPath = projectRoot()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	s.startPostgres()
	s.runMigrations()
	s.startMySQL()
	s.startClickHouse()
	s.startTemporal()
	s.startWorkerAndAPI()
}

func (s *GRPCE2ESuite) TearDownTest() {
	if s.lastWorkflowID == "" || s.temporalClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = s.temporalClient.TerminateWorkflow(ctx, s.lastWorkflowID, "", "e2e cleanup")
	s.lastWorkflowID = ""
}

func (s *GRPCE2ESuite) testCdcFlow(
	ctx context.Context,
	sourcePeerId string,
	sourceConn sqlExecContext,
	dialect sourceDialect,
	cdcFlowSourceConfig gen.CdcFlowConfigSourceConfig,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tableName, qualifiedName, seedRows := s.createAndSeedUsersTable(ctx, sourceConn, dialect)
	var snapshotRows []userRow
	var snapshotErr error

	destPeerId := s.createClickHousePeer(ctx)

	flow, err := s.apiClient.CreateCDCFlow(ctx, &gen.CreateCDCFlowRequest{
		CdcFlow: &gen.CDCFlow{
			Name:        rand.Text(),
			Source:      sourcePeerId,
			Destination: destPeerId,
			Config: &gen.CdcFlowConfig{
				Tables:       []string{qualifiedName},
				SourceConfig: cdcFlowSourceConfig,
			},
		},
	})
	s.Require().NoError(err)

	s.lastWorkflowID = server.CdcFlowPrefix + flow.Id

	s.Require().Eventuallyf(
		func() bool {
			snapshotRows, snapshotErr = s.loadUsersTableFromClickhouse(ctx, tableName)
			if snapshotErr != nil {
				return false
			}
			return userRowsEqual(seedRows, snapshotRows)
		},
		45*time.Second,
		1*time.Second,
		"snapshot table=%s rows=%v err=%v",
		tableName,
		snapshotRows,
		snapshotErr,
	)

	cdcRow := userRow{
		ID:        3,
		Name:      "carol",
		IsActive:  false,
		Age:       28,
		Rating:    300,
		Price:     3.14,
		Score:     2.718281828,
		Birthday:  time.Date(1998, 3, 15, 0, 0, 0, 0, time.UTC),
		CreatedAt: time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC),
		Balance:   "999.99",
		Bio:       `{"role":"admin"}`,
		Avatar:    []byte{0xCA, 0xFE},
	}
	cols := []string{"id", "name", "is_active", "age", "rating", "price", "score", "birthday", "created_at", "balance", "bio", "avatar"}
	insertSQL, insertArgs, err := sq.StatementBuilder.
		PlaceholderFormat(dialect.PlaceholderFormat).
		Insert(qualifiedName).
		Columns(cols...).
		Values(
			cdcRow.ID, cdcRow.Name, cdcRow.IsActive, cdcRow.Age, cdcRow.Rating,
			cdcRow.Price, cdcRow.Score, cdcRow.Birthday, cdcRow.CreatedAt,
			cdcRow.Balance, cdcRow.Bio, cdcRow.Avatar,
		).
		ToSql()
	s.Require().NoError(err)
	_, err = sourceConn.ExecContext(ctx, insertSQL, insertArgs...)
	s.Require().NoError(err)

	expectedRows := append(seedRows, cdcRow)
	var cdcErr error
	var cdcRows []userRow
	s.Require().Eventually(
		func() bool {
			cdcRows, cdcErr = s.loadUsersTableFromClickhouse(ctx, tableName)
			if cdcErr != nil {
				return false
			}
			return userRowsEqual(expectedRows, cdcRows)
		},
		45*time.Second,
		1*time.Second,
		"cdc table=%s rows=%v err=%v",
		tableName,
		cdcRows,
		cdcErr,
	)
}

func (s *GRPCE2ESuite) TestCdcPostgres() {
	sourcePeerId := s.createPostgresPeer(s.ctx)
	s.testCdcFlow(s.ctx, sourcePeerId, s.pgDB, postgresDialect, &gen.CdcFlowConfig_PostgresSource{
		PostgresSource: &gen.PostgresSourceConfig{},
	})
}

func (s *GRPCE2ESuite) TestCdcMySQL() {
	sourcePeerID := s.createMySQLPeer(s.ctx)
	s.testCdcFlow(s.ctx, sourcePeerID, s.mysqlDB, mysqlDialect(mysqlDatabase), &gen.CdcFlowConfig_MysqlSource{
		MysqlSource: &gen.MysqlSourceConfig{},
	})
}

func TestGRPCE2ESuite(t *testing.T) {
	suite.Run(t, new(GRPCE2ESuite))
}
