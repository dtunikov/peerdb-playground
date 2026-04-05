package e2e

import (
	"context"
	"crypto/rand"
	"database/sql"
	"io"
	"log/slog"
	"net/http/httptest"
	"slices"
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
	ID   int64
	Name string
}

type sqlExecContext interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type sourceDialect struct {
	Schema            string
	PlaceholderFormat sq.PlaceholderFormat
}

var postgresDialect = sourceDialect{
	Schema:            "public",
	PlaceholderFormat: sq.Dollar,
}

func mysqlDialect(database string) sourceDialect {
	return sourceDialect{
		Schema:            database,
		PlaceholderFormat: sq.Question,
	}
}

type GRPCE2ESuite struct {
	suite.Suite

	ctx context.Context

	pgContainer *tcpg.PostgresContainer
	mysqlContainer testcontainers.Container
	chContainer testcontainers.Container

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
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))

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
			return slices.Equal(seedRows, snapshotRows)
		},
		45*time.Second,
		1*time.Second,
		"snapshot table=%s rows=%v err=%v",
		tableName,
		snapshotRows,
		snapshotErr,
	)

	cdcRow := userRow{ID: 3, Name: "carol"}
	insertSQL, insertArgs, err := sq.StatementBuilder.
		PlaceholderFormat(dialect.PlaceholderFormat).
		Insert(qualifiedName).
		Columns("id", "name").
		Values(cdcRow.ID, cdcRow.Name).
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
			return slices.Equal(expectedRows, cdcRows)
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
