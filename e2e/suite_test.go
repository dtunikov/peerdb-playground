package e2e

import (
	"context"
	"crypto/rand"
	"log/slog"
	"os"
	"testing"
	"time"

	"peerdb-playground/config"
	"peerdb-playground/gen"
	"peerdb-playground/internal/localenv"
	"peerdb-playground/internal/sqlutil"
	"peerdb-playground/server"

	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"go.temporal.io/api/enums/v1"
)

type GRPCE2ESuite struct {
	suite.Suite

	ctx context.Context

	env            *localenv.Environment
	lastWorkflowID string
}

func (s *GRPCE2ESuite) SetupSuite() {
	if testing.Short() {
		s.T().Skip("skipping e2e suite in short mode")
	}
	testcontainers.SkipIfProviderIsNotHealthy(s.T())

	s.ctx = context.Background()
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug})))

	env, err := localenv.Start(s.ctx, localenv.Options{
		TaskQueue:     taskQueue,
		EncryptionKey: encryptionKey,
		CdcConfig: config.CdcConfig{
			FlushIntervalMs:     100,
			MaxBatchSize:        100,
			HeartbeatIntervalMs: 250,
		},
	})
	s.Require().NoError(err)
	s.env = env

	s.T().Cleanup(func() {
		s.Require().NoError(env.Close(context.Background()))
	})
}

func TestGRPCE2ESuite(t *testing.T) {
	suite.Run(t, new(GRPCE2ESuite))
}

func (s *GRPCE2ESuite) TearDownTest() {
	if s.lastWorkflowID == "" || s.env == nil || s.env.TemporalClient == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_ = s.env.TemporalClient.TerminateWorkflow(ctx, s.lastWorkflowID, "", "e2e cleanup")
	s.lastWorkflowID = ""
}

func (s *GRPCE2ESuite) testCdcFlow(
	_ context.Context,
	sourcePeerId string,
	sourceConn sqlutil.ExecContexter,
	dialect sourceDialect,
	cdcFlowSourceConfig gen.CdcFlowConfigSourceConfig,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	tableName, qualifiedName, seedRows := s.createAndSeedUsersTable(ctx, sourceConn, dialect)
	destPeerId := s.createClickHousePeer(ctx)

	flow, err := s.env.APIClient.CreateCDCFlow(ctx, &gen.CreateCDCFlowRequest{
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

	s.lastWorkflowID = server.CdcFlowPrefix + flow.CdcFlow.Id

	s.waitForEqualRows(ctx, seedRows, tableName)

	// Warm up CDC before the main assertions so the test doesn't race the source stream startup.
	warmupRow := randomUserRow()
	s.insertRows(ctx, sourceConn, dialect, qualifiedName, []userRow{warmupRow})
	expectedRows := append(seedRows, warmupRow)
	s.waitForEqualRows(ctx, expectedRows, tableName)

	cdcRow := randomUserRow()
	s.insertRows(ctx, sourceConn, dialect, qualifiedName, []userRow{cdcRow})
	expectedRows = append(expectedRows, cdcRow)

	s.waitForEqualRows(ctx, expectedRows, tableName)

	// pause cdc flow
	_, err = s.env.APIClient.PauseCDCFlow(ctx, &gen.PauseCDCFlowRequest{
		Id: flow.CdcFlow.Id,
	})
	s.Require().NoError(err)
	// check that temporal workflow is cancelled
	s.Require().Eventually(
		func() bool {
			desc, err := s.env.TemporalClient.DescribeWorkflowExecution(ctx, s.lastWorkflowID, "")
			if err != nil {
				return false
			}
			return desc.WorkflowExecutionInfo.Status == enums.WORKFLOW_EXECUTION_STATUS_CANCELED
		},
		30*time.Second,
		1*time.Second,
		"workflow should be cancelled after pausing flow",
	)

	// add some data to source while flow is paused
	cdcRow2 := randomUserRow()
	s.insertRows(ctx, sourceConn, dialect, qualifiedName, []userRow{cdcRow2})
	expectedRows = append(expectedRows, cdcRow2)

	// resume flow
	_, err = s.env.APIClient.ResumeCDCFlow(ctx, &gen.ResumeCDCFlowRequest{
		Id: flow.CdcFlow.Id,
	})
	s.Require().NoError(err)

	// check that new data is replicated after resuming
	s.waitForEqualRows(ctx, expectedRows, tableName)
}

func (s *GRPCE2ESuite) TestCdcPostgres() {
	sourcePeerId := s.createPostgresPeer(s.ctx)
	s.testCdcFlow(s.ctx, sourcePeerId, s.env.PostgresSQL, postgresDialect, &gen.CdcFlowConfig_PostgresSource{
		PostgresSource: &gen.PostgresSourceConfig{},
	})
}

func (s *GRPCE2ESuite) TestCdcMySQL() {
	sourcePeerID := s.createMySQLPeer(s.ctx)
	s.testCdcFlow(s.ctx, sourcePeerID, s.env.MySQLDB, mysqlDialect(mysqlDatabase), &gen.CdcFlowConfig_MysqlSource{
		MysqlSource: &gen.MysqlSourceConfig{},
	})
}
