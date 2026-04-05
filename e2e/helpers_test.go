package e2e

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"peerdb-playground/config"
	"peerdb-playground/gen"
	"peerdb-playground/gen/genconnect"
	"peerdb-playground/middleware"
	chpkg "peerdb-playground/pkg/clickhouse"
	mysqlpkg "peerdb-playground/pkg/mysql"
	pgpkg "peerdb-playground/pkg/postgres"
	"peerdb-playground/server"
	"peerdb-playground/services/flows"
	"peerdb-playground/services/peers"
	"peerdb-playground/workflows"

	"connectrpc.com/connect"
	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/testcontainers/testcontainers-go"
	tcpg "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
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
	sourcePeer, err := s.apiClient.CreatePeer(ctx, &gen.CreatePeerRequest{
		Peer: &gen.Peer{
			Name: fmt.Sprintf("pg-source-%s", rand.Text()),
			Type: gen.PeerType_POSTGRES,
			Config: &gen.Peer_PostgresConfig{
				PostgresConfig: &gen.PostgresConfig{
					Host:     s.postgresHost,
					Port:     s.postgresPort,
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
	sourcePeer, err := s.apiClient.CreatePeer(ctx, &gen.CreatePeerRequest{
		Peer: &gen.Peer{
			Name: fmt.Sprintf("mysql-source-%s", rand.Text()),
			Type: gen.PeerType_MYSQL,
			Config: &gen.Peer_MysqlConfig{
				MysqlConfig: &gen.MysqlConfig{
					Host:     s.mysqlHost,
					Port:     s.mysqlPort,
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
	destPeer, err := s.apiClient.CreatePeer(ctx, &gen.CreatePeerRequest{
		Peer: &gen.Peer{
			Name: fmt.Sprintf("destination-clickhouse-%s", rand.Text()),
			Type: gen.PeerType_CLICKHOUSE,
			Config: &gen.Peer_ClickhouseConfig{
				ClickhouseConfig: &gen.ClickhouseConfig{
					Host:     s.clickhouseHost,
					Port:     s.clickhousePort,
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
		`CREATE TABLE IF NOT EXISTS %s (id BIGINT PRIMARY KEY, name TEXT NOT NULL)`,
		qualifiedName,
	))
	s.Require().NoError(err)

	seedRows = []userRow{
		{ID: 1, Name: "alice"},
		{ID: 2, Name: "bob"},
	}

	insert := sq.StatementBuilder.
		PlaceholderFormat(dialect.PlaceholderFormat).
		Insert(qualifiedName).
		Columns("id", "name")
	for _, r := range seedRows {
		insert = insert.Values(r.ID, r.Name)
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

func (s *GRPCE2ESuite) startPostgres() {
	pg, err := tcpg.Run(
		s.ctx,
		"postgres:17-alpine",
		tcpg.WithDatabase(postgresDB),
		tcpg.WithUsername(postgresUser),
		tcpg.WithPassword(postgresPassword),
		tcpg.BasicWaitStrategies(),
		testcontainers.WithCmdArgs(
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=10",
			"-c", "max_wal_senders=10",
		),
	)
	s.Require().NoError(err)
	s.pgContainer = pg
	s.T().Cleanup(func() {
		s.Require().NoError(pg.Terminate(context.Background()))
	})

	host, err := pg.Host(s.ctx)
	s.Require().NoError(err)
	s.postgresHost = host

	port, err := pg.MappedPort(s.ctx, "5432/tcp")
	s.Require().NoError(err)
	s.postgresPort = uint32(port.Int())

	connString, err := pg.ConnectionString(s.ctx, "sslmode=disable")
	s.Require().NoError(err)

	pool, err := pgxpool.New(s.ctx, connString)
	s.Require().NoError(err)
	s.pgPool = pool
	s.T().Cleanup(pool.Close)

	pgDB := stdlib.OpenDBFromPool(pool)
	s.Require().NoError(pgDB.PingContext(s.ctx))
	s.pgDB = pgDB
	s.T().Cleanup(func() {
		s.Require().NoError(pgDB.Close())
	})
}

func (s *GRPCE2ESuite) startMySQL() {
	mysqlContainer, err := testcontainers.Run(
		s.ctx,
		"mysql:8.4",
		testcontainers.WithExposedPorts("3306/tcp"),
		testcontainers.WithEnv(map[string]string{
			"MYSQL_DATABASE":      mysqlDatabase,
			"MYSQL_ROOT_PASSWORD": mysqlPassword,
			"MYSQL_ROOT_HOST":     "%",
		}),
		testcontainers.WithCmdArgs(
			"--server-id=1",
			"--log-bin=mysql-bin",
			"--binlog-format=ROW",
			"--binlog-row-image=FULL",
			"--gtid-mode=ON",
			"--enforce-gtid-consistency=ON",
		),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("3306/tcp").WithStartupTimeout(2*time.Minute)),
	)
	s.Require().NoError(err)
	s.mysqlContainer = mysqlContainer
	s.T().Cleanup(func() {
		s.Require().NoError(mysqlContainer.Terminate(context.Background()))
	})

	host, err := mysqlContainer.Host(s.ctx)
	s.Require().NoError(err)
	s.mysqlHost = host

	port, err := mysqlContainer.MappedPort(s.ctx, "3306/tcp")
	s.Require().NoError(err)
	s.mysqlPort = uint32(port.Int())

	s.Require().Eventually(func() bool {
		db, err := mysqlpkg.Connect(s.ctx, mysqlpkg.Config{
			Host:     s.mysqlHost,
			Port:     int(s.mysqlPort),
			User:     mysqlUser,
			Password: mysqlPassword,
			Database: mysqlDatabase,
		})
		if err != nil {
			return false
		}
		s.mysqlDB = db
		return true
	}, 2*time.Minute, time.Second)
	s.T().Cleanup(func() {
		if s.mysqlDB != nil {
			s.Require().NoError(s.mysqlDB.Close())
		}
	})
}

func (s *GRPCE2ESuite) runMigrations() {
	migrationsPath := filepath.Join(s.projectRootPath, "migrations")
	connString, err := s.pgContainer.ConnectionString(s.ctx, "sslmode=disable")
	s.Require().NoError(err)
	s.Require().NoError(pgpkg.RunMigrations(connString, migrationsPath))
}

func (s *GRPCE2ESuite) startClickHouse() {
	exposedPort := "9000/tcp"
	ch, err := testcontainers.Run(
		s.ctx,
		"clickhouse/clickhouse-server:latest",
		testcontainers.WithExposedPorts(exposedPort),
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_USER":     clickhouseUser,
			"CLICKHOUSE_PASSWORD": clickhousePassword,
			"CLICKHOUSE_DB":       clickhouseDatabase,
		}),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("9000/tcp").WithStartupTimeout(2*time.Minute)),
	)
	s.Require().NoError(err)
	s.chContainer = ch
	s.T().Cleanup(func() {
		s.Require().NoError(ch.Terminate(context.Background()))
	})

	host, err := ch.Host(s.ctx)
	s.Require().NoError(err)
	s.clickhouseHost = host

	mappedPort, err := ch.MappedPort(s.ctx, "9000/tcp")
	s.Require().NoError(err)
	s.clickhousePort = uint32(mappedPort.Int())

	s.Require().Eventually(
		func() bool {
			conn, err := chpkg.Connect(s.ctx, chpkg.Config{
				Host:     s.clickhouseHost,
				Port:     int(s.clickhousePort),
				User:     clickhouseUser,
				Password: clickhousePassword,
				Database: clickhouseDatabase,
			})
			if err != nil {
				return false
			}
			s.chConn = conn
			return true
		},
		45*time.Second,
		1*time.Second,
	)
	s.T().Cleanup(func() {
		if s.chConn != nil {
			_ = s.chConn.Close()
		}
	})
}

func (s *GRPCE2ESuite) startTemporal() {
	temporalPath, err := exec.LookPath("temporal")
	if err != nil {
		s.T().Skip("temporal CLI is required for Temporal dev server tests")
	}

	devServer, err := testsuite.StartDevServer(s.ctx, testsuite.DevServerOptions{
		ExistingPath: temporalPath,
		ClientOptions: &client.Options{
			Namespace: "default",
		},
		LogLevel: "error",
		Stdout:   io.Discard,
		Stderr:   io.Discard,
	})
	s.Require().NoError(err)
	s.temporal = devServer
	s.T().Cleanup(func() {
		s.Require().NoError(devServer.Stop())
	})
	s.temporalClient = devServer.Client()
	s.T().Cleanup(s.temporalClient.Close)
}

func (s *GRPCE2ESuite) startWorkerAndAPI() {
	peersSvc, err := peers.NewService(s.pgPool, encryptionKey)
	s.Require().NoError(err)

	flowsSvc := flows.NewService(s.pgPool, peersSvc)

	activities := &workflows.Activities{}
	workflows.Init(activities, flowsSvc, peersSvc, config.CdcConfig{
		FlushIntervalMs:     100,
		MaxBatchSize:        100,
		HeartbeatIntervalMs: 250,
	})

	w := worker.New(s.temporalClient, taskQueue, worker.Options{})
	w.RegisterWorkflow(workflows.CdcFlowWorkflow)
	w.RegisterWorkflow(workflows.SnapshotWorkflow)
	w.RegisterActivity(activities)
	s.Require().NoError(w.Start())
	s.worker = w
	s.T().Cleanup(w.Stop)

	mux := http.NewServeMux()
	path, handler := genconnect.NewPeerdbServiceHandler(
		server.NewServer(peersSvc, flowsSvc, s.temporalClient, taskQueue),
		connect.WithInterceptors(
			middleware.RequestID(),
			middleware.LogRequest(),
			middleware.ErrorHandler(),
		),
	)
	mux.Handle(path, handler)

	api := httptest.NewUnstartedServer(mux)
	api.EnableHTTP2 = true
	api.StartTLS()
	s.api = api
	s.T().Cleanup(api.Close)

	s.apiClient = genconnect.NewPeerdbServiceClient(
		api.Client(),
		api.URL,
		connect.WithGRPC(),
	)
}

func (s *GRPCE2ESuite) loadUsersTableFromClickhouse(ctx context.Context, tableName string) ([]userRow, error) {
	rows, err := s.chConn.Query(ctx, fmt.Sprintf(`SELECT id, name FROM "%s" FINAL ORDER BY id`, tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []userRow
	for rows.Next() {
		var row userRow
		if err := rows.Scan(&row.ID, &row.Name); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return out, nil
}

func projectRoot() string {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		panic("failed to resolve project root")
	}
	return filepath.Dir(filepath.Dir(filename))
}
