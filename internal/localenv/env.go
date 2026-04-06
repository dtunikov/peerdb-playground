package localenv

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"peerdb-playground/config"
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
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
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
	DefaultEncryptionKey      = "0123456789abcdef0123456789abcdef"
	DefaultClickHouseDatabase = "destination"
	DefaultClickHouseUser     = "default"
	DefaultClickHousePassword = "clickhouse"
	DefaultTaskQueue          = "cdc-flow-e2e"
	DefaultPostgresUser       = "postgres"
	DefaultPostgresPassword   = "postgres"
	DefaultPostgresDB         = "peerdb"
	DefaultMySQLUser          = "root"
	DefaultMySQLPassword      = "mysql"
	DefaultMySQLDatabase      = "source"
)

type Options struct {
	ProjectRoot        string
	TaskQueue          string
	EncryptionKey      string
	CdcConfig          config.CdcConfig
	PostgresUser       string
	PostgresPassword   string
	PostgresDB         string
	MySQLUser          string
	MySQLPassword      string
	MySQLDatabase      string
	ClickHouseUser     string
	ClickHousePassword string
	ClickHouseDatabase string
}

type Environment struct {
	ProjectRootPath string
	TaskQueue       string
	EncryptionKey   string

	PostgresHost      string
	PostgresPort      uint32
	PostgresUser      string
	PostgresPassword  string
	PostgresDB        string
	PostgresPool      *pgxpool.Pool
	PostgresSQL       *sql.DB
	PostgresContainer *tcpg.PostgresContainer

	MySQLHost      string
	MySQLPort      uint32
	MySQLUser      string
	MySQLPassword  string
	MySQLDatabase  string
	MySQLDB        *sql.DB
	MySQLContainer testcontainers.Container

	ClickHouseHost      string
	ClickHousePort      uint32
	ClickHouseUser      string
	ClickHousePassword  string
	ClickHouseDatabase  string
	ClickHouseConn      driver.Conn
	ClickHouseContainer testcontainers.Container

	Temporal       *testsuite.DevServer
	TemporalClient client.Client
	Worker         worker.Worker

	API       *httptest.Server
	APIClient genconnect.PeerdbServiceClient
}

func Start(ctx context.Context, opts Options) (*Environment, error) {
	cfg := fillDefaults(opts)
	root, err := resolveProjectRoot(cfg.ProjectRoot)
	if err != nil {
		return nil, err
	}

	env := &Environment{
		ProjectRootPath:    root,
		TaskQueue:          cfg.TaskQueue,
		EncryptionKey:      cfg.EncryptionKey,
		PostgresUser:       cfg.PostgresUser,
		PostgresPassword:   cfg.PostgresPassword,
		PostgresDB:         cfg.PostgresDB,
		MySQLUser:          cfg.MySQLUser,
		MySQLPassword:      cfg.MySQLPassword,
		MySQLDatabase:      cfg.MySQLDatabase,
		ClickHouseUser:     cfg.ClickHouseUser,
		ClickHousePassword: cfg.ClickHousePassword,
		ClickHouseDatabase: cfg.ClickHouseDatabase,
	}

	if err := env.startPostgres(ctx); err != nil {
		env.Close(context.Background())
		return nil, err
	}
	if err := env.runMigrations(ctx); err != nil {
		env.Close(context.Background())
		return nil, err
	}
	if err := env.startMySQL(ctx); err != nil {
		env.Close(context.Background())
		return nil, err
	}
	if err := env.startClickHouse(ctx); err != nil {
		env.Close(context.Background())
		return nil, err
	}
	if err := env.startTemporal(ctx); err != nil {
		env.Close(context.Background())
		return nil, err
	}
	if err := env.startWorkerAndAPI(ctx, cfg.CdcConfig); err != nil {
		env.Close(context.Background())
		return nil, err
	}

	return env, nil
}

func (e *Environment) Close(ctx context.Context) error {
	var errs []error

	if e.API != nil {
		e.API.Close()
	}
	if e.Worker != nil {
		e.Worker.Stop()
	}
	if e.TemporalClient != nil {
		e.TemporalClient.Close()
	}
	if e.Temporal != nil {
		if err := e.Temporal.Stop(); err != nil {
			errs = append(errs, err)
		}
	}
	if e.ClickHouseConn != nil {
		if err := e.ClickHouseConn.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if e.ClickHouseContainer != nil {
		if err := e.ClickHouseContainer.Terminate(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if e.MySQLDB != nil {
		if err := e.MySQLDB.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if e.MySQLContainer != nil {
		if err := e.MySQLContainer.Terminate(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if e.PostgresSQL != nil {
		if err := e.PostgresSQL.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if e.PostgresPool != nil {
		e.PostgresPool.Close()
	}
	if e.PostgresContainer != nil {
		if err := e.PostgresContainer.Terminate(ctx); err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (e *Environment) startPostgres(ctx context.Context) error {
	pg, err := tcpg.Run(
		ctx,
		"postgres:17-alpine",
		tcpg.WithDatabase(e.PostgresDB),
		tcpg.WithUsername(e.PostgresUser),
		tcpg.WithPassword(e.PostgresPassword),
		tcpg.BasicWaitStrategies(),
		testcontainers.WithCmdArgs(
			"-c", "wal_level=logical",
			"-c", "max_replication_slots=10",
			"-c", "max_wal_senders=10",
		),
	)
	if err != nil {
		return err
	}
	e.PostgresContainer = pg

	host, err := pg.Host(ctx)
	if err != nil {
		return err
	}
	e.PostgresHost = host

	port, err := pg.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return err
	}
	e.PostgresPort = uint32(port.Int())

	connString, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return err
	}

	pool, err := pgxpool.New(ctx, connString)
	if err != nil {
		return err
	}
	e.PostgresPool = pool

	pgDB := stdlib.OpenDBFromPool(pool)
	if err := pgDB.PingContext(ctx); err != nil {
		return err
	}
	e.PostgresSQL = pgDB

	return nil
}

func (e *Environment) runMigrations(ctx context.Context) error {
	connString, err := e.PostgresContainer.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return err
	}
	migrationsPath := filepath.Join(e.ProjectRootPath, "migrations")
	return pgpkg.RunMigrations(connString, migrationsPath)
}

func (e *Environment) startMySQL(ctx context.Context) error {
	mysqlContainer, err := testcontainers.Run(
		ctx,
		"mysql:8.4",
		testcontainers.WithExposedPorts("3306/tcp"),
		testcontainers.WithEnv(map[string]string{
			"MYSQL_DATABASE":      e.MySQLDatabase,
			"MYSQL_ROOT_PASSWORD": e.MySQLPassword,
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
	if err != nil {
		return err
	}
	e.MySQLContainer = mysqlContainer

	host, err := mysqlContainer.Host(ctx)
	if err != nil {
		return err
	}
	e.MySQLHost = host

	port, err := mysqlContainer.MappedPort(ctx, "3306/tcp")
	if err != nil {
		return err
	}
	e.MySQLPort = uint32(port.Int())

	var db *sql.DB
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		db, err = mysqlpkg.Connect(ctx, mysqlpkg.Config{
			Host:     e.MySQLHost,
			Port:     int(e.MySQLPort),
			User:     e.MySQLUser,
			Password: e.MySQLPassword,
			Database: e.MySQLDatabase,
		})
		if err == nil {
			e.MySQLDB = db
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}

	return fmt.Errorf("mysql did not become ready: %w", err)
}

func (e *Environment) startClickHouse(ctx context.Context) error {
	ch, err := testcontainers.Run(
		ctx,
		"clickhouse/clickhouse-server:latest",
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithEnv(map[string]string{
			"CLICKHOUSE_USER":     e.ClickHouseUser,
			"CLICKHOUSE_PASSWORD": e.ClickHousePassword,
			"CLICKHOUSE_DB":       e.ClickHouseDatabase,
		}),
		testcontainers.WithWaitStrategy(wait.ForListeningPort("9000/tcp").WithStartupTimeout(2*time.Minute)),
	)
	if err != nil {
		return err
	}
	e.ClickHouseContainer = ch

	host, err := ch.Host(ctx)
	if err != nil {
		return err
	}
	e.ClickHouseHost = host

	mappedPort, err := ch.MappedPort(ctx, "9000/tcp")
	if err != nil {
		return err
	}
	e.ClickHousePort = uint32(mappedPort.Int())

	var conn driver.Conn
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		conn, err = chpkg.Connect(ctx, chpkg.Config{
			Host:     e.ClickHouseHost,
			Port:     int(e.ClickHousePort),
			User:     e.ClickHouseUser,
			Password: e.ClickHousePassword,
			Database: e.ClickHouseDatabase,
		})
		if err == nil {
			e.ClickHouseConn = conn
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}

	return fmt.Errorf("clickhouse did not become ready: %w", err)
}

func (e *Environment) startTemporal(ctx context.Context) error {
	temporalPath, err := exec.LookPath("temporal")
	if err != nil {
		return fmt.Errorf("temporal CLI is required: %w", err)
	}

	devServer, err := testsuite.StartDevServer(ctx, testsuite.DevServerOptions{
		ExistingPath: temporalPath,
		ClientOptions: &client.Options{
			Namespace: "default",
		},
		LogLevel: "error",
		Stdout:   io.Discard,
		Stderr:   io.Discard,
	})
	if err != nil {
		return err
	}
	e.Temporal = devServer
	e.TemporalClient = devServer.Client()

	return nil
}

func (e *Environment) startWorkerAndAPI(ctx context.Context, cdcCfg config.CdcConfig) error {
	_ = ctx
	peersSvc, err := peers.NewService(e.PostgresPool, e.EncryptionKey)
	if err != nil {
		return err
	}
	flowsSvc := flows.NewService(e.PostgresPool, peersSvc)

	activities := &workflows.Activities{}
	workflows.Init(activities, flowsSvc, peersSvc, cdcCfg)

	w := worker.New(e.TemporalClient, e.TaskQueue, worker.Options{})
	w.RegisterWorkflow(workflows.CdcFlowWorkflow)
	w.RegisterWorkflow(workflows.SnapshotWorkflow)
	w.RegisterActivity(activities)
	if err := w.Start(); err != nil {
		return err
	}
	e.Worker = w

	mux := http.NewServeMux()
	path, handler := genconnect.NewPeerdbServiceHandler(
		server.NewServer(peersSvc, flowsSvc, e.TemporalClient, e.TaskQueue),
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
	e.API = api
	e.APIClient = genconnect.NewPeerdbServiceClient(
		api.Client(),
		api.URL,
		connect.WithGRPC(),
	)

	return nil
}

func fillDefaults(opts Options) Options {
	if opts.TaskQueue == "" {
		opts.TaskQueue = DefaultTaskQueue
	}
	if opts.EncryptionKey == "" {
		opts.EncryptionKey = DefaultEncryptionKey
	}
	if opts.PostgresUser == "" {
		opts.PostgresUser = DefaultPostgresUser
	}
	if opts.PostgresPassword == "" {
		opts.PostgresPassword = DefaultPostgresPassword
	}
	if opts.PostgresDB == "" {
		opts.PostgresDB = DefaultPostgresDB
	}
	if opts.MySQLUser == "" {
		opts.MySQLUser = DefaultMySQLUser
	}
	if opts.MySQLPassword == "" {
		opts.MySQLPassword = DefaultMySQLPassword
	}
	if opts.MySQLDatabase == "" {
		opts.MySQLDatabase = DefaultMySQLDatabase
	}
	if opts.ClickHouseUser == "" {
		opts.ClickHouseUser = DefaultClickHouseUser
	}
	if opts.ClickHousePassword == "" {
		opts.ClickHousePassword = DefaultClickHousePassword
	}
	if opts.ClickHouseDatabase == "" {
		opts.ClickHouseDatabase = DefaultClickHouseDatabase
	}

	return opts
}

func resolveProjectRoot(explicit string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}

	wd, err := os.Getwd()
	if err != nil {
		return "", err
	}

	cur := wd
	for {
		if _, err := os.Stat(filepath.Join(cur, "go.mod")); err == nil {
			return cur, nil
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			break
		}
		cur = parent
	}

	return "", fmt.Errorf("could not locate project root from %s", wd)
}
