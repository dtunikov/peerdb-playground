package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"peerdb-playground/config"
	"peerdb-playground/gen/genconnect"
	"peerdb-playground/middleware"
	"peerdb-playground/pkg/postgres"
	"peerdb-playground/server"
	"peerdb-playground/services/flows"
	"peerdb-playground/services/peers"
	"peerdb-playground/workflows"

	"connectrpc.com/connect"
	"connectrpc.com/grpcreflect"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

var (
	mode    = flag.String("mode", "", "service to run: api or worker")
	cfgPath = flag.String("config", "config.yaml", "path to config file")
)

func main() {
	if err := run(); err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}

func run() error {
	flag.Parse()

	cfg, err := config.LoadConfig(*cfgPath)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(cfg.Log.Level)); err != nil {
		return fmt.Errorf("invalid log level %q: %w", cfg.Log.Level, err)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: lvl,
	}))
	slog.SetDefault(logger)

	switch *mode {
	case "api":
		return runAPI(cfg)
	case "worker":
		return runWorker(cfg)
	default:
		return fmt.Errorf("invalid -mode %q: must be \"api\" or \"worker\"", *mode)
	}
}

func runAPI(cfg *config.Config) error {
	slog.Info("Running migrations")
	if err := postgres.RunMigrations(cfg.Database.Url, "migrations"); err != nil {
		return fmt.Errorf("failed to run migrations: %w", err)
	}

	pg, err := pgxpool.New(context.Background(), cfg.Database.Url)
	if err != nil {
		return fmt.Errorf("failed to connect to pg: %w", err)
	}

	peersSvc, err := peers.NewService(pg, cfg.EncryptionKey)
	if err != nil {
		return fmt.Errorf("failed to create peers service: %w", err)
	}

	flowsSvc := flows.NewService(pg, peersSvc)

	tc, err := client.Dial(client.Options{
		HostPort: cfg.Temporal.HostPort,
	})
	if err != nil {
		return fmt.Errorf("failed to create temporal client: %w", err)
	}
	defer tc.Close()

	mux := http.NewServeMux()
	path, handler := genconnect.NewPeerdbServiceHandler(
		server.NewServer(peersSvc, flowsSvc, tc, cfg.Temporal.CdcTaskQueue),
		connect.WithInterceptors(
			middleware.RequestID(),
			middleware.LogRequest(),
			middleware.ErrorHandler(),
		),
	)
	mux.Handle(path, handler)

	reflector := grpcreflect.NewStaticReflector(
		"peerdb.PeerdbService",
	)
	mux.Handle(grpcreflect.NewHandlerV1(reflector))
	mux.Handle(grpcreflect.NewHandlerV1Alpha(reflector))

	slog.Info("Starting server", slog.Int("port", cfg.Server.Port))
	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Server.Port),
		Handler: h2c.NewHandler(mux, &http2.Server{}),
	}
	if err = srv.ListenAndServe(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	return nil
}

func runWorker(cfg *config.Config) error {
	pg, err := pgxpool.New(context.Background(), cfg.Database.Url)
	if err != nil {
		return fmt.Errorf("failed to connect to pg: %w", err)
	}
	defer pg.Close()

	peersSvc, err := peers.NewService(pg, cfg.EncryptionKey)
	if err != nil {
		return fmt.Errorf("failed to create peers service: %w", err)
	}

	flowsSvc := flows.NewService(pg, peersSvc)

	tc, err := client.Dial(client.Options{
		HostPort: cfg.Temporal.HostPort,
	})
	if err != nil {
		return fmt.Errorf("failed to create temporal client: %w", err)
	}
	defer tc.Close()

	w := worker.New(tc, cfg.Temporal.CdcTaskQueue, worker.Options{})

	activities := &workflows.Activities{}
	workflows.Init(activities, flowsSvc, peersSvc, cfg.Cdc)

	w.RegisterWorkflow(workflows.CdcFlowWorkflow)
	w.RegisterWorkflow(workflows.SnapshotWorkflow)
	w.RegisterActivity(activities)

	slog.Info("Starting temporal worker")
	if err := w.Run(worker.InterruptCh()); err != nil {
		return fmt.Errorf("failed to run worker: %w", err)
	}

	return nil
}
