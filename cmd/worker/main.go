package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"peerdb-playground/config"
	"peerdb-playground/services/flows"
	"peerdb-playground/services/peers"
	"peerdb-playground/workflows"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

var (
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
