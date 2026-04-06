package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"peerdb-playground/config"
	"peerdb-playground/internal/cdcbench"
	"peerdb-playground/internal/localenv"
)

func main() {
	if err := run(); err != nil {
		fmt.Println("Error:", err)
		os.Exit(1)
	}
}

func run() error {
	source := flag.String("source", string(cdcbench.SourceBoth), "benchmark source: postgres|mysql|both")
	scenario := flag.String("scenario", string(cdcbench.ScenarioSteady), "benchmark scenario: steady|ramp|burst")
	output := flag.String("output", "benchmarks", "output directory for JSON and Markdown artifacts")
	flushIntervalMs := flag.Int("flush-interval-ms", 0, "optional per-flow CDC flush interval override in milliseconds")
	maxBatchSize := flag.Int("max-batch-size", 0, "optional per-flow CDC max batch size override")
	rate := flag.Int("rate", 0, "optional scenario base rate override")
	duration := flag.Duration("duration", 0, "optional scenario duration override")
	pollInterval := flag.Duration("poll-interval", 10*time.Second, "destination polling interval")
	flag.Parse()

	ctx := context.Background()
	env, err := localenv.Start(ctx, localenv.Options{
		TaskQueue:     "cdcbench",
		EncryptionKey: localenv.DefaultEncryptionKey,
		CdcConfig: config.CdcConfig{
			FlushIntervalMs:     1000,
			MaxBatchSize:        10000,
			HeartbeatIntervalMs: 15000,
		},
	})
	if err != nil {
		return err
	}
	defer env.Close(context.Background())

	results, err := cdcbench.Run(ctx, env, cdcbench.Config{
		Source:          cdcbench.SourceKind(*source),
		Scenario:        cdcbench.ScenarioKind(*scenario),
		OutputDir:       *output,
		FlushIntervalMs: *flushIntervalMs,
		MaxBatchSize:    *maxBatchSize,
		RateOverride:    *rate,
		Duration:        *duration,
		PollInterval:    *pollInterval,
	})
	if err != nil {
		return err
	}

	for _, result := range results {
		fmt.Printf(
			"%s/%s target=%.1f achieved=%.1f visible=%.1f p95=%.1fms max_backlog=%d output=%s\n",
			result.Source,
			result.Scenario,
			result.TargetWriteRate,
			result.AchievedWriteRate,
			result.VisibleIngestRate,
			result.P95LatencyMs,
			result.MaxBacklog,
			*output,
		)
	}

	return nil
}
