package workflows

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"peerdb-playground/config"
	"peerdb-playground/connectors"
	"peerdb-playground/gen"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
)

type CdcStreamActivityInput struct {
	FlowId string
	Tables []*gen.TableSchema
}

// cdcStreamConfig holds resolved CDC stream parameters for a single run.
type cdcStreamConfig struct {
	flushInterval     time.Duration
	maxBatchSize      int
	heartbeatInterval time.Duration
	recordHeartbeat   func(ctx context.Context, details ...any)
}

// resolveCdcStreamConfig builds a cdcStreamConfig by applying per-flow overrides
// on top of server-level defaults.
func resolveCdcStreamConfig(defaults config.CdcConfig, flowCfg *gen.CdcFlowConfig) cdcStreamConfig {
	cfg := cdcStreamConfig{
		flushInterval:     time.Duration(defaults.FlushIntervalMs) * time.Millisecond,
		maxBatchSize:      defaults.MaxBatchSize,
		heartbeatInterval: time.Duration(defaults.HeartbeatIntervalMs) * time.Millisecond,
	}
	if flowCfg.FlushIntervalMs != nil && *flowCfg.FlushIntervalMs > 0 {
		cfg.flushInterval = time.Duration(*flowCfg.FlushIntervalMs) * time.Millisecond
	}
	if flowCfg.MaxBatchSize != nil && *flowCfg.MaxBatchSize > 0 {
		cfg.maxBatchSize = int(*flowCfg.MaxBatchSize)
	}
	if flowCfg.HeartbeatIntervalMs != nil && *flowCfg.HeartbeatIntervalMs > 0 {
		cfg.heartbeatInterval = time.Duration(*flowCfg.HeartbeatIntervalMs) * time.Millisecond
	}
	return cfg
}

func (a *Activities) CdcStreamActivity(ctx context.Context, input CdcStreamActivityInput) error {
	flow, err := a.flowsSvc.GetFlow(ctx, input.FlowId)
	if err != nil {
		return err
	}
	source, err := a.peersSvc.GetPeer(ctx, flow.Source)
	if err != nil {
		return err
	}
	dest, err := a.peersSvc.GetPeer(ctx, flow.Destination)
	if err != nil {
		return err
	}

	logger := slog.With("flowId", input.FlowId)
	srcConn, err := newSourceConnector(ctx, input.FlowId, source, flow.GetConfig(), logger)
	if err != nil {
		return fmt.Errorf("failed to create source connector: %w", err)
	}
	defer srcConn.Close(ctx)

	destConn, err := newDestinationConnector(ctx, dest, flow.GetConfig(), logger)
	if err != nil {
		return fmt.Errorf("failed to create destination connector: %w", err)
	}
	defer destConn.Close(ctx)

	streamCfg := resolveCdcStreamConfig(a.cdcCfg, flow.GetConfig())
	streamCfg.recordHeartbeat = activity.RecordHeartbeat
	return runCdcStream(ctx, srcConn, destConn, streamCfg, logger)
}

func runCdcStream(
	ctx context.Context,
	srcConn connectors.SourceConnector,
	destConn connectors.DestinationConnector,
	cfg cdcStreamConfig,
	logger *slog.Logger,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	logger.Info("starting cdc stream activity",
		"flushInterval", cfg.flushInterval,
		"maxBatchSize", cfg.maxBatchSize,
		"heartbeatInterval", cfg.heartbeatInterval,
	)

	readCh := make(chan connectors.RecordBatch)
	readErrCh := make(chan error, 1)
	pending := cdcBatchAccumulator{}

	go func() {
		readErrCh <- srcConn.Read(ctx, readCh)
	}()

	heartbeatTicker := time.NewTicker(cfg.heartbeatInterval)
	defer heartbeatTicker.Stop()
	flushTicker := time.NewTicker(cfg.flushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
			cfg.recordHeartbeat(ctx, "cdc-stream-running")
		case <-flushTicker.C:
			// flush every interval seconds
			if err := flushCdcBatch(ctx, srcConn, destConn, cfg, &pending, logger); err != nil {
				return err
			}
		case batch, ok := <-readCh:
			if !ok {
				// flush on stream close
				if err := flushCdcBatch(ctx, srcConn, destConn, cfg, &pending, logger); err != nil {
					return err
				}
				readErr := <-readErrCh
				if readErr != nil {
					if srcConn.IsCriticalError(readErr) {
						return temporal.NewNonRetryableApplicationError(
							"critical source read failed",
							"critical_source_read",
							readErr,
						)
					}
					return fmt.Errorf("source read failed: %w", readErr)
				}
				logger.Info("cdc stream source closed")
				return nil
			}

			logger.Debug("buffering cdc batch", "batchId", batch.BatchId, "records", len(batch.Records))
			pending.add(batch)
			if pending.recordCount >= cfg.maxBatchSize {
				// flush if batch size exceeds threshold
				if err := flushCdcBatch(ctx, srcConn, destConn, cfg, &pending, logger); err != nil {
					return err
				}
			}
		}
	}
}

type cdcBatchAccumulator struct {
	highestBatchID string
	recordCount    int
	records        []connectors.Record
}

func (a *cdcBatchAccumulator) add(batch connectors.RecordBatch) {
	if batch.BatchId != "" {
		a.highestBatchID = batch.BatchId
	}
	a.recordCount += len(batch.Records)
	a.records = append(a.records, batch.Records...)
}

func (a *cdcBatchAccumulator) empty() bool {
	return a.recordCount == 0
}

func (a *cdcBatchAccumulator) drain() connectors.RecordBatch {
	batch := connectors.RecordBatch{
		BatchId: a.highestBatchID,
		Records: a.records,
	}
	a.highestBatchID = ""
	a.recordCount = 0
	a.records = nil
	return batch
}

func flushCdcBatch(
	ctx context.Context,
	srcConn connectors.SourceConnector,
	destConn connectors.DestinationConnector,
	cfg cdcStreamConfig,
	pending *cdcBatchAccumulator,
	logger *slog.Logger,
) error {
	if pending.empty() {
		return nil
	}

	batch := pending.drain()
	logger.Debug("flushing cdc batch", "batchId", batch.BatchId, "records", len(batch.Records))
	if err := destConn.WriteBatch(ctx, batch); err != nil {
		if isNonRetryableCdcError(err) {
			return temporal.NewNonRetryableApplicationError(
				"critical destination write failed",
				"critical_destination_write",
				err,
			)
		}
		return fmt.Errorf("destination write failed for batch %s: %w", batch.BatchId, err)
	}
	if batch.BatchId != "" {
		if err := srcConn.Ack(ctx, batch.BatchId); err != nil {
			return fmt.Errorf("failed to ack source batch %s: %w", batch.BatchId, err)
		}
	}
	cfg.recordHeartbeat(ctx, batch.BatchId)
	return nil
}

func isNonRetryableCdcError(err error) bool {
	var appErr *temporal.ApplicationError
	return errors.As(err, &appErr) && appErr.NonRetryable()
}
