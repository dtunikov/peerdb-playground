package workflows

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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

const (
	cdcStreamHeartbeatInterval = 15 * time.Second
)

var (
	recordActivityHeartbeat = activity.RecordHeartbeat
	cdcStreamFlushInterval  = 1 * time.Second
	cdcStreamMaxBatchSize   = 10_000
)

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

	return runCdcStream(ctx, srcConn, destConn, logger)
}

func runCdcStream(
	ctx context.Context,
	srcConn connectors.SourceConnector,
	destConn connectors.DestinationConnector,
	logger *slog.Logger,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	logger.Info("starting cdc stream activity")

	readCh := make(chan connectors.RecordBatch)
	readErrCh := make(chan error, 1)
	pending := cdcBatchAccumulator{}

	go func() {
		readErrCh <- srcConn.Read(ctx, readCh)
	}()

	heartbeatTicker := time.NewTicker(cdcStreamHeartbeatInterval)
	defer heartbeatTicker.Stop()
	flushTicker := time.NewTicker(cdcStreamFlushInterval)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heartbeatTicker.C:
			recordActivityHeartbeat(ctx, "cdc-stream-running")
		case <-flushTicker.C:
			if err := flushCdcBatch(ctx, srcConn, destConn, &pending, logger); err != nil {
				return err
			}
		case batch, ok := <-readCh:
			if !ok {
				if err := flushCdcBatch(ctx, srcConn, destConn, &pending, logger); err != nil {
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
			if pending.recordCount >= cdcStreamMaxBatchSize {
				if err := flushCdcBatch(ctx, srcConn, destConn, &pending, logger); err != nil {
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
	recordActivityHeartbeat(ctx, batch.BatchId)
	return nil
}

func isNonRetryableCdcError(err error) bool {
	var appErr *temporal.ApplicationError
	return errors.As(err, &appErr) && appErr.NonRetryable()
}
