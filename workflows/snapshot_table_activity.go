package workflows

import (
	"context"
	"fmt"
	"log/slog"
	"peerdb-playground/connectors"
	"peerdb-playground/gen"
	"slices"
)

type SnapshotTableActivityInput struct {
	FlowId string
	Table  *gen.TableSchema
}

func (a *Activities) SnapshotTableActivity(ctx context.Context, input SnapshotTableActivityInput) error {
	// 1. create src and dst connectors
	flow, err := a.flowsSvc.GetFlow(ctx, input.FlowId)
	if err != nil {
		return fmt.Errorf("failed to get flow: %w", err)
	}
	source, err := a.peersSvc.GetPeer(ctx, flow.Source)
	if err != nil {
		return fmt.Errorf("failed to get source peer: %w", err)
	}
	dest, err := a.peersSvc.GetPeer(ctx, flow.Destination)
	if err != nil {
		return fmt.Errorf("failed to get destination peer: %w", err)
	}

	logger := slog.With("flowId", input.FlowId, "table", input.Table)
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

	table := applyColumnExclusions(input.Table, flow.GetConfig().TableMappings)
	return runSnapshot(ctx, srcConn, destConn, connectors.TableSchemaFromProto(table), logger)
}

func applyColumnExclusions(table *gen.TableSchema, mappings []*gen.TableMapping) *gen.TableSchema {
	idx := slices.IndexFunc(mappings, func(tm *gen.TableMapping) bool {
		return tm.Source == table.GetName() && len(tm.ExcludeColumns) > 0
	})
	if idx < 0 {
		return table
	}
	tm := mappings[idx]
	table.Columns = slices.DeleteFunc(table.GetColumns(), func(c *gen.ColumnSchema) bool {
		return slices.Contains(tm.ExcludeColumns, c.GetName())
	})
	return table
}

func runSnapshot(
	ctx context.Context,
	srcConn connectors.SourceConnector,
	destConn connectors.DestinationConnector,
	table connectors.TableSchema,
	logger *slog.Logger,
) error {
	logger.Info("Starting snapshot")
	ch, err := srcConn.SnapshotTable(ctx, table)
	if err != nil {
		return fmt.Errorf("failed to snapshot table from source connector: %w", err)
	}

	err = destConn.Write(ctx, ch)
	if err != nil {
		return fmt.Errorf("failed to write snapshot data to destination connector: %w", err)
	}
	logger.Info("Snapshot done!")

	return nil
}
