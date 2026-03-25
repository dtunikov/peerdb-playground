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
	srcConn, err := NewSourceConnector(ctx, input.FlowId, source, flow.GetConfig(), logger)
	if err != nil {
		return fmt.Errorf("failed to create source connector: %w", err)
	}
	defer srcConn.Close(ctx)

	destConn, err := NewDestinationConnector(ctx, dest, flow.GetConfig(), logger)
	if err != nil {
		return fmt.Errorf("failed to create destination connector: %w", err)
	}
	defer destConn.Close(ctx)

	if idx := slices.IndexFunc(flow.GetConfig().TableMappings, func(tm *gen.TableMapping) bool {
		return tm.Source == input.Table.GetName() && len(tm.ExcludeColumns) > 0
	}); idx >= 0 {
		tm := flow.GetConfig().TableMappings[idx]
		input.Table.Columns = slices.DeleteFunc(input.Table.GetColumns(), func(c *gen.ColumnSchema) bool {
			return slices.Contains(tm.ExcludeColumns, c.GetName())
		})
	}

	logger.Info("Starting snapshot")
	ch, err := srcConn.SnapshotTable(ctx, connectors.TableSchemaFromProto(input.Table))
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
