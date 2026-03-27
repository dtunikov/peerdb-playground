package workflows

import (
	"context"
	"fmt"
	"log/slog"
	"peerdb-playground/connectors"
	"peerdb-playground/gen"
)

type SetupActivityInput struct {
	FlowId string
}

type SetupActivityOutput struct {
	Tables []*gen.TableSchema
}

func (a *Activities) SetupActivity(ctx context.Context, input SetupActivityInput) (*SetupActivityOutput, error) {
	flow, err := a.flowsSvc.GetFlow(ctx, input.FlowId)
	if err != nil {
		return nil, fmt.Errorf("failed to get flow: %w", err)
	}
	source, err := a.peersSvc.GetPeer(ctx, flow.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to get source peer: %w", err)
	}
	dest, err := a.peersSvc.GetPeer(ctx, flow.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to get destination peer: %w", err)
	}

	logger := slog.With("flowId", input.FlowId)
	logger.Info("setting up connectors for flow")

	srcConn, err := newSourceConnector(ctx, input.FlowId, source, flow.GetConfig(), logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create source connector: %w", err)
	}
	defer srcConn.Close(ctx)

	err = srcConn.Setup(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to setup source connector: %w", err)
	}

	destConn, err := newDestinationConnector(ctx, dest, flow.GetConfig(), logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create destination connector: %w", err)
	}
	defer destConn.Close(ctx)

	tables, err := srcConn.GetTableSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get table schemas from source connector: %w", err)
	}

	err = destConn.Setup(ctx, tables)
	if err != nil {
		return nil, fmt.Errorf("failed to setup destination connector: %w", err)
	}

	logger.Info("connectors setup complete")
	return &SetupActivityOutput{
		Tables: connectors.TableSchemasToProto(tables),
	}, nil
}
