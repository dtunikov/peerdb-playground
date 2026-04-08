package workflows

import (
	"context"
	"fmt"
	"peerdb-playground/gen"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type UpdateFlowStatusActivityInput struct {
	FlowId string
	Status gen.CdcFlowStatus
}

func defaultActivityCtx(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	})
}

func (a *Activities) UpdateFlowStatusActivity(ctx context.Context, input UpdateFlowStatusActivityInput) error {
	err := a.flowsSvc.UpdateFlowStatus(ctx, input.FlowId, input.Status)
	if err != nil {
		return fmt.Errorf("failed to update flow status to SNAPSHOT: %w", err)
	}

	return nil
}

func (a *Activities) GetFlow(ctx context.Context, flowId string) (*gen.CDCFlow, error) {
	flow, err := a.flowsSvc.GetFlow(ctx, flowId)
	if err != nil {
		return nil, fmt.Errorf("failed to get flow: %w", err)
	}

	return flow, nil
}
