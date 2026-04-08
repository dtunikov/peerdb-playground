package workflows

import (
	"fmt"
	"peerdb-playground/gen"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	maxParallelSnapshots = 10
)

type SnapshotWorkflowInput struct {
	FlowId string
	Tables []*gen.TableSchema
}

func SnapshotWorkflow(ctx workflow.Context, input SnapshotWorkflowInput) error {
	sCtx := defaultActivityCtx(ctx)
	err := workflow.ExecuteActivity(sCtx, activities.UpdateFlowStatusActivity, UpdateFlowStatusActivityInput{
		FlowId: input.FlowId,
		Status: gen.CdcFlowStatus_CDC_FLOW_STATUS_SNAPSHOT,
	}).Get(sCtx, nil)
	if err != nil {
		return fmt.Errorf("failed to update flow status to SNAPSHOT: %w", err)
	}

	snapshotTableCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 1 * time.Hour,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    1 * time.Minute,
			MaximumAttempts:    3,
		},
	})

	logger := workflow.GetLogger(ctx)
	logger.Info("Starting snapshot workflow", "flowId", input.FlowId, "tableCount", len(input.Tables))

	selector := workflow.NewSelector(ctx)
	var snapshotErr error
	running := 0

	for _, table := range input.Tables {
		logger.Info("Scheduling snapshot for table", "table", table.GetName())
		if snapshotErr != nil {
			break
		}
		// wait for a slot if we're at max parallelism
		for running >= maxParallelSnapshots {
			selector.Select(ctx)
			running--
		}

		future := workflow.ExecuteActivity(snapshotTableCtx, activities.SnapshotTableActivity, SnapshotTableActivityInput{
			FlowId: input.FlowId,
			Table:  table,
		})
		selector.AddFuture(future, func(f workflow.Future) {
			if err := f.Get(ctx, nil); err != nil {
				snapshotErr = fmt.Errorf("failed to snapshot table %q: %w", table.GetName(), err)
			}
		})
		running++
	}

	// wait for all in-flight snapshots to complete
	for running > 0 {
		selector.Select(ctx)
		running--
	}

	return snapshotErr
}
