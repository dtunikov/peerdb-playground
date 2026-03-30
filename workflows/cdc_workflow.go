// CdcFlowWorkflow orchestrates the full CDC pipeline:
//
//	CdcFlowWorkflow (main, long-running)
//	│
//	├── 1. SetupActivity
//	│   ├── Create source connector → Setup (publication, slot)
//	│   ├── Get table schemas from source
//	│   └── Create dest connector → Setup (create tables)
//	│
//	├── 2. SnapshotWorkflow (child workflow)
//	│   │
//	│   ├── SnapshotTableActivity(table_1)  ─┐
//	│   ├── SnapshotTableActivity(table_2)  ─┤  bounded parallelism
//	│   ├── SnapshotTableActivity(table_3)  ─┘
//	│   └── ...
//	│   │
//	│   │   Each activity:
//	│   │     source.SnapshotTable(ctx, table) → yields batches
//	│   │     for batch := range batches {
//	│   │         dest.WriteBatch(ctx, batch)
//	│   │         heartbeat(progress)
//	│   │     }
//	│   │
//	│   └── Returns list of completed tables
//	│
//	├── 3. CdcStreamActivity (long-running with heartbeat)
//	│   │   source.Read(ctx, ch)
//	│   │   dest.Write(ctx, ch)
//	│   │   source.Ack(ctx, position)
//	│   └── Runs until cancelled/error
//	│
//	└── 4. TeardownActivity (on cancel/error)
//	    ├── Drop slot
//	    ├── Drop publication (if we created it)
//	    └── Close connections
package workflows

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

type CdcFlowWorkflowInput struct {
	FlowId string
}

func CdcFlowWorkflow(ctx workflow.Context, input CdcFlowWorkflowInput) error {
	setupCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    1 * time.Minute,
			MaximumAttempts:    3,
		},
	})
	cdcCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 365 * 24 * time.Hour,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    5 * time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    1 * time.Minute,
			MaximumAttempts:    0,
		},
	})

	var setupOutput *SetupActivityOutput
	err := workflow.ExecuteActivity(setupCtx, activities.SetupActivity, SetupActivityInput{FlowId: input.FlowId}).Get(setupCtx, &setupOutput)
	if err != nil {
		return fmt.Errorf("failed to setup connectors: %w", err)
	}

	err = workflow.ExecuteChildWorkflow(ctx, SnapshotWorkflow, SnapshotWorkflowInput{
		FlowId: input.FlowId,
		Tables: setupOutput.Tables,
	}).Get(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to execute snapshot workflow: %w", err)
	}

	err = workflow.ExecuteActivity(cdcCtx, activities.CdcStreamActivity, CdcStreamActivityInput{FlowId: input.FlowId}).
		Get(cdcCtx, nil)
	if err != nil {
		return fmt.Errorf("cdc activity failed: %w", err)
	}

	return nil
}
