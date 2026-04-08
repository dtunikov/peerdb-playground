package server

import (
	"context"
	"fmt"
	"peerdb-playground/errs"
	"peerdb-playground/gen"
	"peerdb-playground/services/flows"
	"peerdb-playground/services/peers"
	"peerdb-playground/workflows"

	"go.temporal.io/sdk/client"
)

const (
	CdcFlowPrefix = "cdc-flow-"
)

type PeerdbServiceServer struct {
	peers        *peers.Service
	flows        *flows.Service
	temporal     client.Client
	cdcTaskQueue string
}

func NewServer(peers *peers.Service, flows *flows.Service, temporal client.Client, cdcTaskQueue string) *PeerdbServiceServer {
	return &PeerdbServiceServer{
		peers:        peers,
		flows:        flows,
		temporal:     temporal,
		cdcTaskQueue: cdcTaskQueue,
	}
}

func (s *PeerdbServiceServer) CreatePeer(ctx context.Context, req *gen.CreatePeerRequest) (*gen.CreatePeerResponse, error) {
	peer, err := s.peers.CreatePeer(ctx, req.GetPeer())
	if err != nil {
		return nil, err
	}
	return &gen.CreatePeerResponse{Peer: peer}, nil
}

func (s *PeerdbServiceServer) GetPeers(context.Context, *gen.GetPeersRequest) (*gen.GetPeersResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func cdcFlowId(flowId string) string {
	return fmt.Sprintf("%s%s", CdcFlowPrefix, flowId)
}

func (s *PeerdbServiceServer) CreateCDCFlow(ctx context.Context, req *gen.CreateCDCFlowRequest) (*gen.CreateCDCFlowResponse, error) {
	f, err := s.flows.CreateFlow(ctx, req.GetCdcFlow())
	if err != nil {
		return nil, err
	}

	_, err = s.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cdcFlowId(f.Id),
		TaskQueue: s.cdcTaskQueue,
	}, workflows.CdcFlowWorkflow, workflows.CdcFlowWorkflowInput{FlowId: f.Id})
	if err != nil {
		return nil, fmt.Errorf("failed to start cdc workflow: %w", err)
	}

	return &gen.CreateCDCFlowResponse{CdcFlow: f}, nil
}

func (s *PeerdbServiceServer) GetCDCFlows(context.Context, *gen.GetCDCFlowsRequest) (*gen.GetCDCFlowsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *PeerdbServiceServer) PauseCDCFlow(ctx context.Context, req *gen.PauseCDCFlowRequest) (*gen.PauseCDCFlowResponse, error) {
	flow, err := s.flows.GetFlow(ctx, req.GetId())
	if err != nil {
		return nil, fmt.Errorf("failed to get flow: %w", err)
	}
	status := flow.GetStatus()
	if status < gen.CdcFlowStatus_CDC_FLOW_STATUS_SETUP || status > gen.CdcFlowStatus_CDC_FLOW_STATUS_CDC {
		return nil, errs.BadRequest.WithMessage(fmt.Sprintf("Can't pause flow in status: %v", gen.CdcFlowStatus_name[int32(status)]))
	}

	err = s.temporal.CancelWorkflow(ctx, cdcFlowId(req.GetId()), "")
	if err != nil {
		return nil, fmt.Errorf("failed to cancel cdc workflow: %w", err)
	}

	err = s.flows.UpdateFlowStatus(ctx, req.GetId(), gen.CdcFlowStatus_CDC_FLOW_STATUS_PAUSED)
	if err != nil {
		return nil, fmt.Errorf("failed to update flow status to PAUSED: %w", err)
	}

	return &gen.PauseCDCFlowResponse{}, nil
}

func (s *PeerdbServiceServer) ResumeCDCFlow(ctx context.Context, req *gen.ResumeCDCFlowRequest) (*gen.ResumeCDCFlowResponse, error) {
	f, err := s.flows.GetFlow(ctx, req.GetId())
	if err != nil {
		return nil, fmt.Errorf("failed to get flow: %w", err)
	}

	if f.GetStatus() != gen.CdcFlowStatus_CDC_FLOW_STATUS_PAUSED {
		return nil, errs.BadRequest.WithMessage("Flow must be in 'paused' status!")
	}

	_, err = s.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        cdcFlowId(f.Id),
		TaskQueue: s.cdcTaskQueue,
	}, workflows.CdcFlowWorkflow, workflows.CdcFlowWorkflowInput{FlowId: f.Id})
	if err != nil {
		return nil, fmt.Errorf("failed to start cdc workflow: %w", err)
	}

	return &gen.ResumeCDCFlowResponse{}, nil
}
