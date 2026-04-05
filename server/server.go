package server

import (
	"context"
	"fmt"
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

func (s *PeerdbServiceServer) CreatePeer(ctx context.Context, req *gen.CreatePeerRequest) (*gen.CreateResponse, error) {
	id, err := s.peers.CreatePeer(ctx, req.GetPeer())
	if err != nil {
		return nil, err
	}
	return &gen.CreateResponse{Id: id}, nil
}

func (s *PeerdbServiceServer) GetPeers(context.Context, *gen.GetPeersRequest) (*gen.GetPeersResponse, error) {
	return nil, fmt.Errorf("not implemented")
}

func (s *PeerdbServiceServer) CreateCDCFlow(ctx context.Context, req *gen.CreateCDCFlowRequest) (*gen.CreateResponse, error) {
	id, err := s.flows.CreateFlow(ctx, req.GetCdcFlow())
	if err != nil {
		return nil, err
	}

	_, err = s.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        fmt.Sprintf("%s%s", CdcFlowPrefix, id),
		TaskQueue: s.cdcTaskQueue,
	}, workflows.CdcFlowWorkflow, workflows.CdcFlowWorkflowInput{FlowId: id})
	if err != nil {
		return nil, fmt.Errorf("failed to start cdc workflow: %w", err)
	}

	return &gen.CreateResponse{Id: id}, nil
}

func (s *PeerdbServiceServer) GetCDCFlows(context.Context, *gen.GetCDCFlowsRequest) (*gen.GetCDCFlowsResponse, error) {
	return nil, fmt.Errorf("not implemented")
}
