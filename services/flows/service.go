package flows

import (
	"context"
	"fmt"
	"peerdb-playground/errs"
	"peerdb-playground/gen"
	"peerdb-playground/pkg/postgres"
	"peerdb-playground/services/peers"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

const (
	cdcFlowTable = "cdc_flows"
)

const (
	InitialVersion = iota
	// Add new versions above this line.
	LatestInternalVersion
)

type Service struct {
	pg    *pgxpool.Pool
	peers *peers.Service
}

func NewService(pg *pgxpool.Pool, peers *peers.Service) *Service {
	return &Service{pg: pg, peers: peers}
}

func (s *Service) ValidateFlow(ctx context.Context, flow *gen.CDCFlow) error {
	if flow.Name == "" {
		return errs.BadRequest.WithMessage("name can't be empty")
	}
	if flow.Source == "" {
		return errs.BadRequest.WithMessage("source peer id can't be empty")
	}
	if flow.Destination == "" {
		return errs.BadRequest.WithMessage("destination peer id can't be empty")
	}
	if flow.Source == flow.Destination {
		return errs.BadRequest.WithMessage("source and destination peers must be different")
	}

	var src *gen.Peer
	var err error
	if src, err = s.peers.GetPeer(ctx, flow.Source); err != nil {
		return errs.BadRequest.WithMessage("source peer not found").WithDetail(err)
	}
	if _, err = s.peers.GetPeer(ctx, flow.Destination); err != nil {
		return errs.BadRequest.WithMessage("destination peer not found").WithDetail(err)
	}

	for _, tm := range flow.Config.TableMappings {
		if tm.Source == "" {
			return errs.BadRequest.WithMessage("table mapping source can't be empty")
		}
		if tm.Destination == "" {
			return errs.BadRequest.WithMessage("table mapping destination can't be empty")
		}
	}

	switch cfg := flow.Config.GetSourceConfig().(type) {
	case *gen.CdcFlowConfig_PostgresSource:
		if src.Type != gen.PeerType_POSTGRES {
			return errs.BadRequest.WithMessage("cdc flow source config is for postgres, but source peer is not postgres")
		}

		srcConn, err := postgres.ConnectFromProto(ctx, src.GetPostgresConfig())
		if err != nil {
			return errs.BadRequest.WithMessage("failed to connect to source peer").WithDetail(err)
		}
		defer srcConn.Close(ctx)

		if cfg.PostgresSource.PublicationName != "" {
			// check if it exists
			exists, err := postgres.PublicationExists(ctx, srcConn, cfg.PostgresSource.PublicationName)
			if err != nil {
				return errs.Internal.WithMessage("failed to check publication existence").WithDetail(err)
			}
			if !exists {
				return errs.BadRequest.WithMessage(fmt.Sprintf("publication %s does not exist on source peer", cfg.PostgresSource.PublicationName))
			}
		}
	default:
		return errs.BadRequest.WithMessage("unsupported source config type")
	}

	return nil
}

func (s *Service) CreateFlow(ctx context.Context, flow *gen.CDCFlow) (string, error) {
	if err := s.ValidateFlow(ctx, flow); err != nil {
		return "", err
	}

	configJSON, err := proto.Marshal(flow.Config)
	if err != nil {
		return "", fmt.Errorf("failed to marshal cdc flow config: %w", err)
	}

	sql, args, err := postgres.Sql.
		Insert(cdcFlowTable).
		Columns("name", "source", "destination", "config", "internal_version").
		Values(flow.Name, flow.Source, flow.Destination, configJSON, LatestInternalVersion).
		Suffix("RETURNING id").
		ToSql()
	if err != nil {
		return "", fmt.Errorf("failed to build query: %w", err)
	}

	var id string
	err = s.pg.QueryRow(ctx, sql, args...).Scan(&id)
	if err != nil {
		return "", fmt.Errorf("failed to execute query: %w", err)
	}

	return id, nil
}

func (s *Service) GetFlow(ctx context.Context, id string) (*gen.CDCFlow, error) {
	sql, args, err := postgres.Sql.
		Select("id", "name", "source", "destination", "config").
		From(cdcFlowTable).
		Where("id = ?", id).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	var flow gen.CDCFlow
	var configBytes []byte
	err = s.pg.QueryRow(ctx, sql, args...).Scan(&flow.Id, &flow.Name, &flow.Source, &flow.Destination, &configBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	flow.Config = &gen.CdcFlowConfig{}
	if err := proto.Unmarshal(configBytes, flow.Config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal flow config: %w", err)
	}

	return &flow, nil
}
