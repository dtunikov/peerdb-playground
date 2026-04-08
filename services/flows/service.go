package flows

import (
	"context"
	"errors"
	"fmt"
	"peerdb-playground/errs"
	"peerdb-playground/gen"
	"peerdb-playground/pkg/mysql"
	"peerdb-playground/pkg/postgres"
	"peerdb-playground/services/peers"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

const (
	cdcFlowTable          = "cdc_flows"
	sourceCheckpointTable = "cdc_flow_source_checkpoints"
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
		if src.Type != gen.PeerType_PEER_TYPE_POSTGRES {
			return errs.BadRequest.WithMessage("cdc flow source config is for postgres, but source peer is not postgres")
		}

		srcConn, err := postgres.ConnectFromProto(ctx, src.GetPostgresConfig())
		if err != nil {
			return errs.BadRequest.WithMessage("failed to connect to source peer").WithDetail(err)
		}
		defer srcConn.Close(ctx) //nolint:errcheck

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
	case *gen.CdcFlowConfig_MysqlSource:
		if src.Type != gen.PeerType_PEER_TYPE_MYSQL {
			return errs.BadRequest.WithMessage("cdc flow source config is for mysql, but source peer is not mysql")
		}

		srcConn, err := mysql.ConnectFromProto(ctx, src.GetMysqlConfig())
		if err != nil {
			return errs.BadRequest.WithMessage("failed to connect to source peer").WithDetail(err)
		}
		defer srcConn.Close() //nolint:errcheck

		if err := mysql.ValidateCDCPrerequisites(ctx, srcConn); err != nil {
			return errs.BadRequest.WithMessage("mysql source is not configured for cdc").WithDetail(err)
		}
	default:
		return errs.BadRequest.WithMessage("unsupported source config type")
	}

	return nil
}

func (s *Service) CreateFlow(ctx context.Context, flow *gen.CDCFlow) (*gen.CDCFlow, error) {
	if err := s.ValidateFlow(ctx, flow); err != nil {
		return nil, err
	}

	configJSON, err := proto.Marshal(flow.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal cdc flow config: %w", err)
	}

	sql, args, err := postgres.Sql.
		Insert(cdcFlowTable).
		Columns("name", "source", "destination", "config", "internal_version").
		Values(flow.Name, flow.Source, flow.Destination, configJSON, LatestInternalVersion).
		Suffix("RETURNING id").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	err = s.pg.QueryRow(ctx, sql, args...).Scan(&flow.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return flow, nil
}

func (s *Service) GetFlow(ctx context.Context, id string) (*gen.CDCFlow, error) {
	sql, args, err := postgres.Sql.
		Select("id", "name", "source", "destination", "config", "status").
		From(cdcFlowTable).
		Where("id = ?", id).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	var flow gen.CDCFlow
	var configBytes []byte
	err = s.pg.QueryRow(ctx, sql, args...).Scan(&flow.Id, &flow.Name, &flow.Source, &flow.Destination, &configBytes, &flow.Status)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	flow.Config = &gen.CdcFlowConfig{}
	if err := proto.Unmarshal(configBytes, flow.Config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal flow config: %w", err)
	}

	return &flow, nil
}

func (s *Service) GetSourceCheckpoint(ctx context.Context, flowID string) (string, error) {
	sql, args, err := postgres.Sql.
		Select("checkpoint").
		From(sourceCheckpointTable).
		Where("flow_id = ?", flowID).
		ToSql()
	if err != nil {
		return "", fmt.Errorf("failed to build query: %w", err)
	}

	var checkpoint string
	err = s.pg.QueryRow(ctx, sql, args...).Scan(&checkpoint)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("failed to execute query: %w", err)
	}

	return checkpoint, nil
}

func (s *Service) SaveSourceCheckpoint(ctx context.Context, flowID, checkpoint string) error {
	if checkpoint == "" {
		return nil
	}

	sql, args, err := postgres.Sql.
		Insert(sourceCheckpointTable).
		Columns("flow_id", "checkpoint").
		Values(flowID, checkpoint).
		Suffix("ON CONFLICT (flow_id) DO UPDATE SET checkpoint = EXCLUDED.checkpoint, updated_at = now()").
		ToSql()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}

	if _, err := s.pg.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	return nil
}

func (s *Service) UpdateFlowStatus(ctx context.Context, flowID string, status gen.CdcFlowStatus) error {
	sql, args, err := postgres.Sql.
		Update(cdcFlowTable).
		Set("status", status).
		Set("updated_at", sq.Expr("now()")).
		Where("id = ?", flowID).
		ToSql()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}

	if _, err := s.pg.Exec(ctx, sql, args...); err != nil {
		return fmt.Errorf("failed to execute query: %w", err)
	}

	return nil
}
