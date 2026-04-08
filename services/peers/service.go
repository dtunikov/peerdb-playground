package peers

import (
	"context"
	"encoding/hex"
	"fmt"
	"peerdb-playground/errs"
	"peerdb-playground/gen"
	"peerdb-playground/pkg/clickhouse"
	"peerdb-playground/pkg/crypto"
	"peerdb-playground/pkg/mysql"
	"peerdb-playground/pkg/postgres"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

type Service struct {
	pg            *pgxpool.Pool
	encryptionKey []byte
}

func NewService(pg *pgxpool.Pool, encryptionKey string) (*Service, error) {
	key, err := hex.DecodeString(encryptionKey)
	if err != nil {
		return nil, fmt.Errorf("invalid encryption key: must be hex-encoded: %w", err)
	}
	switch len(key) {
	case 16, 24, 32:
	default:
		return nil, fmt.Errorf("invalid encryption key length %d: must be 16, 24, or 32 bytes", len(key))
	}
	return &Service{
		pg:            pg,
		encryptionKey: key,
	}, nil
}

func (s *Service) encryptConfig(peer *gen.Peer) ([]byte, error) {
	var configMsg proto.Message
	switch peer.Config.(type) {
	case *gen.Peer_PostgresConfig:
		configMsg = peer.GetPostgresConfig()
	case *gen.Peer_ClickhouseConfig:
		configMsg = peer.GetClickhouseConfig()
	case *gen.Peer_MysqlConfig:
		configMsg = peer.GetMysqlConfig()
	default:
		return nil, fmt.Errorf("unknown peer config type")
	}

	plaintext, err := proto.Marshal(configMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config: %w", err)
	}

	encrypted, err := crypto.Encrypt(s.encryptionKey, plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt config: %w", err)
	}

	return encrypted, nil
}

func (s *Service) ValidatePeer(ctx context.Context, peer *gen.Peer) error {
	if peer.Name == "" {
		return errs.BadRequest.WithMessage("name can't be empty")
	}
	if peer.Type == gen.PeerType_PEER_TYPE_UNSPECIFIED {
		return errs.BadRequest.WithMessage("type must be specified")
	}
	if peer.Config == nil {
		return errs.BadRequest.WithMessage("config must be specified")
	}

	if err := validatePeerTypeMatchesConfig(peer); err != nil {
		return err
	}

	switch peer.Config.(type) {
	case *gen.Peer_PostgresConfig:
		cfg := peer.GetPostgresConfig()
		conn, err := postgres.ConnectFromProto(ctx, cfg)
		if err != nil {
			return errs.BadRequest.WithMessage("failed to connect to postgres").WithDetail(err)
		}
		defer conn.Close(ctx) //nolint:errcheck
	case *gen.Peer_ClickhouseConfig:
		cfg := peer.GetClickhouseConfig()
		conn, err := clickhouse.ConnectFromProto(ctx, cfg)
		if err != nil {
			return errs.BadRequest.WithMessage("failed to connect to clickhouse").WithDetail(err)
		}
		defer conn.Close() //nolint:errcheck
	case *gen.Peer_MysqlConfig:
		cfg := peer.GetMysqlConfig()
		conn, err := mysql.ConnectFromProto(ctx, cfg)
		if err != nil {
			return errs.BadRequest.WithMessage("failed to connect to mysql").WithDetail(err)
		}
		defer conn.Close() //nolint:errcheck
	default:
		return errs.BadRequest.WithMessage("unknown peer config type")
	}

	return nil
}

func (s *Service) CreatePeer(ctx context.Context, peer *gen.Peer) (*gen.Peer, error) {
	err := s.ValidatePeer(ctx, peer)
	if err != nil {
		return nil, err
	}

	encryptedConfig, err := s.encryptConfig(peer)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt peer config: %w", err)
	}

	sql, args, err := postgres.Sql.
		Insert("peers").
		Columns("name", "config", "type").
		Values(peer.Name, encryptedConfig, peer.Type).
		Suffix("RETURNING id").
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	err = s.pg.QueryRow(ctx, sql, args...).Scan(&peer.Id)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return peer, nil
}

func (s *Service) GetPeer(ctx context.Context, id string) (*gen.Peer, error) {
	sql, args, err := postgres.Sql.
		Select("id", "name", "type", "config").
		From("peers").
		Where("id = ?", id).
		ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build query: %w", err)
	}

	var peer gen.Peer
	var cfg []byte
	err = s.pg.QueryRow(ctx, sql, args...).Scan(&peer.Id, &peer.Name, &peer.Type, &cfg)
	if err != nil {
		return nil, errs.NotFound.WithMessage("peer not found").WithDetail(err)
	}

	// Decrypt config
	decrypted, err := crypto.Decrypt(s.encryptionKey, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt config: %w", err)
	}

	switch peer.Type {
	case gen.PeerType_PEER_TYPE_POSTGRES:
		var pgCfg gen.PostgresConfig
		err = proto.Unmarshal(decrypted, &pgCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal postgres config: %w", err)
		}
		peer.Config = &gen.Peer_PostgresConfig{PostgresConfig: &pgCfg}
	case gen.PeerType_PEER_TYPE_CLICKHOUSE:
		var chCfg gen.ClickhouseConfig
		err = proto.Unmarshal(decrypted, &chCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal clickhouse config: %w", err)
		}
		peer.Config = &gen.Peer_ClickhouseConfig{ClickhouseConfig: &chCfg}
	case gen.PeerType_PEER_TYPE_MYSQL:
		var mysqlCfg gen.MysqlConfig
		err = proto.Unmarshal(decrypted, &mysqlCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal mysql config: %w", err)
		}
		peer.Config = &gen.Peer_MysqlConfig{MysqlConfig: &mysqlCfg}
	default:
		return nil, fmt.Errorf("unknown peer type")
	}

	return &peer, nil
}

func validatePeerTypeMatchesConfig(peer *gen.Peer) error {
	var configType gen.PeerType
	switch peer.Config.(type) {
	case *gen.Peer_PostgresConfig:
		configType = gen.PeerType_PEER_TYPE_POSTGRES
	case *gen.Peer_ClickhouseConfig:
		configType = gen.PeerType_PEER_TYPE_CLICKHOUSE
	case *gen.Peer_MysqlConfig:
		configType = gen.PeerType_PEER_TYPE_MYSQL
	default:
		return errs.BadRequest.WithMessage("unknown peer config type")
	}

	if peer.Type != configType {
		return errs.BadRequest.WithMessage(fmt.Sprintf("peer type %s does not match config type %s", peer.Type, configType))
	}

	return nil
}
