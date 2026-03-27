package workflows

import (
	"context"
	"fmt"
	"log/slog"
	"peerdb-playground/connectors"
	ch "peerdb-playground/connectors/clickhouse"
	pg "peerdb-playground/connectors/postgres"
	"peerdb-playground/gen"
	"peerdb-playground/pkg/clickhouse"
	"peerdb-playground/pkg/postgres"
)

func newSourceConnector(ctx context.Context, flowId string, peer *gen.Peer, flowCfg *gen.CdcFlowConfig, logger *slog.Logger) (connectors.SourceConnector, error) {
	switch peer.Type {
	case gen.PeerType_POSTGRES:
		return pg.NewConnector(ctx, flowId, postgres.ConfigFromProto(peer.GetPostgresConfig()), logger, flowCfg.GetTables(),
			flowCfg.GetPostgresSource().GetPublicationName())
	default:
		return nil, fmt.Errorf("unsupported source peer type: %s", peer.Type)
	}
}

func newDestinationConnector(ctx context.Context, peer *gen.Peer, flowCfg *gen.CdcFlowConfig, logger *slog.Logger) (connectors.DestinationConnector, error) {
	tableMappings := make(connectors.TableMappings)
	for _, tm := range flowCfg.TableMappings {
		tableMappings[tm.Source] = struct {
			DestTableName  string
			ExcludeColumns []string
		}{
			DestTableName:  tm.Destination,
			ExcludeColumns: tm.ExcludeColumns,
		}
	}
	switch peer.Type {
	case gen.PeerType_CLICKHOUSE:
		return ch.NewConnector(ctx, clickhouse.ConfigFromProto(peer.GetClickhouseConfig()), logger, tableMappings)
	default:
		return nil, fmt.Errorf("unsupported destination peer type: %s", peer.Type)
	}
}
