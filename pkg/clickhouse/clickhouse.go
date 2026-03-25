package clickhouse

import (
	"context"
	"fmt"
	"peerdb-playground/gen"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type Config struct {
	User     string
	Password string
	Host     string
	Port     int
	Database string
}

func Connect(ctx context.Context, cfg Config) (driver.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.User,
			Password: cfg.Password,
		},
		ClientInfo: clickhouse.ClientInfo{
			Products: []struct {
				Name    string
				Version string
			}{
				{Name: "peerdb", Version: "1.0"},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("fail to connect to ch: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			return nil, fmt.Errorf("clickhouse exception [%d] %s: %s", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, fmt.Errorf("failed to ping ch: %w", err)
	}

	return conn, nil
}

func ConfigFromProto(cfg *gen.ClickhouseConfig) Config {
	return Config{
		Host:     cfg.Host,
		Port:     int(cfg.Port),
		User:     cfg.User,
		Password: cfg.Password,
		Database: cfg.Database,
	}
}

func ConnectFromProto(ctx context.Context, cfg *gen.ClickhouseConfig) (driver.Conn, error) {
	return Connect(ctx, ConfigFromProto(cfg))
}
