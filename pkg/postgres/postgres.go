package postgres

import (
	"context"
	"errors"
	"fmt"
	"peerdb-playground/gen"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	Sql                       = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	ErrReplicationSlotMissing = errors.New("replication slot missing")
)

type Config struct {
	Host        string
	Port        int
	User        string
	Password    string
	Database    string
	SslMode     string
	Replication bool
}

func ConnectionString(cfg Config) string {
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		cfg.User,
		cfg.Password,
		cfg.Host,
		cfg.Port,
		cfg.Database,
	)
	queryParams := []string{}
	if cfg.SslMode != "" {
		queryParams = append(queryParams, "sslmode="+cfg.SslMode)
	}
	if cfg.Replication {
		queryParams = append(queryParams, "replication=database")
	}
	if len(queryParams) > 0 {
		connString += "?" + strings.Join(queryParams, "&")
	}

	return connString
}

func Connect(ctx context.Context, cfg Config) (*pgx.Conn, error) {
	connString := ConnectionString(cfg)
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return nil, fmt.Errorf("fail to connect: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping: %w", err)
	}

	return conn, nil
}

// CreatePublication creates a publication with the given name and tables. If tables is empty, it creates a publication for all tables.
func CreatePublication(ctx context.Context, conn *pgx.Conn, name string, tables []string) error {
	// create postgres publiction if it doesn't exist
	forTables := "ALL TABLES"
	if len(tables) != 0 {
		forTables = fmt.Sprintf("TABLE %s", strings.Join(tables, ", "))
	}

	_, err := conn.Exec(ctx, fmt.Sprintf(`CREATE PUBLICATION "%s" FOR %s`, name, forTables))
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	return nil
}

func ConfigFromProto(cfg *gen.PostgresConfig) Config {
	return Config{
		Host:     cfg.Host,
		Port:     int(cfg.Port),
		User:     cfg.User,
		Password: cfg.Password,
		Database: cfg.Database,
		SslMode:  cfg.SslMode,
	}
}

func ConnectFromProto(ctx context.Context, cfg *gen.PostgresConfig) (*pgx.Conn, error) {
	return Connect(ctx, ConfigFromProto(cfg))
}

func PublicationExists(ctx context.Context, conn *pgx.Conn, name string) (bool, error) {
	var exists bool

	err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)", name).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check publication existence: %w", err)
	}

	return exists, nil
}

func ReplicationSlotExists(ctx context.Context, conn *pgx.Conn, name string) (bool, error) {
	var exists bool

	err := conn.QueryRow(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)", name).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check replication slot existence: %w", err)
	}

	return exists, nil
}

func ConnectReplication(ctx context.Context, cfg Config) (*pgconn.PgConn, error) {
	cfg.Replication = true
	conn, err := pgconn.Connect(ctx, ConnectionString(cfg))
	if err != nil {
		return nil, fmt.Errorf("failed to connect for replication: %w", err)
	}

	return conn, nil
}

func CreateReplicationSlot(ctx context.Context, conn *pgconn.PgConn, name string) error {
	_, err := pglogrepl.CreateReplicationSlot(ctx, conn, name, "pgoutput", pglogrepl.CreateReplicationSlotOptions{})
	if err != nil {
		return fmt.Errorf("failed to create replication slot: %w", err)
	}

	return nil
}

func LoadReplicationSlotLSN(ctx context.Context, conn *pgx.Conn, name string) (pglogrepl.LSN, error) {
	var confirmedFlushLSN *string
	var restartLSN *string

	err := conn.QueryRow(ctx,
		`SELECT confirmed_flush_lsn::text, restart_lsn::text
		FROM pg_replication_slots
		WHERE slot_name = $1`,
		name,
	).Scan(&confirmedFlushLSN, &restartLSN)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, fmt.Errorf("%w: %s", ErrReplicationSlotMissing, name)
		}
		return 0, fmt.Errorf("failed to load replication slot state: %w", err)
	}

	lsn, err := ParseReplicationSlotLSN(confirmedFlushLSN, restartLSN)
	if err != nil {
		return 0, fmt.Errorf("failed to parse replication slot state: %w", err)
	}

	return lsn, nil
}

func ParseReplicationSlotLSN(confirmedFlushLSN, restartLSN *string) (pglogrepl.LSN, error) {
	if confirmedFlushLSN != nil && *confirmedFlushLSN != "" {
		lsn, err := pglogrepl.ParseLSN(*confirmedFlushLSN)
		if err != nil {
			return 0, fmt.Errorf("invalid confirmed_flush_lsn %q: %w", *confirmedFlushLSN, err)
		}
		return lsn, nil
	}

	if restartLSN != nil && *restartLSN != "" {
		lsn, err := pglogrepl.ParseLSN(*restartLSN)
		if err != nil {
			return 0, fmt.Errorf("invalid restart_lsn %q: %w", *restartLSN, err)
		}
		return lsn, nil
	}

	return 0, nil
}
