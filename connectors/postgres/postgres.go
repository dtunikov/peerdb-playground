package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"peerdb-playground/connectors"
	"peerdb-playground/pkg/postgres"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

type SourceConnector struct {
	// regular connection for Setup (create slot/publication)
	conn *pgx.Conn
	// replication connection to read cdc changes
	replConn *pgconn.PgConn
	logger   *slog.Logger
	flowId   string
	cfg      postgres.Config
	// tables to replicate
	tables []string
	// publication name to use for this flow (will be generated if not provided in config)
	publicationName string

	lastReceivedLSN atomic.Uint64
	lastAckedLSN    atomic.Uint64
}

var (
	typeMap = pgtype.NewMap()
)

func NewConnector(ctx context.Context, flowId string, connConfig postgres.Config, logger *slog.Logger,
	tables []string, publicationName string, _ string) (*SourceConnector, error) {
	conn, err := postgres.Connect(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("fail to connect to postgres: %w", err)
	}

	replConn, err := postgres.ConnectReplication(ctx, connConfig)
	if err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("fail to connect to postgres with replication: %w", err)
	}

	return &SourceConnector{
		conn:            conn,
		replConn:        replConn,
		logger:          logger,
		flowId:          flowId,
		cfg:             connConfig,
		tables:          tables,
		publicationName: publicationName,
	}, nil
}

// Ack confirms processed LSN to postgres, so that it could clean up old WAL logs.
func (c *SourceConnector) Ack(ctx context.Context, position string) error {
	lsn, err := pglogrepl.ParseLSN(position)
	if err != nil {
		return fmt.Errorf("failed to parse ack position %q: %w", position, err)
	}

	c.advanceAckedLSN(lsn)
	return nil
}

// Setup for postgres source will create the necessary publication and replication slot if they don't already exist.
// This is idempotent to allow for activity retries without creating duplicates.
// Returns the initial LSN from the replication slot for checkpoint consistency across peer types.
func (c *SourceConnector) Setup(ctx context.Context) (string, error) {
	if c.publicationName == "" {
		// create a new publication if it doesn't already exist (idempotent for activity retries)
		pubName := c.generatePublicationName()
		exists, err := postgres.PublicationExists(ctx, c.conn, pubName)
		if err != nil {
			return "", fmt.Errorf("failed to check publication existence: %w", err)
		}
		if !exists {
			if err := postgres.CreatePublication(ctx, c.conn, pubName, c.tables); err != nil {
				return "", fmt.Errorf("failed to create publication: %w", err)
			}
			c.logger.Info("created publication for flow", "publication", pubName)
		} else {
			c.logger.Info("publication already exists, skipping creation", "publication", pubName)
		}
	}

	// create replication slot if it doesn't already exist (idempotent for activity retries)
	slotName := c.slotName()
	slotExists, err := postgres.ReplicationSlotExists(ctx, c.conn, slotName)
	if err != nil {
		return "", fmt.Errorf("failed to check replication slot existence: %w", err)
	}

	var initialLSN pglogrepl.LSN
	if !slotExists {
		initialLSN, err = postgres.CreateReplicationSlot(ctx, c.replConn, slotName)
		if err != nil {
			return "", fmt.Errorf("failed to create replication slot: %w", err)
		}
		c.logger.Info("created replication slot for flow", "slot", slotName, "lsn", initialLSN)
	} else {
		c.logger.Info("replication slot already exists, skipping creation", "slot", slotName)
		initialLSN, err = postgres.LoadReplicationSlotLSN(ctx, c.conn, slotName)
		if err != nil {
			return "", fmt.Errorf("failed to load replication slot LSN: %w", err)
		}
	}

	return initialLSN.String(), nil
}

func (c *SourceConnector) Teardown(ctx context.Context) error {
	c.Close(ctx)
	// TODO: remove publication (if we created it) and replication slot to clean up resources in postgres.
	return nil
}

func (c *SourceConnector) Read(ctx context.Context, ch chan<- connectors.RecordBatch) error {
	defer close(ch)

	tableSchemas, err := c.GetTableSchemas(ctx)
	if err != nil {
		return fmt.Errorf("failed to load table schemas for CDC: %w", err)
	}

	tableSchemaByName := indexTableSchemas(tableSchemas)
	publicationName := c.resolvedPublicationName()
	slotName := c.slotName()

	backoff := initialReconnectBackoff
	for {
		err := c.readOnce(ctx, slotName, publicationName, tableSchemaByName, ch)
		if err == nil || ctx.Err() != nil {
			return nil
		}
		if isCriticalReplicationError(err) {
			return err
		}

		c.logger.Warn("replication stream interrupted, retrying", "slot", slotName, "backoff", backoff, "error", err)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return nil
		}

		if err := c.reconnect(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("failed to reconnect postgres source connector: %w", err)
		}
		if backoff < maxReconnectBackoff {
			backoff *= 2
			if backoff > maxReconnectBackoff {
				backoff = maxReconnectBackoff
			}
		}
	}
}

func (c *SourceConnector) generatePublicationName() string {
	return fmt.Sprintf("peerdb_pub_%s", c.flowId)
}

// GetTableSchemas retrieves the schemas of the tables included in the publication for this flow.
func (c *SourceConnector) GetTableSchemas(ctx context.Context) ([]connectors.TableSchema, error) {
	pubName := c.publicationName
	if pubName == "" {
		pubName = c.generatePublicationName()
	}
	tables, err := c.getPublicationTables(ctx, pubName)
	if err != nil {
		return nil, fmt.Errorf("failed to get publication tables: %w", err)
	}

	schemas := make([]connectors.TableSchema, 0, len(tables))
	for _, table := range tables {
		schema, err := c.getTableSchema(ctx, table)
		if err != nil {
			return nil, fmt.Errorf("failed to get schema for table %s: %w", table, err)
		}
		schemas = append(schemas, *schema)
	}

	return schemas, nil
}

func (c *SourceConnector) getPublicationTables(ctx context.Context, pubName string) ([]string, error) {
	rows, err := c.conn.Query(ctx,
		"SELECT schemaname || '.' || tablename FROM pg_publication_tables WHERE pubname = $1", pubName)
	if err != nil {
		return nil, fmt.Errorf("failed to query publication tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var table string
		if err := rows.Scan(&table); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating publication tables: %w", err)
	}
	return tables, nil
}

func (c *SourceConnector) Close(ctx context.Context) error {
	if c.conn != nil {
		c.conn.Close(ctx)
		c.conn = nil
	}
	if c.replConn != nil {
		c.replConn.Close(ctx)
		c.replConn = nil
	}
	return nil
}

func (c *SourceConnector) getTableSchema(ctx context.Context, qualifiedName string) (*connectors.TableSchema, error) {
	// get primary key columns
	pkCols, err := c.getPrimaryKeyColumns(ctx, qualifiedName)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key columns: %w", err)
	}

	rows, err := c.conn.Query(ctx, `
		SELECT column_name, is_nullable, udt_name,
			   numeric_precision, numeric_scale
		FROM information_schema.columns
		WHERE table_schema || '.' || table_name = $1
		ORDER BY ordinal_position`, qualifiedName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var columns []connectors.ColumnSchema
	for rows.Next() {
		var colName, isNullable, udtName string
		var numericPrecision, numericScale *int
		if err := rows.Scan(&colName, &isNullable, &udtName, &numericPrecision, &numericScale); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		columns = append(columns, connectors.ColumnSchema{
			Name:       colName,
			Type:       pgTypeToQType(udtName, numericPrecision, numericScale),
			Nullable:   isNullable == "YES",
			PrimaryKey: pkCols[colName],
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over columns information_schema: %w", err)
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s not found or has no columns", qualifiedName)
	}

	parts := strings.SplitN(qualifiedName, ".", 2)
	table := connectors.TableIdentifier{Name: qualifiedName}
	if len(parts) == 2 {
		table = connectors.TableIdentifier{Schema: parts[0], Name: parts[1]}
	}

	return &connectors.TableSchema{
		Table:   table,
		Columns: columns,
	}, nil
}

func (c *SourceConnector) getPrimaryKeyColumns(ctx context.Context, qualifiedName string) (map[string]bool, error) {
	rows, err := c.conn.Query(ctx, `
		SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass AND i.indisprimary`, qualifiedName)
	if err != nil {
		return nil, fmt.Errorf("failed to query primary key: %w", err)
	}
	defer rows.Close()

	pkCols := make(map[string]bool)
	for rows.Next() {
		var colName string
		if err := rows.Scan(&colName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		pkCols[colName] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating primary key columns: %w", err)
	}

	return pkCols, nil
}

func (c *SourceConnector) slotName() string {
	return fmt.Sprintf("peerdb_slot_%s", strings.ReplaceAll(c.flowId, "-", "_"))
}

func (c *SourceConnector) resolvedPublicationName() string {
	if c.publicationName != "" {
		return c.publicationName
	}
	return c.generatePublicationName()
}

func (c *SourceConnector) advanceReceivedLSN(lsn pglogrepl.LSN) {
	for {
		cur := pglogrepl.LSN(c.lastReceivedLSN.Load())
		if lsn <= cur || c.lastReceivedLSN.CompareAndSwap(uint64(cur), uint64(lsn)) {
			return
		}
	}
}

// advanceAckedLSN atomic update lastAchedLSN, it has to be atomic, because it can be read by replication loop AND updated by Ack method concurrently. It only advances forward and never goes back, so it's safe to just ignore updates that are older than the current value.
func (c *SourceConnector) advanceAckedLSN(lsn pglogrepl.LSN) {
	for {
		cur := pglogrepl.LSN(c.lastAckedLSN.Load())
		if lsn <= cur || c.lastAckedLSN.CompareAndSwap(uint64(cur), uint64(lsn)) {
			return
		}
	}
}

func (c *SourceConnector) initializeConfirmedLSN(lsn pglogrepl.LSN) {
	c.advanceReceivedLSN(lsn)
	c.advanceAckedLSN(lsn)
}

func (c *SourceConnector) replicationPositions() (pglogrepl.LSN, pglogrepl.LSN) {
	return pglogrepl.LSN(c.lastReceivedLSN.Load()), pglogrepl.LSN(c.lastAckedLSN.Load())
}
