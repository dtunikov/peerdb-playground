package postgres

import (
	"context"
	"fmt"
	"log/slog"
	"peerdb-playground/connectors"
	"peerdb-playground/pkg/postgres"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type SourceConnector struct {
	// regular connection for Setup (create slot/publication)
	conn *pgx.Conn
	// replication connection to read cdc changes
	replConn *pgconn.PgConn
	logger   *slog.Logger
	flowId   string
	// tables to replicate
	tables []string
	// publication name to use for this flow (will be generated if not provided in config)
	publicationName string
}

func NewConnector(ctx context.Context, flowId string, connConfig postgres.Config, logger *slog.Logger,
	tables []string, publicationName string) (*SourceConnector, error) {
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
		tables:          tables,
		publicationName: publicationName,
	}, nil
}

// Ack confirms processed LSN to postgres, so that it could clean up old WAL logs.
func (c *SourceConnector) Ack(ctx context.Context, position string) error {
	return nil
}

// Setup for postgres source will create the necessary publication and replication slot if they don't already exist.
// This is idempotent to allow for activity retries without creating duplicates.
func (c *SourceConnector) Setup(ctx context.Context) error {
	if c.publicationName == "" {
		// create a new publication if it doesn't already exist (idempotent for activity retries)
		pubName := c.generatePublicationName()
		exists, err := postgres.PublicationExists(ctx, c.conn, pubName)
		if err != nil {
			return fmt.Errorf("failed to check publication existence: %w", err)
		}
		if !exists {
			if err := postgres.CreatePublication(ctx, c.conn, pubName, c.tables); err != nil {
				return fmt.Errorf("failed to create publication: %w", err)
			}
			c.logger.Info("created publication for flow", "publication", pubName)
		} else {
			c.logger.Info("publication already exists, skipping creation", "publication", pubName)
		}
	}

	// create replication slot if it doesn't already exist (idempotent for activity retries)
	slotName := fmt.Sprintf("peerdb_slot_%s", strings.ReplaceAll(c.flowId, "-", "_"))
	slotExists, err := postgres.ReplicationSlotExists(ctx, c.conn, slotName)
	if err != nil {
		return fmt.Errorf("failed to check replication slot existence: %w", err)
	}
	if !slotExists {
		if err := postgres.CreateReplicationSlot(ctx, c.replConn, slotName); err != nil {
			return fmt.Errorf("failed to create replication slot: %w", err)
		}
		c.logger.Info("created replication slot for flow", "slot", slotName)
	} else {
		c.logger.Info("replication slot already exists, skipping creation", "slot", slotName)
	}

	return nil
}

func (c *SourceConnector) Teardown(ctx context.Context) error {
	c.Close(ctx)
	// TODO: remove publication (if we created it) and replication slot to clean up resources in postgres.
	return nil
}

func (c *SourceConnector) Read(ctx context.Context, ch chan<- connectors.RecordBatch) error {
	return nil
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
	c.conn.Close(ctx)
	c.replConn.Close(ctx)
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
