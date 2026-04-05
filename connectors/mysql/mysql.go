package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"peerdb-playground/connectors"
	mysqlpkg "peerdb-playground/pkg/mysql"
	"strings"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

const (
	snapshotBatchSize = 10_000
)

type criticalMySQLError struct {
	err error
}

func (e criticalMySQLError) Error() string {
	return e.err.Error()
}

func (e criticalMySQLError) Unwrap() error {
	return e.err
}

func criticalMySQL(err error) error {
	if err == nil {
		return nil
	}
	return criticalMySQLError{err: err}
}

func isCriticalMySQLError(err error) bool {
	var criticalErr criticalMySQLError
	return errors.As(err, &criticalErr)
}

type SourceConnector struct {
	db               *sql.DB
	logger           *slog.Logger
	flowID           string
	cfg              mysqlpkg.Config
	tables           []string
	resumeCheckpoint string
	syncer           *replication.BinlogSyncer
	tableSchemas     []connectors.TableSchema
}

func NewConnector(
	ctx context.Context,
	flowID string,
	cfg mysqlpkg.Config,
	logger *slog.Logger,
	tables []string,
	resumeCheckpoint string,
) (*SourceConnector, error) {
	db, err := mysqlpkg.Connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to mysql: %w", err)
	}

	return &SourceConnector{
		db:               db,
		logger:           logger,
		flowID:           flowID,
		cfg:              cfg,
		tables:           tables,
		resumeCheckpoint: resumeCheckpoint,
	}, nil
}

func (c *SourceConnector) Setup(ctx context.Context) (string, error) {
	if err := mysqlpkg.ValidateCDCPrerequisites(ctx, c.db); err != nil {
		return "", criticalMySQL(err)
	}

	gtidSet, err := mysqlpkg.LoadExecutedGTIDSet(ctx, c.db)
	if err != nil {
		return "", criticalMySQL(err)
	}

	return gtidSet, nil
}

func (c *SourceConnector) Teardown(ctx context.Context) error {
	return c.Close(ctx)
}

func (c *SourceConnector) Close(ctx context.Context) error {
	if c.syncer != nil {
		c.syncer.Close()
		c.syncer = nil
	}
	if c.db != nil {
		err := c.db.Close()
		c.db = nil
		return err
	}
	return nil
}

func (c *SourceConnector) Ack(ctx context.Context, position string) error {
	if position == "" {
		return nil
	}
	if _, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, position); err != nil {
		return fmt.Errorf("failed to parse ack gtid set %q: %w", position, err)
	}
	return nil
}

func (c *SourceConnector) IsCriticalError(err error) bool {
	return isCriticalMySQLError(err)
}

func (c *SourceConnector) GetTableSchemas(ctx context.Context) ([]connectors.TableSchema, error) {
	if c.tableSchemas != nil {
		return c.tableSchemas, nil
	}

	tableRefs, err := c.selectedTables(ctx)
	if err != nil {
		return nil, err
	}

	schemas := make([]connectors.TableSchema, 0, len(tableRefs))
	for _, tableRef := range tableRefs {
		schema, err := c.getTableSchema(ctx, tableRef)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}

	c.tableSchemas = schemas
	return schemas, nil
}

func (c *SourceConnector) selectedTables(ctx context.Context) ([]connectors.TableIdentifier, error) {
	if len(c.tables) == 0 {
		rows, err := c.db.QueryContext(ctx, `
			SELECT table_name
			FROM information_schema.tables
			WHERE table_schema = ?
			  AND table_type = 'BASE TABLE'
			ORDER BY table_name
		`, c.cfg.Database)
		if err != nil {
			return nil, fmt.Errorf("failed to list mysql tables: %w", err)
		}
		defer rows.Close()

		var tables []connectors.TableIdentifier
		for rows.Next() {
			var tableName string
			if err := rows.Scan(&tableName); err != nil {
				return nil, fmt.Errorf("failed to scan mysql table name: %w", err)
			}
			tables = append(tables, connectors.TableIdentifier{Schema: c.cfg.Database, Name: tableName})
		}
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("failed to iterate mysql tables: %w", err)
		}
		return tables, nil
	}

	tables := make([]connectors.TableIdentifier, 0, len(c.tables))
	for _, qualifiedName := range c.tables {
		table, err := mysqlParseTableIdentifier(qualifiedName, c.cfg.Database)
		if err != nil {
			return nil, err
		}
		if table.Schema != c.cfg.Database {
			return nil, fmt.Errorf("mysql source only supports tables from configured database %s, got %s", c.cfg.Database, qualifiedName)
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func (c *SourceConnector) getTableSchema(ctx context.Context, table connectors.TableIdentifier) (connectors.TableSchema, error) {
	pkCols, err := c.mysqlPrimaryKeyColumns(ctx, table)
	if err != nil {
		return connectors.TableSchema{}, err
	}

	rows, err := c.db.QueryContext(ctx, `
		SELECT column_name, is_nullable, data_type, column_type, numeric_precision, numeric_scale
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position
	`, table.Schema, table.Name)
	if err != nil {
		return connectors.TableSchema{}, fmt.Errorf("failed to query mysql columns for %s: %w", table.String(), err)
	}
	defer rows.Close()

	columns := make([]connectors.ColumnSchema, 0)
	for rows.Next() {
		var columnName string
		var isNullable string
		var dataType string
		var columnType string
		var numericPrecision sql.NullInt64
		var numericScale sql.NullInt64
		if err := rows.Scan(&columnName, &isNullable, &dataType, &columnType, &numericPrecision, &numericScale); err != nil {
			return connectors.TableSchema{}, fmt.Errorf("failed to scan mysql column metadata for %s: %w", table.String(), err)
		}

		qtype, err := mysqlTypeToQType(dataType, columnType, numericPrecision, numericScale)
		if err != nil {
			return connectors.TableSchema{}, fmt.Errorf("unsupported mysql column %s.%s: %w", table.String(), columnName, err)
		}

		columns = append(columns, connectors.ColumnSchema{
			Name:       columnName,
			Type:       qtype,
			Nullable:   isNullable == "YES",
			PrimaryKey: pkCols[columnName],
		})
	}
	if err := rows.Err(); err != nil {
		return connectors.TableSchema{}, fmt.Errorf("failed to iterate mysql columns for %s: %w", table.String(), err)
	}
	if len(columns) == 0 {
		return connectors.TableSchema{}, fmt.Errorf("mysql table %s not found or has no columns", table.String())
	}

	return connectors.TableSchema{
		Table:   table,
		Columns: columns,
	}, nil
}

func (c *SourceConnector) mysqlPrimaryKeyColumns(ctx context.Context, table connectors.TableIdentifier) (map[string]bool, error) {
	rows, err := c.db.QueryContext(ctx, `
		SELECT column_name
		FROM information_schema.statistics
		WHERE table_schema = ?
		  AND table_name = ?
		  AND index_name = 'PRIMARY'
		ORDER BY seq_in_index
	`, table.Schema, table.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to query mysql primary key for %s: %w", table.String(), err)
	}
	defer rows.Close()

	pkCols := make(map[string]bool)
	for rows.Next() {
		var columnName string
		if err := rows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan mysql primary key column for %s: %w", table.String(), err)
		}
		pkCols[columnName] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate mysql primary key columns for %s: %w", table.String(), err)
	}

	return pkCols, nil
}

func mysqlParseTableIdentifier(qualifiedName string, defaultDatabase string) (connectors.TableIdentifier, error) {
	parts := strings.SplitN(qualifiedName, ".", 2)
	if len(parts) == 1 {
		return connectors.TableIdentifier{Schema: defaultDatabase, Name: parts[0]}, nil
	}
	if parts[0] == "" || parts[1] == "" {
		return connectors.TableIdentifier{}, fmt.Errorf("invalid mysql table name %q", qualifiedName)
	}
	return connectors.TableIdentifier{Schema: parts[0], Name: parts[1]}, nil
}

func mysqlQuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func mysqlQuoteQualifiedName(schema string, table string) string {
	if schema == "" {
		return mysqlQuoteIdentifier(table)
	}
	return mysqlQuoteIdentifier(schema) + "." + mysqlQuoteIdentifier(table)
}
