package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"peerdb-playground/connectors"
	"peerdb-playground/pkg/clickhouse"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type DestinationConnector struct {
	conn   driver.Conn
	logger *slog.Logger
	// mappings between source and destination tables, along with any columns to exclude from replication
	tableMappings connectors.TableMappings
}

func NewConnector(ctx context.Context, cfg clickhouse.Config, logger *slog.Logger,
	tableMappings connectors.TableMappings) (*DestinationConnector, error) {
	conn, err := clickhouse.Connect(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("fail to connect to clickhouse: %w", err)
	}

	return &DestinationConnector{
		conn:          conn,
		logger:        logger,
		tableMappings: tableMappings,
	}, nil
}

// Setup for clickhouse destination will create the necessary tables if they don't already exist based on the source table schemas
// and the provided table mappings in the flow config.
func (c *DestinationConnector) Setup(ctx context.Context, tables []connectors.TableSchema) error {
	for _, table := range tables {
		destName := table.Table.Name
		var excludeCols map[string]struct{}

		if tm, ok := c.tableMappings[table.Table.String()]; ok {
			if tm.DestTableName != "" {
				destName = tm.DestTableName
			}
			if len(tm.ExcludeColumns) > 0 {
				excludeCols = make(map[string]struct{}, len(tm.ExcludeColumns))
				for _, col := range tm.ExcludeColumns {
					excludeCols[col] = struct{}{}
				}
			}
		}

		var cols []string
		var pkCols []string
		for _, col := range table.Columns {
			if _, excluded := excludeCols[col.Name]; excluded {
				continue
			}
			chType := qTypToClickhouseType(col.Type)
			if col.Nullable {
				chType = fmt.Sprintf("Nullable(%s)", chType)
			}
			cols = append(cols, fmt.Sprintf("%s %s", col.Name, chType))
			if col.PrimaryKey {
				pkCols = append(pkCols, col.Name)
			}
		}
		// for deduplication we can add a version column that will be used to keep only the latest version of the record in case of updates
		// or in case of duplicate cdc event writes (e.g. due to at-least-once semantics of the source connector)
		cols = append(cols, "_version UInt64")
		cols = append(cols, "_ingested_at DateTime64(3, 'UTC') DEFAULT now64(3)")
		// for soft deletes, since clickhouse doesn't support hard deletes well
		// client would have to filter with _is_deleted=0 to get only non deleted rows
		cols = append(cols, "_is_deleted UInt8 DEFAULT 0")

		orderBy := "tuple()" // default if no pk cols is defined, rather rare case
		if len(pkCols) > 0 {
			orderBy = strings.Join(pkCols, ", ")
		}

		ddl := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) ENGINE = ReplacingMergeTree(_version, _is_deleted) ORDER BY (%s)",
			destName, strings.Join(cols, ", "), orderBy)
		c.logger.Info("creating destination table if not exists", "table", destName, "ddl", ddl)
		if err := c.conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("failed to create table %s: %w", destName, err)
		}
		c.logger.Info("created destination table", "table", destName)
	}

	return nil
}

func (c *DestinationConnector) Teardown(ctx context.Context) error {
	// TODO: remove tables to clean up resources in clickhouse.
	return c.Close(ctx)
}

func (c *DestinationConnector) Close(ctx context.Context) error {
	return c.conn.Close()
}

func (c *DestinationConnector) Write(ctx context.Context, ch <-chan connectors.RecordBatch) error {
	for recBatch := range ch {
		if err := c.WriteBatch(ctx, recBatch); err != nil {
			return err
		}
	}

	return nil
}

func columnNames(record connectors.Record) ([]string, error) {
	var values []connectors.ColumnValue
	switch r := record.(type) {
	case connectors.InsertRecord:
		values = r.Values
	case connectors.DeleteRecord:
		values = r.Values
	default:
		return nil, fmt.Errorf("unsupported record type %T", record)
	}

	names := make([]string, len(values))
	for i, col := range values {
		names[i] = fmt.Sprintf(`"%s"`, col.Name)
	}

	names = append(names, `"_version"`)
	names = append(names, `"_is_deleted"`)

	return names, nil
}

func (c *DestinationConnector) WriteBatch(ctx context.Context, recBatch connectors.RecordBatch) error {
	if len(recBatch.Records) == 0 {
		return nil
	}
	c.logger.Debug("writing batch to clickhouse", "batchId", recBatch.BatchId, "recordCount", len(recBatch.Records))
	batchByTable := make(map[string][]connectors.Record)
	for _, record := range recBatch.Records {
		sourceTable := record.GetTable().String()
		destName := record.GetTable().Name // default: use table name without schema
		if tm, ok := c.tableMappings[sourceTable]; ok && tm.DestTableName != "" {
			destName = tm.DestTableName
		}
		batchByTable[destName] = append(batchByTable[destName], record)
	}

	for table, records := range batchByTable {
		if len(records) == 0 {
			continue
		}

		tableName := fmt.Sprintf(`"%s"`, table)
		colNames, err := columnNames(records[0])
		if err != nil {
			return fmt.Errorf("failed to get column names for table %s: %w", table, err)
		}
		colList := strings.Join(colNames, ", ")
		chBatch, err := c.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s (%s)", tableName, colList))
		if err != nil {
			return fmt.Errorf("failed to prepare batch for table %s: %w", table, err)
		}

		for _, record := range records {
			var values []any
			switch r := record.(type) {
			case connectors.InsertRecord:
				for _, col := range r.Values {
					values = append(values, col.Value.Value())
				}
				values = append(values, r.Version)
				values = append(values, uint8(0))
			case connectors.DeleteRecord:
				// all values except PKs will be NULL for delete records
				for _, col := range r.Values {
					values = append(values, col.Value.Value())
				}
				values = append(values, r.Version)
				// soft delete flag, it will tell clickhouse to not return this record, cause we use ReplacingMergeTree(_version, _is_deleted)
				values = append(values, uint8(1))
			default:
				return fmt.Errorf("unsupported record type %T for table %s", record, table)
			}
			err = chBatch.Append(values...)
			if err != nil {
				return fmt.Errorf("failed to append record to batch for table %s: %w", table, err)
			}
		}

		err = chBatch.Send()
		if err != nil {
			return fmt.Errorf("failed to send batch for table %s: %w", table, err)
		}
	}
	c.logger.Debug("finished writing batch to clickhouse", "batchId", recBatch.BatchId)

	return nil
}
