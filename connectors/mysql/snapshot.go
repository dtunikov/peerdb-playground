package mysql

import (
	"context"
	"fmt"
	"peerdb-playground/connectors"
	"strings"
)

func (c *SourceConnector) SnapshotTable(ctx context.Context, table connectors.TableSchema) (<-chan connectors.RecordBatch, error) {
	ch := make(chan connectors.RecordBatch)
	go func() {
		defer close(ch)

		cols := make([]string, len(table.Columns))
		for i, col := range table.Columns {
			cols[i] = mysqlQuoteIdentifier(col.Name)
		}

		query := fmt.Sprintf(
			"SELECT %s FROM %s",
			strings.Join(cols, ", "),
			mysqlQuoteQualifiedName(table.Table.Schema, table.Table.Name),
		)

		rows, err := c.db.QueryContext(ctx, query)
		if err != nil {
			c.logger.Error("failed to execute mysql snapshot query", "query", query, "error", err)
			return
		}
		defer rows.Close()

		rawValues := make([]any, len(table.Columns))
		scanArgs := make([]any, len(table.Columns))
		for i := range rawValues {
			scanArgs[i] = &rawValues[i]
		}

		batch := make([]connectors.Record, 0, snapshotBatchSize)
		for rows.Next() {
			if err := rows.Scan(scanArgs...); err != nil {
				c.logger.Error("failed to scan mysql snapshot row", "error", err)
				return
			}

			values := make([]connectors.ColumnValue, len(table.Columns))
			for i, col := range table.Columns {
				value, err := mysqlColumnValue(col.Name, col.Type, rawValues[i])
				if err != nil {
					c.logger.Error("failed to convert mysql snapshot value", "table", table.Table.String(), "column", col.Name, "error", err)
					return
				}
				values[i] = value
			}

			batch = append(batch, connectors.InsertRecord{
				BaseRecord: connectors.BaseRecord{
					Table: table.Table,
				},
				Values: values,
			})
			if len(batch) >= snapshotBatchSize {
				select {
				case ch <- connectors.RecordBatch{Records: batch}:
					batch = nil
				case <-ctx.Done():
					return
				}
			}
		}
		if err := rows.Err(); err != nil {
			c.logger.Error("failed while iterating mysql snapshot rows", "error", err)
			return
		}

		if len(batch) > 0 {
			select {
			case ch <- connectors.RecordBatch{Records: batch}:
			case <-ctx.Done():
			}
		}
	}()

	return ch, nil
}
