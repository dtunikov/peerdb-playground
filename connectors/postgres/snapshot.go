package postgres

import (
	"context"
	"fmt"
	"peerdb-playground/connectors"
	"strings"
)

const (
	snapshotBatchSize = 10_000
)

func (c *SourceConnector) SnapshotTable(ctx context.Context, table connectors.TableSchema) (<-chan connectors.RecordBatch, error) {
	ch := make(chan connectors.RecordBatch)
	go func() {
		defer close(ch)

		colsTransformed := make([]string, len(table.Columns))
		for i, col := range table.Columns {
			colsTransformed[i] = fmt.Sprintf(`"%s"`, col.Name)
		}

		tableName := fmt.Sprintf(`"%s"."%s"`, table.Table.Schema, table.Table.Name)
		query := fmt.Sprintf("SELECT %s from %s", strings.Join(colsTransformed, ","), tableName)

		batch := []connectors.Record{}
		rows, err := c.conn.Query(ctx, query)
		if err != nil {
			c.logger.Error("failed to execute snapshot query", "error", err, "query", query)
			return
		}
		defer rows.Close()

		for rows.Next() {
			values, err := rows.Values()
			if err != nil {
				c.logger.Error("failed to read snapshot row values", "error", err)
				return
			}
			colValues := make([]connectors.ColumnValue, len(values))
			for i, val := range values {
				colValues[i] = convertValue(table.Columns[i].Name, table.Columns[i].Type, val)
			}

			batch = append(batch, connectors.InsertRecord{
				BaseRecord: connectors.BaseRecord{
					Table: table.Table,
				},
				Values: colValues,
			})
			if len(batch) >= snapshotBatchSize {
				rb := connectors.RecordBatch{
					Records: batch,
				}
				select {
				case ch <- rb:
					batch = nil
				case <-ctx.Done():
					c.logger.Info("snapshot context cancelled, stopping snapshot")
					return
				}
			}
		}

		// flush the rest
		if len(batch) > 0 {
			select {
			case ch <- connectors.RecordBatch{
				Records: batch,
			}:
			case <-ctx.Done():
			}
		}
	}()

	return ch, nil
}
