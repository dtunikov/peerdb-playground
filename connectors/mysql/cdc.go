package mysql

import (
	"context"
	"fmt"
	"hash/crc32"
	"peerdb-playground/connectors"
	"peerdb-playground/connectors/types"
	mysqlpkg "peerdb-playground/pkg/mysql"
	"strconv"
	"strings"
	"time"

	gomysql "github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type transactionBuffer struct {
	batchID string
	records []connectors.Record
}

func (t *transactionBuffer) begin(batchID string) {
	t.batchID = batchID
	t.records = nil
}

func (t *transactionBuffer) reset() {
	t.batchID = ""
	t.records = nil
}

func (c *SourceConnector) Read(ctx context.Context, ch chan<- connectors.RecordBatch) error {
	defer close(ch)

	tableSchemas, err := c.GetTableSchemas(ctx)
	if err != nil {
		return criticalMySQL(fmt.Errorf("failed to load mysql table schemas: %w", err))
	}

	tableSchemaByName := make(map[string]connectors.TableSchema, len(tableSchemas))
	for _, tableSchema := range tableSchemas {
		tableSchemaByName[tableSchema.Table.String()] = tableSchema
	}

	checkpoint := c.resumeCheckpoint
	if checkpoint == "" {
		checkpoint, err = mysqlpkg.LoadExecutedGTIDSet(ctx, c.db)
		if err != nil {
			return criticalMySQL(fmt.Errorf("failed to load mysql gtid checkpoint: %w", err))
		}
	}

	gset, err := gomysql.ParseGTIDSet(gomysql.MySQLFlavor, checkpoint)
	if err != nil {
		return criticalMySQL(fmt.Errorf("failed to parse mysql gtid checkpoint %q: %w", checkpoint, err))
	}

	currentGTIDSet := gset.Clone()
	currentBinlogName, _, _, err := mysqlpkg.LoadMasterStatus(ctx, c.db)
	if err != nil {
		c.logger.Warn("failed to load current mysql master status", "error", err)
	}

	syncer := replication.NewBinlogSyncer(replication.BinlogSyncerConfig{
		ServerID:                mysqlReplicationServerID(c.flowID),
		Flavor:                  gomysql.MySQLFlavor,
		Host:                    c.cfg.Host,
		Port:                    uint16(c.cfg.Port),
		User:                    c.cfg.User,
		Password:                c.cfg.Password,
		ParseTime:               true,
		TimestampStringLocation: time.UTC,
		MaxReconnectAttempts:    0,
		Logger:                  c.logger,
	})
	c.syncer = syncer

	streamer, err := syncer.StartSyncGTID(gset)
	if err != nil {
		return criticalMySQL(fmt.Errorf("failed to start mysql binlog sync: %w", err))
	}

	txn := transactionBuffer{}
	for {
		event, err := streamer.GetEvent(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("failed to read mysql binlog event: %w", err)
		}

		// syncer may update binlog name internally without emitting a RotateEvent (e.g. on initial connection),
		// so we also check GetNextPosition as a fallback in addition to handling RotateEvent below.
		nextPos := syncer.GetNextPosition()
		if nextPos.Name != "" {
			currentBinlogName = nextPos.Name
		}

		switch ev := event.Event.(type) {
		case *replication.RotateEvent:
			currentBinlogName = string(ev.NextLogName)
		case *replication.GTIDEvent:
			nextGTID, err := ev.GTIDNext()
			if err != nil {
				return criticalMySQL(fmt.Errorf("failed to decode mysql gtid event: %w", err))
			}
			if err := currentGTIDSet.Update(nextGTID.String()); err != nil {
				return criticalMySQL(fmt.Errorf("failed to advance mysql gtid set: %w", err))
			}
			txn.begin(currentGTIDSet.String())
		case *replication.RowsEvent:
			qualifiedName := string(ev.Table.Schema) + "." + string(ev.Table.Table)
			tableSchema, ok := tableSchemaByName[qualifiedName]
			if !ok {
				continue
			}
			version := mysqlVersionForPosition(currentBinlogName, nextPos.Pos)
			if err := appendRowsEvent(&txn, ev, tableSchema, version, event.Header.EventType); err != nil {
				return criticalMySQL(err)
			}
		case *replication.XIDEvent:
			// XIDEvent is the normal commit marker for InnoDB transactions.
			flushTransaction(ctx, ch, &txn)
		case *replication.QueryEvent:
			// QueryEvent("COMMIT") covers non-transactional engines (e.g. MyISAM)
			// where XIDEvent is not emitted.
			query := strings.ToUpper(strings.TrimSpace(string(ev.Query)))
			switch query {
			case "BEGIN":
			case "COMMIT":
				flushTransaction(ctx, ch, &txn)
			default:
				c.logger.Debug("ignoring mysql query event", "query", string(ev.Query))
			}
		default:
		}
	}
}

func flushTransaction(ctx context.Context, ch chan<- connectors.RecordBatch, txn *transactionBuffer) {
	if txn.batchID == "" {
		return
	}

	batch := connectors.RecordBatch{
		BatchId: txn.batchID,
		Records: txn.records,
	}
	txn.reset()

	select {
	case ch <- batch:
		return
	case <-ctx.Done():
		return
	}
}

func appendRowsEvent(
	txn *transactionBuffer,
	event *replication.RowsEvent,
	tableSchema connectors.TableSchema,
	version uint64,
	eventType replication.EventType,
) error {
	switch eventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		for _, row := range event.Rows {
			record, err := mysqlInsertRecord(tableSchema, row, version)
			if err != nil {
				return err
			}
			txn.records = append(txn.records, record)
		}
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2, replication.PARTIAL_UPDATE_ROWS_EVENT:
		for i := 1; i < len(event.Rows); i += 2 {
			record, err := mysqlInsertRecord(tableSchema, event.Rows[i], version)
			if err != nil {
				return err
			}
			txn.records = append(txn.records, record)
		}
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		for _, row := range event.Rows {
			record, err := mysqlDeleteRecord(tableSchema, row, version)
			if err != nil {
				return err
			}
			txn.records = append(txn.records, record)
		}
	default:
	}

	return nil
}

func mysqlInsertRecord(tableSchema connectors.TableSchema, row []any, version uint64) (connectors.InsertRecord, error) {
	if len(row) != len(tableSchema.Columns) {
		return connectors.InsertRecord{}, fmt.Errorf(
			"mysql row column count mismatch for table %s: got %d want %d",
			tableSchema.Table.String(),
			len(row),
			len(tableSchema.Columns),
		)
	}

	values := make([]connectors.ColumnValue, len(tableSchema.Columns))
	for i, col := range tableSchema.Columns {
		value, err := mysqlColumnValue(col.Name, col.Type, row[i])
		if err != nil {
			return connectors.InsertRecord{}, fmt.Errorf(
				"failed to convert mysql cdc value for table %s column %s: %w",
				tableSchema.Table.String(),
				col.Name,
				err,
			)
		}
		values[i] = value
	}

	return connectors.InsertRecord{
		BaseRecord: connectors.BaseRecord{
			Table:   tableSchema.Table,
			Version: version,
		},
		Values: values,
	}, nil
}

func mysqlDeleteRecord(tableSchema connectors.TableSchema, row []any, version uint64) (connectors.DeleteRecord, error) {
	if len(row) != len(tableSchema.Columns) {
		return connectors.DeleteRecord{}, fmt.Errorf(
			"mysql delete row column count mismatch for table %s: got %d want %d",
			tableSchema.Table.String(),
			len(row),
			len(tableSchema.Columns),
		)
	}

	hasPk := false
	for _, col := range tableSchema.Columns {
		if col.PrimaryKey {
			hasPk = true
			break
		}
	}
	if !hasPk {
		return connectors.DeleteRecord{}, fmt.Errorf(
			"cannot process mysql delete for table %s without primary key",
			tableSchema.Table.String(),
		)
	}

	values := make([]connectors.ColumnValue, len(tableSchema.Columns))
	for i, col := range tableSchema.Columns {
		if !col.PrimaryKey {
			values[i] = connectors.ColumnValue{Name: col.Name, Value: types.QValueNull{}}
			continue
		}
		value, err := mysqlColumnValue(col.Name, col.Type, row[i])
		if err != nil {
			return connectors.DeleteRecord{}, fmt.Errorf(
				"failed to convert mysql cdc delete value for table %s column %s: %w",
				tableSchema.Table.String(),
				col.Name,
				err,
			)
		}
		values[i] = value
	}

	return connectors.DeleteRecord{
		BaseRecord: connectors.BaseRecord{
			Table:   tableSchema.Table,
			Version: version,
		},
		Values: values,
	}, nil
}

func mysqlReplicationServerID(flowID string) uint32 {
	sum := crc32.ChecksumIEEE([]byte(flowID))
	if sum == 0 {
		return 1
	}
	return sum
}

// mysqlVersionForPosition encodes binlog file sequence and position into a single monotonically increasing uint64.
// Example: binlogName="binlog.000003", pos=1000 -> (3 << 32) | 1000 = 12885886952
// This ensures records from later binlog files always have higher versions.
func mysqlVersionForPosition(binlogName string, pos uint32) uint64 {
	if binlogName == "" {
		return uint64(pos)
	}

	parts := strings.Split(binlogName, ".")
	lastPart := parts[len(parts)-1]
	fileSeq, err := strconv.ParseUint(lastPart, 10, 32)
	if err != nil {
		return uint64(pos)
	}

	return (fileSeq << 32) | uint64(pos)
}
