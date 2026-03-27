package postgres

import (
	"context"
	"errors"
	"fmt"
	"peerdb-playground/connectors"
	pg "peerdb-playground/pkg/postgres"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
)

const (
	standbyMessageTimeout   = 10 * time.Second
	initialReconnectBackoff = time.Second
	maxReconnectBackoff     = 30 * time.Second
)

type criticalReplicationError struct {
	err error
}

func (e criticalReplicationError) Error() string {
	return e.err.Error()
}

func (e criticalReplicationError) Unwrap() error {
	return e.err
}

type transactionBuffer struct {
	records []connectors.InsertRecord
}

func (t *transactionBuffer) reset() {
	t.records = nil
}

func criticalReplication(err error) error {
	if err == nil {
		return nil
	}
	return criticalReplicationError{err: err}
}

func isCriticalReplicationError(err error) bool {
	var criticalErr criticalReplicationError
	return errors.As(err, &criticalErr) || errors.Is(err, pg.ErrReplicationSlotMissing)
}

func (c *SourceConnector) IsCriticalError(err error) bool {
	return isCriticalReplicationError(err)
}

func (c *SourceConnector) readOnce(
	ctx context.Context,
	slotName string,
	publicationName string,
	tableSchemaByName map[string]connectors.TableSchema,
	typeMap *pgtype.Map,
	ch chan<- connectors.RecordBatch,
) error {
	startLSN, err := pg.LoadReplicationSlotLSN(ctx, c.conn, slotName)
	if err != nil {
		if errors.Is(err, pg.ErrReplicationSlotMissing) {
			return criticalReplication(err)
		}
		return err
	}

	c.initializeConfirmedLSN(startLSN)

	err = pglogrepl.StartReplication(ctx, c.replConn, slotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pgoutputPluginArgs(publicationName),
	})
	if err != nil {
		if isMissingSlotError(err) {
			return criticalReplication(fmt.Errorf("failed to start replication for slot %s: %w", slotName, err))
		}
		return fmt.Errorf("failed to start logical replication: %w", err)
	}

	relations := make(map[uint32]*pglogrepl.RelationMessage)
	txn := transactionBuffer{}
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			if err := c.sendStandbyStatus(ctx, false); err != nil {
				return err
			}
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		receiveCtx, cancel := context.WithDeadline(ctx, nextStandbyMessageDeadline)
		rawMsg, err := c.replConn.ReceiveMessage(receiveCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("failed to receive replication message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			return criticalReplication(fmt.Errorf("received postgres replication error %s: %s", errMsg.Code, errMsg.Message))
		}

		msg, ok := rawMsg.(*pgproto3.CopyData)
		if !ok {
			c.logger.Debug("ignoring unexpected replication message", "type", fmt.Sprintf("%T", rawMsg))
			continue
		}
		if len(msg.Data) == 0 {
			continue
		}

		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			keepalive, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			if err != nil {
				return criticalReplication(fmt.Errorf("failed to parse keepalive message: %w", err))
			}
			c.advanceReceivedLSN(keepalive.ServerWALEnd)
			if keepalive.ReplyRequested {
				if err := c.sendStandbyStatus(ctx, true); err != nil {
					return err
				}
				nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
			}

		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			if err != nil {
				return criticalReplication(fmt.Errorf("failed to parse xlog data: %w", err))
			}
			c.advanceReceivedLSN(xld.WALStart)
			if err := c.handleLogicalMessage(ctx, typeMap, xld.WALData, relations, tableSchemaByName, &txn, ch); err != nil {
				return err
			}
		}
	}
}

func (c *SourceConnector) handleLogicalMessage(
	ctx context.Context,
	typeMap *pgtype.Map,
	walData []byte,
	relations map[uint32]*pglogrepl.RelationMessage,
	tableSchemaByName map[string]connectors.TableSchema,
	txn *transactionBuffer,
	ch chan<- connectors.RecordBatch,
) error {
	logicalMsg, err := pglogrepl.Parse(walData)
	if err != nil {
		return criticalReplication(fmt.Errorf("failed to parse logical replication message: %w", err))
	}

	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		relations[msg.RelationID] = msg
	case *pglogrepl.BeginMessage:
		txn.reset()
	case *pglogrepl.InsertMessage:
		record, skip, err := makeInsertRecord(typeMap, relations, tableSchemaByName, msg.RelationID, msg.Tuple)
		if err != nil {
			return criticalReplication(err)
		}
		if !skip {
			txn.records = append(txn.records, record)
		}
	case *pglogrepl.UpdateMessage:
		if msg.NewTuple == nil {
			return criticalReplication(fmt.Errorf("update message for relation %d is missing new tuple", msg.RelationID))
		}
		record, skip, err := makeInsertRecord(typeMap, relations, tableSchemaByName, msg.RelationID, msg.NewTuple)
		if err != nil {
			return criticalReplication(err)
		}
		if skip {
			rel, relErr := relationForID(relations, msg.RelationID)
			if relErr != nil {
				return criticalReplication(relErr)
			}
			c.logger.Warn("skipping update with unchanged TOAST columns",
				"flowId", c.flowId,
				"table", relationQualifiedName(rel),
			)
			return nil
		}
		txn.records = append(txn.records, record)
	case *pglogrepl.CommitMessage:
		return c.flushTransaction(ctx, txn, msg.CommitLSN, ch)
	case *pglogrepl.DeleteMessage:
		c.logger.Debug("skipping delete event", "flowId", c.flowId, "relationId", msg.RelationID)
	case *pglogrepl.TruncateMessage:
		c.logger.Debug("skipping truncate event", "flowId", c.flowId)
	case *pglogrepl.TypeMessage:
	case *pglogrepl.OriginMessage:
	case *pglogrepl.LogicalDecodingMessage:
	default:
		c.logger.Debug("ignoring logical replication message", "type", fmt.Sprintf("%T", msg))
	}

	return nil
}

func (c *SourceConnector) flushTransaction(
	ctx context.Context,
	txn *transactionBuffer,
	commitLSN pglogrepl.LSN,
	ch chan<- connectors.RecordBatch,
) error {
	c.advanceReceivedLSN(commitLSN)
	if len(txn.records) == 0 {
		c.advanceAckedLSN(commitLSN)
		txn.reset()
		return nil
	}

	records := make([]connectors.Record, len(txn.records))
	for i, record := range txn.records {
		record.Version = uint64(commitLSN)
		records[i] = record
	}

	batch := connectors.RecordBatch{
		BatchId: commitLSN.String(),
		Records: records,
	}

	select {
	case ch <- batch:
		txn.reset()
		return nil
	case <-ctx.Done():
		return nil
	}
}

func (c *SourceConnector) sendStandbyStatus(ctx context.Context, replyRequested bool) error {
	lastReceivedLSN, lastAckedLSN := c.replicationPositions()
	err := pglogrepl.SendStandbyStatusUpdate(ctx, c.replConn, pglogrepl.StandbyStatusUpdate{
		WALWritePosition: lastReceivedLSN,
		WALFlushPosition: lastAckedLSN,
		WALApplyPosition: lastAckedLSN,
		ClientTime:       time.Now(),
		ReplyRequested:   replyRequested,
	})
	if err != nil {
		if isMissingSlotError(err) {
			return criticalReplication(fmt.Errorf("failed to send standby status update: %w", err))
		}
		return fmt.Errorf("failed to send standby status update: %w", err)
	}

	return nil
}

func (c *SourceConnector) reconnect(ctx context.Context) error {
	if c.replConn != nil {
		c.replConn.Close(ctx)
		c.replConn = nil
	}
	if c.conn != nil {
		c.conn.Close(ctx)
		c.conn = nil
	}

	conn, err := pg.Connect(ctx, c.cfg)
	if err != nil {
		return fmt.Errorf("failed to reconnect postgres connection: %w", err)
	}

	replConn, err := pg.ConnectReplication(ctx, c.cfg)
	if err != nil {
		conn.Close(ctx)
		return fmt.Errorf("failed to reconnect postgres replication connection: %w", err)
	}

	c.conn = conn
	c.replConn = replConn
	return nil
}

func indexTableSchemas(tableSchemas []connectors.TableSchema) map[string]connectors.TableSchema {
	index := make(map[string]connectors.TableSchema, len(tableSchemas))
	for _, tableSchema := range tableSchemas {
		index[tableSchema.Table.String()] = tableSchema
	}
	return index
}

func schemaForRelation(rel *pglogrepl.RelationMessage, tableSchemaByName map[string]connectors.TableSchema) (connectors.TableSchema, error) {
	qualifiedName := relationQualifiedName(rel)
	tableSchema, ok := tableSchemaByName[qualifiedName]
	if !ok {
		return connectors.TableSchema{}, fmt.Errorf("missing table schema for relation %s", qualifiedName)
	}
	return tableSchema, nil
}

func relationForID(relations map[uint32]*pglogrepl.RelationMessage, relationID uint32) (*pglogrepl.RelationMessage, error) {
	rel, ok := relations[relationID]
	if !ok {
		return nil, fmt.Errorf("missing relation metadata for relation ID %d", relationID)
	}
	return rel, nil
}

func relationQualifiedName(rel *pglogrepl.RelationMessage) string {
	return rel.Namespace + "." + rel.RelationName
}

func makeInsertRecord(
	typeMap *pgtype.Map,
	relations map[uint32]*pglogrepl.RelationMessage,
	tableSchemaByName map[string]connectors.TableSchema,
	relationID uint32,
	tuple *pglogrepl.TupleData,
) (connectors.InsertRecord, bool, error) {
	rel, err := relationForID(relations, relationID)
	if err != nil {
		return connectors.InsertRecord{}, false, err
	}

	tableSchema, err := schemaForRelation(rel, tableSchemaByName)
	if err != nil {
		return connectors.InsertRecord{}, false, err
	}

	values, skip, err := decodeTupleColumns(typeMap, rel, tableSchema, tuple)
	if err != nil {
		return connectors.InsertRecord{}, false, err
	}
	if skip {
		return connectors.InsertRecord{}, true, nil
	}

	return connectors.InsertRecord{
		BaseRecord: connectors.BaseRecord{
			Table: tableSchema.Table,
		},
		Values: values,
	}, false, nil
}

func decodeTupleColumns(
	typeMap *pgtype.Map,
	rel *pglogrepl.RelationMessage,
	tableSchema connectors.TableSchema,
	tuple *pglogrepl.TupleData,
) ([]connectors.ColumnValue, bool, error) {
	if tuple == nil {
		return nil, false, fmt.Errorf("missing tuple data for relation %s", relationQualifiedName(rel))
	}
	if len(tuple.Columns) > len(rel.Columns) {
		return nil, false, fmt.Errorf("tuple column count %d exceeds relation column count %d for %s", len(tuple.Columns), len(rel.Columns), relationQualifiedName(rel))
	}

	columnSchemas := make(map[string]connectors.ColumnSchema, len(tableSchema.Columns))
	for _, columnSchema := range tableSchema.Columns {
		columnSchemas[columnSchema.Name] = columnSchema
	}

	values := make([]connectors.ColumnValue, 0, len(tuple.Columns))
	for idx, col := range tuple.Columns {
		relationColumn := rel.Columns[idx]
		columnSchema, ok := columnSchemas[relationColumn.Name]
		if !ok {
			return nil, false, fmt.Errorf("missing column schema for %s.%s", relationQualifiedName(rel), relationColumn.Name)
		}

		value, skip, err := decodeTupleColumn(typeMap, columnSchema, relationColumn, col)
		if err != nil {
			return nil, false, err
		}
		if skip {
			return nil, true, nil
		}
		values = append(values, value)
	}

	return values, false, nil
}

func decodeTupleColumn(
	typeMap *pgtype.Map,
	columnSchema connectors.ColumnSchema,
	relationColumn *pglogrepl.RelationMessageColumn,
	column *pglogrepl.TupleDataColumn,
) (connectors.ColumnValue, bool, error) {
	switch column.DataType {
	case pglogrepl.TupleDataTypeNull:
		return convertValue(columnSchema.Name, columnSchema.Type, nil), false, nil
	case pglogrepl.TupleDataTypeText:
		value, err := decodeTextColumnData(typeMap, column.Data, relationColumn.DataType)
		if err != nil {
			return connectors.ColumnValue{}, false, fmt.Errorf("failed to decode %s: %w", columnSchema.Name, err)
		}
		return convertValue(columnSchema.Name, columnSchema.Type, value), false, nil
	case pglogrepl.TupleDataTypeBinary:
		return connectors.ColumnValue{}, false, fmt.Errorf("binary tuple format is not supported for column %s", columnSchema.Name)
	case pglogrepl.TupleDataTypeToast:
		return connectors.ColumnValue{}, true, nil
	default:
		return connectors.ColumnValue{}, false, fmt.Errorf("unsupported tuple data type %q for column %s", string(column.DataType), columnSchema.Name)
	}
}

func decodeTextColumnData(typeMap *pgtype.Map, data []byte, dataType uint32) (any, error) {
	if data == nil {
		return nil, nil
	}

	dt, ok := typeMap.TypeForOID(dataType)
	if !ok {
		return string(data), nil
	}
	return dt.Codec.DecodeValue(typeMap, dataType, pgtype.TextFormatCode, data)
}

func pgoutputPluginArgs(publicationName string) []string {
	safePublicationName := strings.ReplaceAll(publicationName, "'", "''")
	return []string{
		"proto_version '1'",
		fmt.Sprintf("publication_names '%s'", safePublicationName),
	}
}

func isMissingSlotError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "42704" && strings.Contains(strings.ToLower(pgErr.Message), "replication slot")
}
