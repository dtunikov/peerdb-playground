package cdcbench

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"peerdb-playground/gen"
	"peerdb-playground/internal/localenv"
	"peerdb-playground/internal/sqlutil"
	chpkg "peerdb-playground/pkg/clickhouse"
	"peerdb-playground/server"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	sq "github.com/Masterminds/squirrel"
)

const (
	insertRatio         = 0.9
	defaultPollInterval = 10 * time.Second
	windowSize          = 100 * time.Millisecond
	maxRecentIDs        = 4096
	recoveryTimeout     = 2 * time.Minute
)

type SourceKind string

const (
	SourcePostgres SourceKind = "postgres"
	SourceMySQL    SourceKind = "mysql"
	SourceBoth     SourceKind = "both"
)

type ScenarioKind string

const (
	ScenarioSteady ScenarioKind = "steady"
	ScenarioRamp   ScenarioKind = "ramp"
	ScenarioBurst  ScenarioKind = "burst"
)

type Config struct {
	Source          SourceKind
	Scenario        ScenarioKind
	OutputDir       string
	FlushIntervalMs int
	MaxBatchSize    int
	RateOverride    int
	Duration        time.Duration
	PollInterval    time.Duration
}

type RunResult struct {
	Source               SourceKind      `json:"source"`
	Scenario             ScenarioKind    `json:"scenario"`
	StartedAt            time.Time       `json:"started_at"`
	FinishedAt           time.Time       `json:"finished_at"`
	TargetWriteRate      float64         `json:"target_write_rate"`
	AchievedWriteRate    float64         `json:"achieved_write_rate"`
	VisibleIngestRate    float64         `json:"visible_ingest_rate"`
	TotalOps             int64           `json:"total_ops"`
	TotalInserts         int64           `json:"total_inserts"`
	TotalUpdates         int64           `json:"total_updates"`
	P50LatencyMs         float64         `json:"p50_latency_ms"`
	P95LatencyMs         float64         `json:"p95_latency_ms"`
	P99LatencyMs         float64         `json:"p99_latency_ms"`
	MaxLatencyMs         float64         `json:"max_latency_ms"`
	MaxObservedSeq       int64           `json:"max_observed_seq"`
	MaxBacklog           int64           `json:"max_backlog"`
	RecoveryTimeMs       int64           `json:"recovery_time_ms"`
	HighestSustainedRate float64         `json:"highest_sustained_rate"`
	FinalValidation      FinalValidation `json:"final_validation"`
	Samples              []Sample        `json:"samples"`
}

type FinalValidation struct {
	FinalCount  uint64 `json:"final_count"`
	FinalMaxSeq int64  `json:"final_max_seq"`
}

type Sample struct {
	At           time.Time `json:"at"`
	VisibleRows  uint64    `json:"visible_rows"`
	MaxSeq       int64     `json:"max_seq"`
	Backlog      int64     `json:"backlog"`
	P50LatencyMs float64   `json:"p50_latency_ms"`
	P95LatencyMs float64   `json:"p95_latency_ms"`
	P99LatencyMs float64   `json:"p99_latency_ms"`
	MaxLatencyMs float64   `json:"max_latency_ms"`
}

type scenarioSpec struct {
	Name          ScenarioKind
	Duration      time.Duration
	SteadyRate    int
	RampStartRate int
	RampStep      int
	RampEvery     time.Duration
	RampMaxRate   int
	BurstBaseline int
	BurstSpike    int
	BurstDuration time.Duration
	BurstEvery    time.Duration
}

type sourceSpec struct {
	kind         SourceKind
	db           *sql.DB
	dialect      sourceDialect
	sourceConfig gen.CdcFlowConfigSourceConfig
	sourcePeer   *gen.Peer
}

type sourceDialect struct {
	schema            string
	placeholderFormat sq.PlaceholderFormat
	ddl               string
}

type monitor struct {
	mu                sync.Mutex
	samples           []Sample
	lastZeroBacklogAt time.Time
	lastLatencySeq    int64
	latenciesMs       []float64
}

func Run(ctx context.Context, env *localenv.Environment, cfg Config) ([]RunResult, error) {
	if cfg.OutputDir == "" {
		cfg.OutputDir = "benchmarks"
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if err := os.MkdirAll(cfg.OutputDir, 0o755); err != nil {
		return nil, err
	}

	var sources []SourceKind
	switch cfg.Source {
	case "", SourceBoth:
		sources = []SourceKind{SourcePostgres, SourceMySQL}
	case SourcePostgres, SourceMySQL:
		sources = []SourceKind{cfg.Source}
	default:
		return nil, fmt.Errorf("unsupported source %q", cfg.Source)
	}

	results := make([]RunResult, 0, len(sources))
	for _, src := range sources {
		result, err := runOne(ctx, env, cfg, src)
		if err != nil {
			return nil, err
		}
		results = append(results, result)
		if err := persistArtifacts(cfg.OutputDir, result); err != nil {
			return nil, err
		}
	}

	return results, nil
}

func runOne(ctx context.Context, env *localenv.Environment, cfg Config, source SourceKind) (RunResult, error) {
	spec, err := buildSourceSpec(env, source)
	if err != nil {
		return RunResult{}, err
	}
	scenario := resolveScenario(cfg)

	chConn, err := chpkg.Connect(ctx, chpkg.Config{
		Host:     env.ClickHouseHost,
		Port:     int(env.ClickHousePort),
		User:     env.ClickHouseUser,
		Password: env.ClickHousePassword,
		Database: env.ClickHouseDatabase,
	})
	if err != nil {
		return RunResult{}, err
	}
	defer chConn.Close()

	pollConn, err := chpkg.Connect(ctx, chpkg.Config{
		Host:     env.ClickHouseHost,
		Port:     int(env.ClickHousePort),
		User:     env.ClickHouseUser,
		Password: env.ClickHousePassword,
		Database: env.ClickHouseDatabase,
	})
	if err != nil {
		return RunResult{}, err
	}
	defer pollConn.Close()

	tableName := fmt.Sprintf("cdcbench_%s_%s_%d", source, cfg.Scenario, time.Now().UnixNano())
	qualifiedName := fmt.Sprintf("%s.%s", spec.dialect.schema, tableName)

	if _, err := spec.db.ExecContext(ctx, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (%s)`, qualifiedName, spec.dialect.ddl)); err != nil {
		return RunResult{}, fmt.Errorf("create source benchmark table: %w", err)
	}

	sourcePeerID, err := createPeer(ctx, env, spec.sourcePeer)
	if err != nil {
		return RunResult{}, err
	}
	destPeerID, err := createPeer(ctx, env, clickhousePeer(env))
	if err != nil {
		return RunResult{}, err
	}

	flowID, workflowID, err := createFlow(ctx, env, sourcePeerID, destPeerID, qualifiedName, spec.sourceConfig, cfg)
	if err != nil {
		return RunResult{}, err
	}

	cleanup := func() error {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		var errs []error
		_ = flowID
		if env.TemporalClient != nil {
			if err := env.TemporalClient.TerminateWorkflow(cleanupCtx, workflowID, "", "cdcbench cleanup"); err != nil && !strings.Contains(err.Error(), "workflow execution already completed") {
				errs = append(errs, err)
			}
		}
		if _, err := spec.db.ExecContext(cleanupCtx, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, qualifiedName)); err != nil {
			errs = append(errs, err)
		}
		if err := dropClickHouseTable(cleanupCtx, chConn, tableName); err != nil {
			errs = append(errs, err)
		}
		return errors.Join(errs...)
	}
	defer cleanup()

	if err := waitForDestinationTable(ctx, chConn, tableName); err != nil {
		return RunResult{}, err
	}

	state := newWriterState()
	if err := insertWarmupRow(ctx, spec, qualifiedName, state); err != nil {
		return RunResult{}, err
	}
	if err := waitForWarmup(ctx, chConn, tableName, state.maxInsertedSeq.Load()); err != nil {
		return RunResult{}, err
	}

	mon := &monitor{lastZeroBacklogAt: time.Now()}
	pollCtx, cancelPoll := context.WithCancel(ctx)
	defer cancelPoll()
	go pollDestination(pollCtx, pollConn, tableName, cfg.PollInterval, &state.maxInsertedSeq, mon)

	startedAt := time.Now().UTC()
	writerStats, err := runScenario(ctx, spec, qualifiedName, scenario, state, mon)
	if err != nil {
		return RunResult{}, err
	}

	if err := waitForDrain(ctx, chConn, tableName, state.maxInsertedSeq.Load(), mon, cfg.PollInterval); err != nil {
		return RunResult{}, err
	}
	cancelPoll()

	final, err := finalValidation(ctx, chConn, tableName)
	if err != nil {
		return RunResult{}, err
	}
	samples := mon.snapshot()
	finishedAt := time.Now().UTC()

	result := summarize(source, scenario.Name, startedAt, finishedAt, writerStats, final, samples)
	return result, nil
}

func buildSourceSpec(env *localenv.Environment, source SourceKind) (sourceSpec, error) {
	switch source {
	case SourcePostgres:
		return sourceSpec{
			kind: source,
			db:   env.PostgresSQL,
			dialect: sourceDialect{
				schema:            "public",
				placeholderFormat: sq.Dollar,
				ddl: `id BIGINT PRIMARY KEY,
seq BIGINT NOT NULL,
event_ts TIMESTAMP NOT NULL,
updated_at TIMESTAMP NOT NULL,
is_active BOOLEAN NOT NULL,
score DOUBLE PRECISION NOT NULL,
amount NUMERIC(12,2) NOT NULL,
payload TEXT NOT NULL`,
			},
			sourceConfig: &gen.CdcFlowConfig_PostgresSource{PostgresSource: &gen.PostgresSourceConfig{}},
			sourcePeer: &gen.Peer{
				Name: fmt.Sprintf("cdcbench-pg-%d", time.Now().UnixNano()),
				Type: gen.PeerType_POSTGRES,
				Config: &gen.Peer_PostgresConfig{PostgresConfig: &gen.PostgresConfig{
					Host:     env.PostgresHost,
					Port:     env.PostgresPort,
					User:     env.PostgresUser,
					Password: env.PostgresPassword,
					Database: env.PostgresDB,
					SslMode:  "disable",
				}},
			},
		}, nil
	case SourceMySQL:
		return sourceSpec{
			kind: source,
			db:   env.MySQLDB,
			dialect: sourceDialect{
				schema:            env.MySQLDatabase,
				placeholderFormat: sq.Question,
				ddl: `id BIGINT PRIMARY KEY,
seq BIGINT NOT NULL,
event_ts DATETIME(3) NOT NULL,
updated_at DATETIME(3) NOT NULL,
is_active TINYINT(1) NOT NULL,
score DOUBLE NOT NULL,
amount DECIMAL(12,2) NOT NULL,
payload TEXT NOT NULL`,
			},
			sourceConfig: &gen.CdcFlowConfig_MysqlSource{MysqlSource: &gen.MysqlSourceConfig{}},
			sourcePeer: &gen.Peer{
				Name: fmt.Sprintf("cdcbench-mysql-%d", time.Now().UnixNano()),
				Type: gen.PeerType_MYSQL,
				Config: &gen.Peer_MysqlConfig{MysqlConfig: &gen.MysqlConfig{
					Host:     env.MySQLHost,
					Port:     env.MySQLPort,
					User:     env.MySQLUser,
					Password: env.MySQLPassword,
					Database: env.MySQLDatabase,
				}},
			},
		}, nil
	default:
		return sourceSpec{}, fmt.Errorf("unsupported source %q", source)
	}
}

func clickhousePeer(env *localenv.Environment) *gen.Peer {
	return &gen.Peer{
		Name: fmt.Sprintf("cdcbench-clickhouse-%d", time.Now().UnixNano()),
		Type: gen.PeerType_CLICKHOUSE,
		Config: &gen.Peer_ClickhouseConfig{ClickhouseConfig: &gen.ClickhouseConfig{
			Host:     env.ClickHouseHost,
			Port:     env.ClickHousePort,
			User:     env.ClickHouseUser,
			Password: env.ClickHousePassword,
			Database: env.ClickHouseDatabase,
		}},
	}
}

func createPeer(ctx context.Context, env *localenv.Environment, peer *gen.Peer) (string, error) {
	resp, err := env.APIClient.CreatePeer(ctx, &gen.CreatePeerRequest{Peer: peer})
	if err != nil {
		return "", err
	}
	return resp.Id, nil
}

func createFlow(
	ctx context.Context,
	env *localenv.Environment,
	sourcePeerID string,
	destPeerID string,
	qualifiedName string,
	sourceCfg gen.CdcFlowConfigSourceConfig,
	cfg Config,
) (string, string, error) {
	flowCfg := &gen.CdcFlowConfig{
		Tables:       []string{qualifiedName},
		SourceConfig: sourceCfg,
	}
	if cfg.FlushIntervalMs > 0 {
		v := uint32(cfg.FlushIntervalMs)
		flowCfg.FlushIntervalMs = &v
	}
	if cfg.MaxBatchSize > 0 {
		v := uint32(cfg.MaxBatchSize)
		flowCfg.MaxBatchSize = &v
	}

	resp, err := env.APIClient.CreateCDCFlow(ctx, &gen.CreateCDCFlowRequest{
		CdcFlow: &gen.CDCFlow{
			Name:        fmt.Sprintf("cdcbench-%d", time.Now().UnixNano()),
			Source:      sourcePeerID,
			Destination: destPeerID,
			Config:      flowCfg,
		},
	})
	if err != nil {
		return "", "", err
	}

	return resp.Id, server.CdcFlowPrefix + resp.Id, nil
}

func waitForDestinationTable(ctx context.Context, conn driver.Conn, tableName string) error {
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		exists, err := clickhouseTableExists(ctx, conn, tableName)
		if err == nil && exists {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("destination table %s was not created in time", tableName)
}

func clickhouseTableExists(ctx context.Context, conn driver.Conn, tableName string) (bool, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(`EXISTS TABLE "%s"`, tableName))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if !rows.Next() {
		return false, rows.Err()
	}
	var exists uint8
	if err := rows.Scan(&exists); err != nil {
		return false, err
	}
	return exists == 1, rows.Err()
}

type writerState struct {
	maxInsertedSeq atomic.Int64
	insertedCount  atomic.Int64
	totalOps       atomic.Int64
	totalUpdates   atomic.Int64
	totalInserts   atomic.Int64

	recentIDs []int64
}

func newWriterState() *writerState {
	return &writerState{recentIDs: make([]int64, 0, maxRecentIDs)}
}

func insertWarmupRow(ctx context.Context, spec sourceSpec, qualifiedName string, state *writerState) error {
	if err := insertBatch(ctx, spec.db, spec.dialect, qualifiedName, []benchRow{newBenchRow(1)}); err != nil {
		return err
	}
	state.noteInsert(1)
	state.maxInsertedSeq.Store(1)
	return nil
}

func waitForWarmup(ctx context.Context, conn driver.Conn, tableName string, warmupSeq int64) error {
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		maxSeq, err := rawMaxSeq(ctx, conn, tableName)
		if err == nil && maxSeq >= warmupSeq {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	return fmt.Errorf("warmup row did not arrive in destination")
}

type benchRow struct {
	ID        int64
	Seq       int64
	EventTS   time.Time
	UpdatedAt time.Time
	IsActive  bool
	Score     float64
	Amount    string
	Payload   string
}

func newBenchRow(seq int64) benchRow {
	now := time.Now().UTC()
	return benchRow{
		ID:        seq,
		Seq:       seq,
		EventTS:   now,
		UpdatedAt: now,
		IsActive:  seq%2 == 0,
		Score:     float64(seq%1000) / 10,
		Amount:    fmt.Sprintf("%d.%02d", seq%1000, seq%100),
		Payload:   fmt.Sprintf("payload-%d-%s", seq, strings.Repeat("x", 96)),
	}
}

type writerStats struct {
	startedAt       time.Time
	finishedAt      time.Time
	targetWriteRate float64
	totalOps        int64
	totalInserts    int64
	totalUpdates    int64
}

func runScenario(
	ctx context.Context,
	spec sourceSpec,
	qualifiedName string,
	scenario scenarioSpec,
	state *writerState,
	mon *monitor,
) (writerStats, error) {
	stats := writerStats{
		startedAt:       time.Now().UTC(),
		targetWriteRate: scenario.maxTargetRate(),
	}
	nextSeq := int64(2)
	var carry float64

	for elapsed := time.Duration(0); elapsed < scenario.Duration; {
		select {
		case <-ctx.Done():
			return writerStats{}, ctx.Err()
		default:
		}

		window := minDuration(windowSize, scenario.Duration-elapsed)
		rate := scenario.targetRateAt(elapsed)
		carry += float64(rate) * window.Seconds()
		targetOps := int(math.Floor(carry))
		carry -= float64(targetOps)

		if scenario.Name == ScenarioRamp && mon.timeSinceZeroBacklog() >= 2*time.Minute {
			break
		}

		windowStart := time.Now()
		insertCount := int(math.Round(float64(targetOps) * insertRatio))
		updateCount := targetOps - insertCount
		if len(state.recentIDs) == 0 {
			insertCount = targetOps
			updateCount = 0
		}

		rows := make([]benchRow, 0, insertCount)
		for i := 0; i < insertCount; i++ {
			row := newBenchRow(nextSeq)
			rows = append(rows, row)
			nextSeq++
		}

		if err := executeWindow(ctx, spec, qualifiedName, state, rows, updateCount); err != nil {
			return writerStats{}, err
		}

		elapsed += window
		sleepFor := window - time.Since(windowStart)
		if sleepFor > 0 {
			select {
			case <-ctx.Done():
				return writerStats{}, ctx.Err()
			case <-time.After(sleepFor):
			}
		}
	}

	stats.finishedAt = time.Now().UTC()
	stats.totalOps = state.totalOps.Load()
	stats.totalInserts = state.totalInserts.Load()
	stats.totalUpdates = state.totalUpdates.Load()
	return stats, nil
}

func executeWindow(
	ctx context.Context,
	spec sourceSpec,
	qualifiedName string,
	state *writerState,
	rows []benchRow,
	updateCount int,
) error {
	tx, err := spec.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if len(rows) > 0 {
		if err := insertBatchTx(ctx, tx, spec.dialect, qualifiedName, rows); err != nil {
			tx.Rollback()
			return err
		}
	}
	for i := 0; i < updateCount; i++ {
		id := state.pickRecentID(i)
		if id == 0 {
			break
		}
		if err := updateRowTx(ctx, tx, spec.dialect, qualifiedName, id); err != nil {
			tx.Rollback()
			return err
		}
	}
	if err := tx.Commit(); err != nil {
		return err
	}

	for _, row := range rows {
		state.noteInsert(row.ID)
	}
	state.totalInserts.Add(int64(len(rows)))
	state.totalUpdates.Add(int64(updateCount))
	state.totalOps.Add(int64(len(rows) + updateCount))
	if len(rows) > 0 {
		state.maxInsertedSeq.Store(rows[len(rows)-1].Seq)
		state.insertedCount.Add(int64(len(rows)))
	}

	return nil
}

func insertBatch(ctx context.Context, db *sql.DB, dialect sourceDialect, qualifiedName string, rows []benchRow) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := insertBatchTx(ctx, tx, dialect, qualifiedName, rows); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func insertBatchTx(ctx context.Context, exec sqlutil.ExecContexter, dialect sourceDialect, qualifiedName string, rows []benchRow) error {
	if len(rows) == 0 {
		return nil
	}
	builder := sq.StatementBuilder.
		PlaceholderFormat(dialect.placeholderFormat).
		Insert(qualifiedName).
		Columns("id", "seq", "event_ts", "updated_at", "is_active", "score", "amount", "payload")
	for _, row := range rows {
		builder = builder.Values(row.ID, row.Seq, row.EventTS, row.UpdatedAt, row.IsActive, row.Score, row.Amount, row.Payload)
	}
	query, args, err := builder.ToSql()
	if err != nil {
		return err
	}
	_, err = exec.ExecContext(ctx, query, args...)
	return err
}

func updateRowTx(ctx context.Context, exec sqlutil.ExecContexter, dialect sourceDialect, qualifiedName string, id int64) error {
	query, args, err := sq.StatementBuilder.
		PlaceholderFormat(dialect.placeholderFormat).
		Update(qualifiedName).
		Set("updated_at", time.Now().UTC()).
		Set("score", float64(id%1000)/7.0).
		Set("payload", fmt.Sprintf("updated-%d-%s", id, strings.Repeat("u", 96))).
		Where(sq.Eq{"id": id}).
		ToSql()
	if err != nil {
		return err
	}
	_, err = exec.ExecContext(ctx, query, args...)
	return err
}

func (s *writerState) noteInsert(id int64) {
	if len(s.recentIDs) == maxRecentIDs {
		copy(s.recentIDs, s.recentIDs[1:])
		s.recentIDs[len(s.recentIDs)-1] = id
		return
	}
	s.recentIDs = append(s.recentIDs, id)
}

func (s *writerState) pickRecentID(i int) int64 {
	if len(s.recentIDs) == 0 {
		return 0
	}
	idx := len(s.recentIDs) - 1 - (i % len(s.recentIDs))
	return s.recentIDs[idx]
}

func pollDestination(
	ctx context.Context,
	conn driver.Conn,
	tableName string,
	interval time.Duration,
	maxInsertedSeq *atomic.Int64,
	mon *monitor,
) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	record := func() {
		snap, err := loadSnapshot(ctx, conn, tableName, mon)
		if err != nil {
			maxSeq, seqErr := rawMaxSeq(ctx, conn, tableName)
			if seqErr != nil {
				return
			}
			snap = Sample{
				At:          time.Now().UTC(),
				VisibleRows: uint64(max(0, maxSeq)),
				MaxSeq:      maxSeq,
			}
		}
		snap.Backlog = max(0, maxInsertedSeq.Load()-snap.MaxSeq)
		mon.addSample(snap)
	}

	record()
	for {
		select {
		case <-ctx.Done():
			record()
			return
		case <-ticker.C:
			record()
		}
	}
}

func waitForDrain(ctx context.Context, conn driver.Conn, tableName string, maxSeq int64, mon *monitor, pollInterval time.Duration) error {
	_ = mon
	deadline := time.Now().Add(recoveryTimeout)
	for time.Now().Before(deadline) {
		visibleSeq, err := rawMaxSeq(ctx, conn, tableName)
		if err == nil && visibleSeq >= maxSeq {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
	return fmt.Errorf("destination did not drain within %s", recoveryTimeout)
}

func loadSnapshot(ctx context.Context, conn driver.Conn, tableName string, mon *monitor) (Sample, error) {
	snap := Sample{At: time.Now().UTC()}
	maxSeq, err := rawMaxSeq(ctx, conn, tableName)
	if err != nil {
		return Sample{}, err
	}
	snap.MaxSeq = maxSeq
	snap.VisibleRows = uint64(max(0, maxSeq))
	if err := fillLatencyStats(ctx, conn, tableName, maxSeq, &snap, mon); err != nil {
		return Sample{}, err
	}
	return snap, nil
}

func finalValidation(ctx context.Context, conn driver.Conn, tableName string) (FinalValidation, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(`SELECT count(), ifNull(max(seq), toInt64(0)) FROM "%s" FINAL`, tableName))
	if err != nil {
		return FinalValidation{}, err
	}
	defer rows.Close()

	if !rows.Next() {
		return FinalValidation{}, rows.Err()
	}

	var out FinalValidation
	if err := rows.Scan(&out.FinalCount, &out.FinalMaxSeq); err != nil {
		return FinalValidation{}, err
	}
	return out, rows.Err()
}

func rawMaxSeq(ctx context.Context, conn driver.Conn, tableName string) (int64, error) {
	rows, err := conn.Query(ctx, fmt.Sprintf(`SELECT ifNull(max(seq), toInt64(0)) FROM "%s"`, tableName))
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	if !rows.Next() {
		return 0, rows.Err()
	}

	var maxSeq int64
	if err := rows.Scan(&maxSeq); err != nil {
		return 0, err
	}
	return maxSeq, rows.Err()
}

func fillLatencyStats(ctx context.Context, conn driver.Conn, tableName string, maxSeq int64, snap *Sample, mon *monitor) error {
	fromSeq := mon.lastSeenLatencySeq()
	if maxSeq > fromSeq {
		rows, err := conn.Query(ctx, fmt.Sprintf(`
SELECT seq, event_ts, _ingested_at
FROM "%s"
WHERE seq > ? AND seq <= ?
ORDER BY seq`, tableName), fromSeq, maxSeq)
		if err != nil {
			return err
		}
		defer rows.Close()

		var (
			newLatencies []float64
			highestSeq   = fromSeq
		)
		for rows.Next() {
			var (
				seq        int64
				eventTS    time.Time
				ingestedAt time.Time
			)
			if err := rows.Scan(&seq, &eventTS, &ingestedAt); err != nil {
				return err
			}
			newLatencies = append(newLatencies, ingestedAt.Sub(eventTS).Seconds()*1000)
			if seq > highestSeq {
				highestSeq = seq
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(newLatencies) > 0 {
			mon.recordLatencies(highestSeq, newLatencies)
		}
	}

	snap.P50LatencyMs, snap.P95LatencyMs, snap.P99LatencyMs, snap.MaxLatencyMs = mon.latencyStats()
	return nil
}

func dropClickHouseTable(ctx context.Context, conn driver.Conn, tableName string) error {
	return conn.Exec(ctx, fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, tableName))
}

func (m *monitor) addSample(sample Sample) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if sample.Backlog == 0 {
		m.lastZeroBacklogAt = sample.At
	}
	m.samples = append(m.samples, sample)
}

func (m *monitor) lastSeenLatencySeq() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastLatencySeq
}

func (m *monitor) recordLatencies(lastSeq int64, latencies []float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lastSeq > m.lastLatencySeq {
		m.lastLatencySeq = lastSeq
	}
	m.latenciesMs = append(m.latenciesMs, latencies...)
}

func (m *monitor) latencyStats() (float64, float64, float64, float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.latenciesMs) == 0 {
		return 0, 0, 0, 0
	}

	sorted := make([]float64, len(m.latenciesMs))
	copy(sorted, m.latenciesMs)
	sort.Float64s(sorted)

	return percentile(sorted, 0.50), percentile(sorted, 0.95), percentile(sorted, 0.99), sorted[len(sorted)-1]
}

func (m *monitor) timeSinceZeroBacklog() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lastZeroBacklogAt.IsZero() {
		return 0
	}
	return time.Since(m.lastZeroBacklogAt)
}

func (m *monitor) snapshot() []Sample {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Sample, len(m.samples))
	copy(out, m.samples)
	return out
}

func resolveScenario(cfg Config) scenarioSpec {
	switch cfg.Scenario {
	case ScenarioRamp:
		spec := scenarioSpec{
			Name:          ScenarioRamp,
			Duration:      10 * time.Minute,
			RampStartRate: 100,
			RampStep:      100,
			RampEvery:     45 * time.Second,
			RampMaxRate:   1500,
		}
		if cfg.Duration > 0 {
			spec.Duration = cfg.Duration
		}
		if cfg.RateOverride > 0 {
			spec.RampStartRate = cfg.RateOverride
		}
		return spec
	case ScenarioBurst:
		spec := scenarioSpec{
			Name:          ScenarioBurst,
			Duration:      8 * time.Minute,
			BurstBaseline: 250,
			BurstSpike:    1000,
			BurstDuration: 20 * time.Second,
			BurstEvery:    90 * time.Second,
		}
		if cfg.Duration > 0 {
			spec.Duration = cfg.Duration
		}
		if cfg.RateOverride > 0 {
			spec.BurstBaseline = cfg.RateOverride
			if spec.BurstSpike < spec.BurstBaseline {
				spec.BurstSpike = spec.BurstBaseline
			}
		}
		return spec
	default:
		spec := scenarioSpec{
			Name:       ScenarioSteady,
			Duration:   5 * time.Minute,
			SteadyRate: 250,
		}
		if cfg.Duration > 0 {
			spec.Duration = cfg.Duration
		}
		if cfg.RateOverride > 0 {
			spec.SteadyRate = cfg.RateOverride
		}
		return spec
	}
}

func (s scenarioSpec) targetRateAt(elapsed time.Duration) int {
	switch s.Name {
	case ScenarioRamp:
		steps := int(elapsed / s.RampEvery)
		rate := s.RampStartRate + steps*s.RampStep
		if rate > s.RampMaxRate {
			return s.RampMaxRate
		}
		return rate
	case ScenarioBurst:
		if elapsed%s.BurstEvery < s.BurstDuration {
			return s.BurstSpike
		}
		return s.BurstBaseline
	default:
		return s.SteadyRate
	}
}

func (s scenarioSpec) maxTargetRate() float64 {
	switch s.Name {
	case ScenarioRamp:
		return float64(s.RampMaxRate)
	case ScenarioBurst:
		return float64(s.BurstSpike)
	default:
		return float64(s.SteadyRate)
	}
}

func summarize(
	source SourceKind,
	scenario ScenarioKind,
	startedAt time.Time,
	finishedAt time.Time,
	writer writerStats,
	final FinalValidation,
	samples []Sample,
) RunResult {
	result := RunResult{
		Source:               source,
		Scenario:             scenario,
		StartedAt:            startedAt,
		FinishedAt:           finishedAt,
		TargetWriteRate:      writer.targetWriteRate,
		TotalOps:             writer.totalOps,
		TotalInserts:         writer.totalInserts,
		TotalUpdates:         writer.totalUpdates,
		FinalValidation:      final,
		Samples:              samples,
		HighestSustainedRate: writer.targetWriteRate,
	}

	writeSeconds := finishedAt.Sub(startedAt).Seconds()
	if writeSeconds > 0 {
		result.AchievedWriteRate = float64(writer.totalOps) / writeSeconds
	}

	if len(samples) > 0 {
		last := samples[len(samples)-1]
		result.P50LatencyMs = last.P50LatencyMs
		result.P95LatencyMs = last.P95LatencyMs
		result.P99LatencyMs = last.P99LatencyMs
		result.MaxLatencyMs = last.MaxLatencyMs
		result.MaxObservedSeq = last.MaxSeq
	}

	var maxBacklog int64
	var maxVisibleRate float64
	for i := range samples {
		if samples[i].Backlog > maxBacklog {
			maxBacklog = samples[i].Backlog
		}
		if i == 0 {
			continue
		}
		deltaRows := float64(samples[i].VisibleRows - samples[i-1].VisibleRows)
		deltaSeconds := samples[i].At.Sub(samples[i-1].At).Seconds()
		if deltaSeconds > 0 {
			rate := deltaRows / deltaSeconds
			if rate > maxVisibleRate {
				maxVisibleRate = rate
			}
		}
	}
	result.MaxBacklog = maxBacklog
	result.VisibleIngestRate = maxVisibleRate
	result.RecoveryTimeMs = computeRecoveryMillis(samples)
	if result.VisibleIngestRate > 0 {
		result.HighestSustainedRate = result.VisibleIngestRate
	}
	if result.MaxBacklog == 0 {
		result.HighestSustainedRate = result.AchievedWriteRate
	}

	return result
}

func computeRecoveryMillis(samples []Sample) int64 {
	var backlogPeakAt time.Time
	var peakSeen bool
	for _, sample := range samples {
		if sample.Backlog > 0 && !peakSeen {
			backlogPeakAt = sample.At
			peakSeen = true
		}
		if peakSeen && sample.Backlog == 0 {
			return sample.At.Sub(backlogPeakAt).Milliseconds()
		}
	}
	return 0
}

func persistArtifacts(outputDir string, result RunResult) error {
	base := fmt.Sprintf("%s-%s-%s", result.Source, result.Scenario, result.StartedAt.Format("20060102-150405"))

	jsonBytes, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outputDir, base+".json"), jsonBytes, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outputDir, base+".md"), []byte(markdownSummary(result)), 0o644); err != nil {
		return err
	}

	return nil
}

func markdownSummary(result RunResult) string {
	return fmt.Sprintf(`# CDC Benchmark Summary

- Source: %s
- Scenario: %s
- Started: %s
- Finished: %s
- Target write rate: %.1f rows/sec
- Achieved write rate: %.1f ops/sec
- Visible ingest rate: %.1f rows/sec
- Total ops: %d
- Total inserts: %d
- Total updates: %d
- p50 latency: %.1f ms
- p95 latency: %.1f ms
- p99 latency: %.1f ms
- Max latency: %.1f ms
- Max observed seq: %d
- Max backlog: %d
- Recovery time: %d ms
- Highest sustained rate: %.1f rows/sec
- FINAL count: %d
- FINAL max seq: %d
`,
		result.Source,
		result.Scenario,
		result.StartedAt.Format(time.RFC3339),
		result.FinishedAt.Format(time.RFC3339),
		result.TargetWriteRate,
		result.AchievedWriteRate,
		result.VisibleIngestRate,
		result.TotalOps,
		result.TotalInserts,
		result.TotalUpdates,
		result.P50LatencyMs,
		result.P95LatencyMs,
		result.P99LatencyMs,
		result.MaxLatencyMs,
		result.MaxObservedSeq,
		result.MaxBacklog,
		result.RecoveryTimeMs,
		result.HighestSustainedRate,
		result.FinalValidation.FinalCount,
		result.FinalValidation.FinalMaxSeq,
	)
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	if p <= 0 {
		return sorted[0]
	}
	if p >= 1 {
		return sorted[len(sorted)-1]
	}

	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
