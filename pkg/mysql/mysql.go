package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"peerdb-playground/gen"
	"strconv"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"
)

type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
}

func driverConfig(cfg Config) *mysqldriver.Config {
	return &mysqldriver.Config{
		User:                 cfg.User,
		Passwd:               cfg.Password,
		Net:                  "tcp",
		Addr:                 net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port)),
		DBName:               cfg.Database,
		ParseTime:            true,
		AllowNativePasswords: true,
		CheckConnLiveness:    true,
	}
}

func ConnectionString(cfg Config) string {
	return driverConfig(cfg).FormatDSN()
}

func Connect(ctx context.Context, cfg Config) (*sql.DB, error) {
	db, err := sql.Open("mysql", ConnectionString(cfg))
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping mysql: %w", err)
	}

	return db, nil
}

func ConfigFromProto(cfg *gen.MysqlConfig) Config {
	return Config{
		Host:     cfg.Host,
		Port:     int(cfg.Port),
		User:     cfg.User,
		Password: cfg.Password,
		Database: cfg.Database,
	}
}

func ConnectFromProto(ctx context.Context, cfg *gen.MysqlConfig) (*sql.DB, error) {
	return Connect(ctx, ConfigFromProto(cfg))
}

func ValidateCDCPrerequisites(ctx context.Context, db *sql.DB) error {
	var logBin any
	var gtidMode string
	var binlogFormat string
	var binlogRowImage string

	err := db.QueryRowContext(ctx, `
		SELECT @@GLOBAL.log_bin, @@GLOBAL.gtid_mode, @@GLOBAL.binlog_format, @@GLOBAL.binlog_row_image
	`).Scan(&logBin, &gtidMode, &binlogFormat, &binlogRowImage)
	if err != nil {
		return fmt.Errorf("failed to load mysql cdc variables: %w", err)
	}

	if !mysqlBoolEnabled(logBin) {
		return fmt.Errorf("mysql cdc requires @@GLOBAL.log_bin=ON")
	}
	if !strings.EqualFold(gtidMode, "ON") {
		return fmt.Errorf("mysql cdc requires @@GLOBAL.gtid_mode=ON")
	}
	if !strings.EqualFold(binlogFormat, "ROW") {
		return fmt.Errorf("mysql cdc requires @@GLOBAL.binlog_format=ROW")
	}
	if !strings.EqualFold(binlogRowImage, "FULL") {
		return fmt.Errorf("mysql cdc requires @@GLOBAL.binlog_row_image=FULL")
	}

	return nil
}

func LoadExecutedGTIDSet(ctx context.Context, db *sql.DB) (string, error) {
	var gtid string
	if err := db.QueryRowContext(ctx, `SELECT @@GLOBAL.gtid_executed`).Scan(&gtid); err != nil {
		return "", fmt.Errorf("failed to load mysql executed gtid set: %w", err)
	}
	return gtid, nil
}

func LoadMasterStatus(ctx context.Context, db *sql.DB) (string, uint32, string, error) {
	var file string
	var position uint32
	var binlogDoDB sql.NullString
	var binlogIgnoreDB sql.NullString
	var executedGTIDSet sql.NullString

	err := db.QueryRowContext(ctx, `SHOW MASTER STATUS`).Scan(
		&file,
		&position,
		&binlogDoDB,
		&binlogIgnoreDB,
		&executedGTIDSet,
	)
	if err != nil {
		return "", 0, "", fmt.Errorf("failed to load mysql master status: %w", err)
	}

	return file, position, executedGTIDSet.String, nil
}

func mysqlBoolEnabled(v any) bool {
	switch value := v.(type) {
	case bool:
		return value
	case int64:
		return value != 0
	case []byte:
		return strings.EqualFold(string(value), "ON") || string(value) == "1"
	case string:
		return strings.EqualFold(value, "ON") || value == "1"
	default:
		return strings.EqualFold(fmt.Sprint(v), "ON") || fmt.Sprint(v) == "1"
	}
}
