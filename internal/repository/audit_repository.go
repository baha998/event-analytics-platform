package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rs/zerolog"
)

// AuditEntry represents an admin action audit record.
type AuditEntry struct {
	Timestamp time.Time
	Username  string
	Role      string
	Action    string
	Resource  string
	IP        string
	Status    string
	Details   string
}

// AuditRepository writes audit records to ClickHouse.
type AuditRepository struct {
	conn clickhouse.Conn
	log  zerolog.Logger
}

// NewAuditRepository constructs an AuditRepository.
func NewAuditRepository(conn clickhouse.Conn, log zerolog.Logger) *AuditRepository {
	return &AuditRepository{
		conn: conn,
		log:  log,
	}
}

// EnsureSchema creates the audit table if it does not exist.
func (r *AuditRepository) EnsureSchema(ctx context.Context) error {
	const ddl = `
		CREATE TABLE IF NOT EXISTS admin_audit
		(
			timestamp DateTime,
			username String,
			role String,
			action String,
			resource String,
			ip String,
			status String,
			details String
		)
		ENGINE = MergeTree()
		PARTITION BY toYYYYMM(timestamp)
		ORDER BY (timestamp, username, action)
		SETTINGS index_granularity = 8192
	`

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := r.conn.Exec(ctx, ddl); err != nil {
		return fmt.Errorf("failed to ensure audit table: %w", err)
	}

	return nil
}

// Insert writes an audit entry.
func (r *AuditRepository) Insert(ctx context.Context, entry *AuditEntry) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	query := `
		INSERT INTO admin_audit
			(timestamp, username, role, action, resource, ip, status, details)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`

	if err := r.conn.Exec(ctx, query,
		entry.Timestamp,
		entry.Username,
		entry.Role,
		entry.Action,
		entry.Resource,
		entry.IP,
		entry.Status,
		entry.Details,
	); err != nil {
		r.log.Error().Err(err).
			Str("action", entry.Action).
			Str("username", entry.Username).
			Msg("failed to write audit entry")
		return fmt.Errorf("failed to insert audit entry: %w", err)
	}
	return nil
}
