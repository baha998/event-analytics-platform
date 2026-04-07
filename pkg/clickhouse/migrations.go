package clickhouse

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rs/zerolog"
)

// MigrationRunner handles database migrations
type MigrationRunner struct {
	repo *Repository
	log  zerolog.Logger
}

// NewMigrationRunner creates a new migration runner
func NewMigrationRunner(repo *Repository, log zerolog.Logger) *MigrationRunner {
	return &MigrationRunner{
		repo: repo,
		log:  log,
	}
}

// RunMigration executes a migration SQL file
func (m *MigrationRunner) RunMigration(ctx context.Context, migrationPath string) error {
	sql, err := os.ReadFile(migrationPath)
	if err != nil {
		return fmt.Errorf("failed to read migration file: %w", err)
	}

	m.log.Info().Str("migration", filepath.Base(migrationPath)).Msg("Running migration")

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	if err := m.repo.conn.Exec(ctx, string(sql)); err != nil {
		return fmt.Errorf("failed to execute migration: %w", err)
	}

	m.log.Info().Str("migration", filepath.Base(migrationPath)).Msg("Migration completed")
	return nil
}

// RunAllMigrations runs all migrations in the migrations directory
func (m *MigrationRunner) RunAllMigrations(ctx context.Context, migrationsDir string) error {
	migrations := []string{
		"001_create_events_table.sql",
		"002_create_metrics_table.sql",
	}

	for _, migration := range migrations {
		migrationPath := filepath.Join(migrationsDir, migration)
		if err := m.RunMigration(ctx, migrationPath); err != nil {
			return fmt.Errorf("migration %s failed: %w", migration, err)
		}
	}

	m.log.Info().Int("count", len(migrations)).Msg("All migrations completed")
	return nil
}
