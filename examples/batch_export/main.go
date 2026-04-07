package main

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/internal/batch"
	"devops-fall2025-23b031152/internal/storage"
)

func main() {
	// Initialize logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Connect to ClickHouse
	dsn := "clickhouse://default:clickhouse123@localhost:9000/analytics"
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer db.Close()

	// Ping database
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping ClickHouse: %v", err)
	}
	logger.Info().Msg("✓ Connected to ClickHouse")

	// Initialize MinIO client
	minioClient, err := storage.NewMinIOClient(storage.MinIOConfig{
		Endpoint:        "localhost:9002",
		AccessKeyID:     "admin",
		SecretAccessKey: "admin123",
		UseSSL:          false,
	}, logger)
	if err != nil {
		log.Fatalf("Failed to create MinIO client: %v", err)
	}
	logger.Info().Msg("✓ Connected to MinIO")

	// Create exporter
	exporter := batch.NewExporter(db, minioClient, logger)

	// Example 1: Export events from a specific date
	date := "2025-12-13"
	logger.Info().Str("date", date).Msg("Exporting events for specific date")
	if err := exporter.ExportDailyEvents(date); err != nil {
		log.Printf("ERROR: Failed to export events for %s: %v", date, err)
	} else {
		logger.Info().Str("date", date).Msg("✓ Successfully exported events")
	}

	// Example 2: Export events from yesterday
	logger.Info().Msg("Exporting events from yesterday")
	if err := exporter.ExportLastDayEvents(); err != nil {
		log.Printf("ERROR: Failed to export yesterday's events: %v", err)
	} else {
		logger.Info().Msg("✓ Successfully exported yesterday's events")
	}

	// Example 3: Export date range
	logger.Info().Msg("Exporting date range")
	if err := exporter.ExportDateRange("2025-12-10", "2025-12-13"); err != nil {
		log.Printf("ERROR: Failed to export date range: %v", err)
	} else {
		logger.Info().Msg("✓ Successfully exported date range")
	}

	logger.Info().Msg("Export job completed")
	logger.Info().Msg("Check MinIO console at http://localhost:9001 for uploaded files")
}
