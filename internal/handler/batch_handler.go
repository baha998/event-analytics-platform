package handler

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/internal/batch"
	"devops-fall2025-23b031152/internal/processor"
	"devops-fall2025-23b031152/internal/repository"
	"devops-fall2025-23b031152/internal/storage"
	"devops-fall2025-23b031152/pkg/metrics"
)

// BatchHandler handles batch processing REST API endpoints
type BatchHandler struct {
	batchProcessor *processor.BatchProcessor
	exporter       *batch.Exporter
	repo           *repository.ClickHouseRepository
	minioClient    *storage.MinIOClient
	auditRepo      *repository.AuditRepository
	log            zerolog.Logger
}

// NewBatchHandler creates a new batch handler
func NewBatchHandler(
	exporter *batch.Exporter,
	batchProcessor *processor.BatchProcessor,
	repo *repository.ClickHouseRepository,
	minioClient *storage.MinIOClient,
	auditRepo *repository.AuditRepository,
	log zerolog.Logger,
) *BatchHandler {
	return &BatchHandler{
		exporter:       exporter,
		batchProcessor: batchProcessor,
		repo:           repo,
		minioClient:    minioClient,
		auditRepo:      auditRepo,
		log:            log,
	}
}

// ExportBatchEvents handles POST /api/batch/export (admin only)
// Triggers ExportLastDayEvents() and returns export statistics
func (h *BatchHandler) ExportBatchEvents(c *gin.Context) {
	startTime := time.Now()

	h.log.Info().Msg("Starting batch export")

	// Call the exporter to export yesterday's events
	yesterday := time.Now().AddDate(0, 0, -1).Format("2006-01-02")

	// Track duration with Prometheus
	defer func() {
		duration := time.Since(startTime).Seconds()
		metrics.BatchExportDurationSeconds.Observe(duration)
	}()

	err := h.exporter.ExportLastDayEvents()
	if err != nil {
		h.log.Error().Err(err).Msg("Failed to export batch events")
		h.writeAudit(c, "batch_export", "/api/batch/export", "failed", err.Error())
		respondError(c, http.StatusInternalServerError, "export_failed", "Failed to export events", err.Error())
		return
	}

	// Get row count from yesterday's data (query ClickHouse)
	exportedRows, err := h.getEventCountForDate(yesterday)
	if err != nil {
		h.log.Warn().Err(err).Msg("Failed to get exact row count, using estimate")
		exportedRows = 0 // Use 0 if we can't get the count
	}

	// Increment Prometheus counter
	if exportedRows > 0 {
		metrics.BatchExportRowsTotal.Add(float64(exportedRows))
	}

	fileName := fmt.Sprintf("date=%s/events.parquet", yesterday)

	h.log.Info().
		Int64("exported_rows", exportedRows).
		Str("file", fileName).
		Msg("Batch export completed successfully")

	h.writeAudit(c, "batch_export", "/api/batch/export", "success",
		fmt.Sprintf("rows=%d file=%s", exportedRows, fileName))

	respondSuccess(c, http.StatusOK, gin.H{
		"status":        "success",
		"exported_rows": exportedRows,
		"file":          fileName,
	})
}

// AggregateBatchData handles POST /api/batch/aggregate (admin only)
// Runs batch aggregation on yesterday's data
func (h *BatchHandler) AggregateBatchData(c *gin.Context) {
	h.log.Info().Msg("Starting batch aggregation")

	yesterday := time.Now().UTC().AddDate(0, 0, -1)
	if err := h.batchProcessor.ProcessDailyAggregation(c.Request.Context(), yesterday, "click_count"); err != nil {
		respondError(c, 500, "aggregation_failed", "Aggregation failed", nil)
		return
	}
	respondSuccess(c, 200, gin.H{"status": "success", "metric": "click_count_daily_summary"})
}

// CleanupTempFiles handles POST /api/batch/cleanup (admin only)
// Deletes temporary local files
func (h *BatchHandler) CleanupTempFiles(c *gin.Context) {
	h.log.Info().Msg("Starting temp file cleanup")

	tempDir := "/tmp"
	pattern := "events-*.parquet"

	deletedCount, err := h.cleanupLocalFiles(tempDir, pattern)
	if err != nil {
		h.log.Error().Err(err).Msg("Failed to cleanup temp files")
		h.writeAudit(c, "cleanup_temp", "/api/batch/cleanup", "failed", err.Error())
		respondError(c, http.StatusInternalServerError, "cleanup_failed", "Failed to cleanup temp files", err.Error())
		return
	}

	h.log.Info().Int("deleted_files", deletedCount).Msg("Temp file cleanup completed")

	h.writeAudit(c, "cleanup_temp", "/api/batch/cleanup", "success",
		fmt.Sprintf("deleted=%d", deletedCount))

	respondSuccess(c, http.StatusOK, gin.H{
		"status":        "success",
		"deleted_files": deletedCount,
	})
}

// CleanupOldEvents handles POST /api/cleanup/events?days=30 (admin only)
// Deletes ClickHouse events older than N days
func (h *BatchHandler) CleanupOldEvents(c *gin.Context) {
	daysParam := c.DefaultQuery("days", "30")
	days, err := strconv.Atoi(daysParam)
	if err != nil || days < 1 {
		respondError(c, http.StatusBadRequest, "invalid_days", "Invalid days parameter", nil)
		return
	}

	h.log.Info().Int("days", days).Msg("Starting old events cleanup")

	cutoffDate := time.Now().AddDate(0, 0, -days)

	// Delete events older than cutoff date
	deletedRows, err := h.deleteOldEvents(c.Request.Context(), cutoffDate)
	if err != nil {
		h.log.Error().Err(err).Msg("Failed to delete old events")
		h.writeAudit(c, "cleanup_events", "/api/cleanup/events", "failed", err.Error())
		respondError(c, http.StatusInternalServerError, "cleanup_failed", "Failed to delete old events", err.Error())
		return
	}

	h.log.Info().
		Int("days", days).
		Int64("deleted_rows", deletedRows).
		Msg("Old events cleanup completed")

	h.writeAudit(c, "cleanup_events", "/api/cleanup/events", "success",
		fmt.Sprintf("days=%d rows=%d", days, deletedRows))

	respondSuccess(c, http.StatusOK, gin.H{
		"status":       "success",
		"deleted_rows": deletedRows,
	})
}

// CleanupMinIOFiles handles POST /api/cleanup/minio?days=30 (admin only)
// Deletes Parquet files from MinIO older than N days
func (h *BatchHandler) CleanupMinIOFiles(c *gin.Context) {
	daysParam := c.DefaultQuery("days", "30")
	days, err := strconv.Atoi(daysParam)
	if err != nil || days < 1 {
		respondError(c, http.StatusBadRequest, "invalid_days", "Invalid days parameter", nil)
		return
	}

	h.log.Info().Int("days", days).Msg("Starting MinIO files cleanup")

	cutoffDate := time.Now().AddDate(0, 0, -days)

	// Delete old Parquet files from MinIO
	deletedObjects, err := h.deleteOldMinIOFiles(c.Request.Context(), cutoffDate)
	if err != nil {
		h.log.Error().Err(err).Msg("Failed to delete old MinIO files")
		h.writeAudit(c, "cleanup_minio", "/api/cleanup/minio", "failed", err.Error())
		respondError(c, http.StatusInternalServerError, "cleanup_failed", "Failed to delete old MinIO files", err.Error())
		return
	}

	h.log.Info().
		Int("days", days).
		Int("deleted_objects", deletedObjects).
		Msg("MinIO files cleanup completed")

	h.writeAudit(c, "cleanup_minio", "/api/cleanup/minio", "success",
		fmt.Sprintf("days=%d objects=%d", days, deletedObjects))

	respondSuccess(c, http.StatusOK, gin.H{
		"status":          "success",
		"deleted_objects": deletedObjects,
	})
}

// Helper methods

func (h *BatchHandler) writeAudit(c *gin.Context, action, resource, status, details string) {
	if h.auditRepo == nil {
		return
	}

	username := c.GetString("username")
	role := c.GetString("role")

	entry := &repository.AuditEntry{
		Timestamp: time.Now(),
		Username:  username,
		Role:      role,
		Action:    action,
		Resource:  resource,
		IP:        c.ClientIP(),
		Status:    status,
		Details:   details,
	}

	if err := h.auditRepo.Insert(context.Background(), entry); err != nil {
		h.log.Error().Err(err).Msg("Failed to write audit entry")
	}
}

func (h *BatchHandler) getEventCountForDate(date string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	count, err := h.repo.CountEventsOnDate(ctx, date)
	return int64(count), err
}

func (h *BatchHandler) cleanupLocalFiles(dir, pattern string) (int, error) {
	matches, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		return 0, fmt.Errorf("failed to glob files: %w", err)
	}

	deletedCount := 0
	for _, file := range matches {
		if err := os.Remove(file); err != nil {
			h.log.Warn().Err(err).Str("file", file).Msg("Failed to delete file")
			continue
		}
		deletedCount++
	}

	return deletedCount, nil
}

func (h *BatchHandler) deleteOldEvents(ctx context.Context, cutoffDate time.Time) (int64, error) {
	count, err := h.repo.DeleteEventsBefore(ctx, cutoffDate)
	return int64(count), err
}
func (h *BatchHandler) deleteOldMinIOFiles(ctx context.Context, cutoffDate time.Time) (int, error) {
	return h.minioClient.DeleteBefore(ctx, "events", cutoffDate)
}
func (h *BatchHandler) Optimize(c *gin.Context) {
	if err := h.repo.Optimize(c.Request.Context()); err != nil {
		respondError(c, 500, "optimize_failed", "Optimization failed", nil)
		return
	}
	respondSuccess(c, 200, gin.H{"status": "success"})
}
