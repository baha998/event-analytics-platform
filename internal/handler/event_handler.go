package handler

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/internal/processor"
	"devops-fall2025-23b031152/internal/repository"
	"devops-fall2025-23b031152/internal/service"
)

// EventHandler handles HTTP requests for events
type EventHandler struct {
	service        *service.EventService
	repo           *repository.ClickHouseRepository
	auditRepo      *repository.AuditRepository
	batchProcessor *processor.BatchProcessor
	validate       *validator.Validate
	log            zerolog.Logger
}

// NewEventHandler creates a new event handler
func NewEventHandler(
	eventService *service.EventService,
	repo *repository.ClickHouseRepository,
	batchProcessor *processor.BatchProcessor,
	auditRepo *repository.AuditRepository,
	log zerolog.Logger,
) *EventHandler {
	return &EventHandler{
		service:        eventService,
		repo:           repo,
		batchProcessor: batchProcessor,
		auditRepo:      auditRepo,
		validate:       validator.New(),
		log:            log,
	}
}

// CreateSource handles POST /api/sources
func (h *EventHandler) CreateSource(c *gin.Context) {
	var req repository.SourceRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request body", err.Error())
		return
	}

	if err := h.validate.Struct(req); err != nil {
		respondError(c, http.StatusBadRequest, "validation_error", "Validation failed", err.Error())
		return
	}

	if err := h.service.CreateSource(c.Request.Context(), &req); err != nil {
		h.log.Error().Err(err).Msg("Failed to create source")
		h.writeAudit(c, "create_source", "/api/sources", "failed", err.Error())
		respondError(c, http.StatusInternalServerError, "create_source_failed", "Failed to create source", nil)
		return
	}

	h.writeAudit(c, "create_source", "/api/sources", "success", fmt.Sprintf("topic=%s count=%d", req.Topic, req.Count))

	respondSuccess(c, http.StatusAccepted, gin.H{
		"message": "Source creation initiated",
		"topic":   req.Topic,
		"count":   req.Count,
	})
}

// TriggerProcessing handles POST /api/process
func (h *EventHandler) TriggerProcessing(c *gin.Context) {
	type processRequest struct {
		MetricName string `json:"metric_name" validate:"required,oneof=click_count transaction_sum"`
		Date       string `json:"date,omitempty" validate:"omitempty,datetime=2006-01-02"`
	}

	var req processRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request body", err.Error())
		return
	}

	if err := h.validate.Struct(req); err != nil {
		respondError(c, http.StatusBadRequest, "validation_error", "Validation failed", err.Error())
		return
	}

	runDate := time.Now()
	if req.Date != "" {
		d, err := time.Parse("2006-01-02", req.Date)
		if err != nil {
			respondError(c, http.StatusBadRequest, "validation_error", "Invalid date format. Use YYYY-MM-DD.", nil)
			return
		}
		runDate = d
	}

	go func(date time.Time, metric string) {
		ctx := context.Background()
		if err := h.batchProcessor.ProcessDailyAggregation(ctx, date, metric); err != nil {
			h.log.Error().Err(err).Msg("Processing job failed")
		}
	}(runDate, req.MetricName)

	h.writeAudit(c, "trigger_processing", "/api/process", "success", fmt.Sprintf("metric=%s date=%s", req.MetricName, runDate.Format("2006-01-02")))

	respondSuccess(c, http.StatusAccepted, gin.H{
		"message":     "Processing job triggered",
		"metric_name": req.MetricName,
		"date":        runDate.Format("2006-01-02"),
	})
}

// QueryEvents handles GET /api/query by proxying validated SQL
func (h *EventHandler) QueryEvents(c *gin.Context) {
	sqlQuery := c.Query("sql")
	if sqlQuery == "" {
		respondError(c, http.StatusBadRequest, "validation_error", "sql parameter is required", nil)
		return
	}

	if err := validateSQLQuery(sqlQuery); err != nil {
		respondError(c, http.StatusBadRequest, "sql_validation_failed", err.Error(), nil)
		return
	}

	results, err := h.repo.ProxyQuery(c.Request.Context(), sqlQuery)
	if err != nil {
		h.log.Error().Err(err).Msg("Failed to proxy query")
		respondError(c, http.StatusInternalServerError, "query_failed", "Failed to execute query", nil)
		return
	}

	respondSuccess(c, http.StatusOK, gin.H{
		"data":  results,
		"count": len(results),
	})
}

func (h *EventHandler) writeAudit(c *gin.Context, action, resource, status, details string) {
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

func validateSQLQuery(query string) error {
	if strings.Contains(query, ";") {
		return fmt.Errorf("multiple statements are not allowed")
	}

	trimmed := strings.TrimSpace(strings.ToLower(query))
	if !strings.HasPrefix(trimmed, "select") {
		return fmt.Errorf("only SELECT statements are allowed")
	}

	disallowed := []string{"insert", "update", "delete", "drop", "alter", "create", "system", "truncate", "optimize"}
	for _, kw := range disallowed {
		if strings.Contains(trimmed, kw+" ") || strings.Contains(trimmed, " "+kw) {
			return fmt.Errorf("keyword '%s' is not allowed", kw)
		}
	}

	limitRe := regexp.MustCompile(`(?i)limit\s+\d+`)
	if !limitRe.MatchString(query) {
		return fmt.Errorf("a LIMIT clause is required")
	}

	return nil
}
