package handler

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
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
	auditRepo      *repository.AuditRepository
	batchProcessor *processor.BatchProcessor
	validate       *validator.Validate
	log            zerolog.Logger
}

// NewEventHandler creates a new event handler
func NewEventHandler(
	eventService *service.EventService,
	batchProcessor *processor.BatchProcessor,
	auditRepo *repository.AuditRepository,
	log zerolog.Logger,
) *EventHandler {
	return &EventHandler{
		service:        eventService,
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

	if err := h.service.ValidateSource(&req); err != nil {
		respondError(c, 400, "validation_error", err.Error(), nil)
		return
	}

	if err := h.service.CreateSource(c.Request.Context(), &req); err != nil {
		h.log.Error().Err(err).Msg("Failed to create source")
		h.writeAudit(c, "create_source", "/api/sources", "failed", err.Error())
		respondError(c, http.StatusInternalServerError, "create_source_failed", "Failed to create source", nil)
		return
	}

	h.writeAudit(c, "create_source", "/api/sources", "success", fmt.Sprintf("topic=%s count=%d", req.Topic, req.Count))

	respondSuccess(c, http.StatusCreated, gin.H{
		"message": "Events published",
		"topic":   req.Topic,
		"count":   req.Count,
	})
}

// TriggerProcessing handles POST /api/process
func (h *EventHandler) TriggerProcessing(c *gin.Context) {
	type processRequest struct {
		MetricName string `json:"metric_name" validate:"required,oneof=click_count transaction_count"`
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

	if err := h.batchProcessor.ProcessDailyAggregation(c.Request.Context(), runDate, req.MetricName); err != nil {
		respondError(c, 500, "processing_failed", "Daily aggregation failed", nil)
		return
	}

	h.writeAudit(c, "trigger_processing", "/api/process", "success", fmt.Sprintf("metric=%s date=%s", req.MetricName, runDate.Format("2006-01-02")))

	respondSuccess(c, http.StatusOK, gin.H{
		"message":     "Daily aggregation completed",
		"metric_name": req.MetricName,
		"date":        runDate.Format("2006-01-02"),
	})
}

// QueryEvents handles GET /api/query by proxying validated SQL
func (h *EventHandler) QueryEvents(c *gin.Context) {
	var req repository.QueryRequest
	var err error
	if v := c.Query("start_time"); v != "" {
		req.StartTime, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			respondError(c, 400, "invalid_request", "Invalid start_time", nil)
			return
		}
	}
	if v := c.Query("end_time"); v != "" {
		req.EndTime, err = strconv.ParseInt(v, 10, 64)
		if err != nil {
			respondError(c, 400, "invalid_request", "Invalid end_time", nil)
			return
		}
	}
	req.PageURL, req.UserID = c.Query("page_url"), c.Query("user_id")
	req.Limit = 10
	if v := c.Query("limit"); v != "" {
		req.Limit, err = strconv.Atoi(v)
		if err != nil {
			respondError(c, 400, "invalid_request", "Invalid limit", nil)
			return
		}
	}
	if req.Limit < 1 || req.Limit > 1000 || req.StartTime < 0 || req.EndTime < 0 || (req.EndTime > 0 && req.EndTime < req.StartTime) || c.Query("sql") != "" {
		respondError(c, 400, "invalid_request", "Use filters and limit 1..1000; raw SQL is disabled", nil)
		return
	}
	results, err := h.service.QueryEvents(c.Request.Context(), &req)
	if err != nil {
		respondError(c, 500, "query_failed", "Failed to query events", nil)
		return
	}
	respondSuccess(c, 200, gin.H{"data": results, "count": len(results)})
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
