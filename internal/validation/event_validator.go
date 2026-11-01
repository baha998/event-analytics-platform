package validation

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

// EventSchema represents a validated event with strict validation rules
type EventSchema struct {
	EventID   string `json:"event_id" validate:"required,uuid"`
	EventType string `json:"event_type" validate:"required,oneof=click transaction pageview"`
	UserID    string `json:"user_id" validate:"required,min=1"`
	PageURL   string `json:"page_url,omitempty" validate:"omitempty,pageurl"`
	Timestamp string `json:"timestamp" validate:"required,datetime=2006-01-02T15:04:05Z07:00"`
}

var validate *validator.Validate

func init() {
	validate = validator.New()
	validate.RegisterValidation("pageurl", func(fl validator.FieldLevel) bool {
		value := fl.Field().String()
		parsed, err := url.ParseRequestURI(value)
		return err == nil && ((strings.HasPrefix(value, "/") && !strings.HasPrefix(value, "//")) || ((parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != ""))
	})
}

// ValidateEvent unmarshals and validates JSON event data
func ValidateEvent(jsonData []byte) (*EventSchema, error) {
	var event EventSchema

	// Unmarshal JSON
	if err := json.Unmarshal(jsonData, &event); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	// Validate struct
	if err := validate.Struct(event); err != nil {
		// Format validation errors for better readability
		if validationErrors, ok := err.(validator.ValidationErrors); ok {
			return nil, fmt.Errorf("%s (PageURL must be an HTTP URL or absolute path)", formatValidationErrors(validationErrors))
		}
		return nil, fmt.Errorf("validation failed: %w", err)
	}

	return &event, nil
}

// MapToClickHouse converts EventSchema to a map ready for ClickHouse INSERT
func MapToClickHouse(event *EventSchema) (map[string]interface{}, error) {
	// Parse timestamp
	timestamp, err := time.Parse(time.RFC3339, event.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp format: %w", err)
	}

	// Parse UUID to ensure it's valid
	eventUUID, err := uuid.Parse(event.EventID)
	if err != nil {
		return nil, fmt.Errorf("invalid UUID: %w", err)
	}

	return map[string]interface{}{
		"event_id":   eventUUID.String(),
		"event_type": event.EventType,
		"user_id":    event.UserID,
		"page_url":   event.PageURL,
		"timestamp":  timestamp,
	}, nil
}

// ValidateAndMapToClickHouse combines validation and mapping in one step
func ValidateAndMapToClickHouse(jsonData []byte) (map[string]interface{}, error) {
	event, err := ValidateEvent(jsonData)
	if err != nil {
		return nil, err
	}

	return MapToClickHouse(event)
}

// formatValidationErrors creates a readable error message from validation errors
func formatValidationErrors(errors validator.ValidationErrors) error {
	errorMessages := make([]string, 0, len(errors))

	for _, err := range errors {
		var message string
		switch err.Tag() {
		case "required":
			message = fmt.Sprintf("field '%s' is required", err.Field())
		case "uuid":
			message = fmt.Sprintf("field '%s' must be a valid UUID", err.Field())
		case "oneof":
			message = fmt.Sprintf("field '%s' must be one of: %s", err.Field(), err.Param())
		case "min":
			message = fmt.Sprintf("field '%s' must have minimum length %s", err.Field(), err.Param())
		case "url":
			message = fmt.Sprintf("field '%s' must be a valid URL", err.Field())
		case "datetime":
			message = fmt.Sprintf("field '%s' must be in ISO8601 format (RFC3339)", err.Field())
		default:
			message = fmt.Sprintf("field '%s' failed validation: %s", err.Field(), err.Tag())
		}
		errorMessages = append(errorMessages, message)
	}

	return fmt.Errorf("validation errors: %v", errorMessages)
}

// BatchValidateEvents validates multiple events at once
func BatchValidateEvents(jsonDataList [][]byte) ([]EventSchema, []error) {
	events := make([]EventSchema, 0, len(jsonDataList))
	errors := make([]error, 0)

	for i, jsonData := range jsonDataList {
		event, err := ValidateEvent(jsonData)
		if err != nil {
			errors = append(errors, fmt.Errorf("event %d: %w", i, err))
			continue
		}
		events = append(events, *event)
	}

	return events, errors
}
