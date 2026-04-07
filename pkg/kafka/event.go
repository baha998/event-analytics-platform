package kafka

import (
	"encoding/json"
	"fmt"
	"time"
)

// Event represents the event schema
type Event struct {
	EventID   string `json:"event_id"`
	EventType string `json:"event_type"` // "click" or "transaction"
	UserID    string `json:"user_id"`
	PageURL   string `json:"page_url"`
	Timestamp string `json:"timestamp"` // ISO8601 format
}

// Validate validates the event structure
func (e *Event) Validate() error {
	if e.EventID == "" {
		return fmt.Errorf("event_id is required")
	}
	if e.EventType != "click" && e.EventType != "transaction" {
		return fmt.Errorf("event_type must be 'click' or 'transaction', got: %s", e.EventType)
	}
	if e.UserID == "" {
		return fmt.Errorf("user_id is required")
	}
	if e.PageURL == "" {
		return fmt.Errorf("page_url is required")
	}
	if e.Timestamp == "" {
		return fmt.Errorf("timestamp is required")
	}

	// Validate ISO8601 timestamp
	if _, err := time.Parse(time.RFC3339, e.Timestamp); err != nil {
		return fmt.Errorf("invalid timestamp format (expected ISO8601): %w", err)
	}

	return nil
}

// UnmarshalEvent unmarshals JSON bytes to Event
func UnmarshalEvent(data []byte) (*Event, error) {
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event: %w", err)
	}

	if err := event.Validate(); err != nil {
		return nil, fmt.Errorf("invalid event: %w", err)
	}

	return &event, nil
}

// MarshalEvent marshals Event to JSON bytes
func MarshalEvent(event *Event) ([]byte, error) {
	return json.Marshal(event)
}
