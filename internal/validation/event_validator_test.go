package validation

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateEvent_ValidClick(t *testing.T) {
	jsonData := []byte(`{
		"event_id": "550e8400-e29b-41d4-a716-446655440000",
		"event_type": "click",
		"user_id": "user123",
		"timestamp": "2025-12-13T10:00:00Z"
	}`)

	event, err := ValidateEvent(jsonData)
	require.NoError(t, err)
	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", event.EventID)
	assert.Equal(t, "click", event.EventType)
	assert.Equal(t, "user123", event.UserID)
	assert.Equal(t, "2025-12-13T10:00:00Z", event.Timestamp)
}

func TestValidateEvent_ValidTransaction(t *testing.T) {
	jsonData := []byte(`{
		"event_id": "550e8400-e29b-41d4-a716-446655440001",
		"event_type": "transaction",
		"user_id": "user456",
		"page_url": "https://example.com/checkout",
		"timestamp": "2025-12-13T10:05:00Z"
	}`)

	event, err := ValidateEvent(jsonData)
	require.NoError(t, err)
	assert.Equal(t, "transaction", event.EventType)
	assert.Equal(t, "https://example.com/checkout", event.PageURL)
}

func TestValidateEvent_ValidPageview(t *testing.T) {
	jsonData := []byte(`{
		"event_id": "550e8400-e29b-41d4-a716-446655440002",
		"event_type": "pageview",
		"user_id": "user789",
		"page_url": "http://example.com/home",
		"timestamp": "2025-12-13T10:10:00+05:00"
	}`)

	event, err := ValidateEvent(jsonData)
	require.NoError(t, err)
	assert.Equal(t, "pageview", event.EventType)
}

func TestValidateEvent_MissingEventID(t *testing.T) {
	jsonData := []byte(`{
		"event_type": "click",
		"user_id": "user123",
		"timestamp": "2025-12-13T10:00:00Z"
	}`)

	_, err := ValidateEvent(jsonData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EventID")
	assert.Contains(t, err.Error(), "required")
}

func TestValidateEvent_InvalidUUID(t *testing.T) {
	jsonData := []byte(`{
		"event_id": "not-a-uuid",
		"event_type": "click",
		"user_id": "user123",
		"timestamp": "2025-12-13T10:00:00Z"
	}`)

	_, err := ValidateEvent(jsonData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UUID")
}

func TestValidateEvent_InvalidEventType(t *testing.T) {
	jsonData := []byte(`{
		"event_id": "550e8400-e29b-41d4-a716-446655440000",
		"event_type": "invalid_type",
		"user_id": "user123",
		"timestamp": "2025-12-13T10:00:00Z"
	}`)

	_, err := ValidateEvent(jsonData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "EventType")
}

func TestValidateEvent_EmptyUserID(t *testing.T) {
	jsonData := []byte(`{
		"event_id": "550e8400-e29b-41d4-a716-446655440000",
		"event_type": "click",
		"user_id": "",
		"timestamp": "2025-12-13T10:00:00Z"
	}`)

	_, err := ValidateEvent(jsonData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "UserID")
}

func TestValidateEvent_InvalidURL(t *testing.T) {
	jsonData := []byte(`{
		"event_id": "550e8400-e29b-41d4-a716-446655440000",
		"event_type": "click",
		"user_id": "user123",
		"page_url": "not-a-valid-url",
		"timestamp": "2025-12-13T10:00:00Z"
	}`)

	_, err := ValidateEvent(jsonData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PageURL")
	assert.Contains(t, err.Error(), "URL")
}

func TestValidateEvent_InvalidTimestamp(t *testing.T) {
	jsonData := []byte(`{
		"event_id": "550e8400-e29b-41d4-a716-446655440000",
		"event_type": "click",
		"user_id": "user123",
		"timestamp": "2025-12-13 10:00:00"
	}`)

	_, err := ValidateEvent(jsonData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Timestamp")
}

func TestValidateEvent_InvalidJSON(t *testing.T) {
	jsonData := []byte(`{invalid json}`)

	_, err := ValidateEvent(jsonData)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid JSON")
}

func TestMapToClickHouse_Valid(t *testing.T) {
	event := &EventSchema{
		EventID:   "550e8400-e29b-41d4-a716-446655440000",
		EventType: "click",
		UserID:    "user123",
		PageURL:   "https://example.com/home",
		Timestamp: "2025-12-13T10:00:00Z",
	}

	clickhouseMap, err := MapToClickHouse(event)
	require.NoError(t, err)

	assert.Equal(t, "550e8400-e29b-41d4-a716-446655440000", clickhouseMap["event_id"])
	assert.Equal(t, "click", clickhouseMap["event_type"])
	assert.Equal(t, "user123", clickhouseMap["user_id"])
	assert.Equal(t, "https://example.com/home", clickhouseMap["page_url"])

	timestamp, ok := clickhouseMap["timestamp"].(time.Time)
	require.True(t, ok)
	assert.Equal(t, "2025-12-13T10:00:00Z", timestamp.Format(time.RFC3339))
}

func TestMapToClickHouse_InvalidTimestamp(t *testing.T) {
	event := &EventSchema{
		EventID:   "550e8400-e29b-41d4-a716-446655440000",
		EventType: "click",
		UserID:    "user123",
		Timestamp: "invalid-timestamp",
	}

	_, err := MapToClickHouse(event)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timestamp")
}

func TestValidateAndMapToClickHouse_Valid(t *testing.T) {
	jsonData := []byte(`{
		"event_id": "550e8400-e29b-41d4-a716-446655440000",
		"event_type": "click",
		"user_id": "user123",
		"page_url": "https://example.com",
		"timestamp": "2025-12-13T10:00:00Z"
	}`)

	clickhouseMap, err := ValidateAndMapToClickHouse(jsonData)
	require.NoError(t, err)
	assert.NotNil(t, clickhouseMap)
	assert.Equal(t, "click", clickhouseMap["event_type"])
}

func TestValidateAndMapToClickHouse_Invalid(t *testing.T) {
	jsonData := []byte(`{
		"event_id": "invalid-uuid",
		"event_type": "click",
		"user_id": "user123",
		"timestamp": "2025-12-13T10:00:00Z"
	}`)

	_, err := ValidateAndMapToClickHouse(jsonData)
	require.Error(t, err)
}

func TestBatchValidateEvents(t *testing.T) {
	jsonDataList := [][]byte{
		[]byte(`{"event_id": "550e8400-e29b-41d4-a716-446655440000", "event_type": "click", "user_id": "user1", "timestamp": "2025-12-13T10:00:00Z"}`),
		[]byte(`{"event_id": "invalid", "event_type": "click", "user_id": "user2", "timestamp": "2025-12-13T10:01:00Z"}`),
		[]byte(`{"event_id": "550e8400-e29b-41d4-a716-446655440001", "event_type": "transaction", "user_id": "user3", "timestamp": "2025-12-13T10:02:00Z"}`),
	}

	events, errors := BatchValidateEvents(jsonDataList)

	assert.Len(t, events, 2) // 2 valid events
	assert.Len(t, errors, 1) // 1 invalid event
	assert.Contains(t, errors[0].Error(), "event 1")
}
