package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockHealthRepository is a mock for health checks
type MockHealthRepository struct {
	mock.Mock
}

func (m *MockHealthRepository) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func TestHealth_Success(t *testing.T) {
	// Arrange
	gin.SetMode(gin.TestMode)
	mockRepo := new(MockHealthRepository)
	mockRepo.On("Ping", mock.Anything).Return(nil)

	handler := &HealthHandler{
		repo: mockRepo,
		log:  createTestLogger(),
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("GET", "/health", nil)

	// Act
	handler.Health(c)

	// Assert
	assert.Equal(t, http.StatusOK, w.Code)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)
	assert.Equal(t, "ok", response["status"])

	mockRepo.AssertExpectations(t)
}

func TestHealth_DatabaseDown(t *testing.T) {
	// Arrange
	gin.SetMode(gin.TestMode)
	mockRepo := new(MockHealthRepository)
	mockRepo.On("Ping", mock.Anything).Return(assert.AnError)

	handler := &HealthHandler{
		repo: mockRepo,
		log:  createTestLogger(),
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("GET", "/health", nil)

	// Act
	handler.Health(c)

	// Assert
	assert.Equal(t, http.StatusOK, w.Code) // Should still return 200

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	assert.NoError(t, err)

	// Check that clickhouse status indicates unhealthy
	clickhouse, ok := response["clickhouse"].(map[string]interface{})
	assert.True(t, ok)
	assert.Equal(t, "unhealthy", clickhouse["status"])

	mockRepo.AssertExpectations(t)
}

func TestHealth_HEAD_Request(t *testing.T) {
	// Arrange
	gin.SetMode(gin.TestMode)
	mockRepo := new(MockHealthRepository)
	mockRepo.On("Ping", mock.Anything).Return(nil)

	handler := &HealthHandler{
		repo: mockRepo,
		log:  createTestLogger(),
	}

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request, _ = http.NewRequest("HEAD", "/health", nil)

	// Act
	handler.Health(c)

	// Assert
	assert.Equal(t, http.StatusOK, w.Code)
	mockRepo.AssertExpectations(t)
}

// Helper function
func createTestLogger() zerolog.Logger {
	// Return test logger
	return zerolog.Nop()
}
