package service

import (
	"context"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"devops-fall2025-23b031152/internal/repository"
	"devops-fall2025-23b031152/pkg/kafka"
)

// MockClickHouseRepository is a mock for ClickHouseRepository
type MockClickHouseRepository struct {
	mock.Mock
}

func (m *MockClickHouseRepository) InsertEvent(ctx context.Context, event *repository.Event) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func (m *MockClickHouseRepository) QueryEvents(ctx context.Context, req *repository.QueryRequest) ([]*repository.QueryResult, error) {
	args := m.Called(ctx, req)
	return args.Get(0).([]*repository.QueryResult), args.Error(1)
}

// MockKafkaProducer is a mock for Kafka producer
type MockKafkaProducer struct {
	mock.Mock
}

func (m *MockKafkaProducer) SendEvent(event *kafka.Event) error {
	args := m.Called(event)
	return args.Error(0)
}

func TestEventService_CreateSource_Success(t *testing.T) {
	// Arrange
	mockProducer := new(MockKafkaProducer)
	mockRepo := new(MockClickHouseRepository)

	// Create logger (you can use a test logger or discard logger)
	logger := createTestLogger()

	service := &EventService{
		repo:     mockRepo,
		producer: mockProducer,
		log:      logger,
	}

	// Create test request
	req := &repository.SourceRequest{
		Topic: "test-topic",
		Count: 10,
	}

	// Expect SendEvent to be called 10 times
	mockProducer.On("SendEvent", mock.AnythingOfType("*kafka.Event")).Return(nil).Times(10)

	// Act
	err := service.CreateSource(context.Background(), req)

	// Assert
	assert.NoError(t, err)
	mockProducer.AssertExpectations(t)
}

func TestEventService_CreateSource_ValidationError(t *testing.T) {
	// Arrange
	mockProducer := new(MockKafkaProducer)
	mockRepo := new(MockClickHouseRepository)
	logger := createTestLogger()

	service := &EventService{
		repo:     mockRepo,
		producer: mockProducer,
		log:      logger,
	}

	// Invalid request (empty topic)
	req := &repository.SourceRequest{
		Topic: "",
		Count: 10,
	}

	// Act
	err := service.CreateSource(context.Background(), req)

	// Assert
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "topic")
}

func TestEventService_CreateSource_KafkaError(t *testing.T) {
	// Arrange
	mockProducer := new(MockKafkaProducer)
	mockRepo := new(MockClickHouseRepository)
	logger := createTestLogger()

	service := &EventService{
		repo:     mockRepo,
		producer: mockProducer,
		log:      logger,
	}

	req := &repository.SourceRequest{
		Topic: "test-topic",
		Count: 5,
	}

	// Simulate Kafka error
	mockProducer.On("SendEvent", mock.AnythingOfType("*kafka.Event")).
		Return(assert.AnError).Once()

	// Act
	err := service.CreateSource(context.Background(), req)

	// Assert
	assert.Error(t, err)
	mockProducer.AssertExpectations(t)
}

func TestEventService_ProcessEvent_Success(t *testing.T) {
	// Arrange
	mockProducer := new(MockKafkaProducer)
	mockRepo := new(MockClickHouseRepository)
	logger := createTestLogger()

	service := &EventService{
		repo:     mockRepo,
		producer: mockProducer,
		log:      logger,
	}

	eventJSON := []byte(`{
		"event_id": "test-123",
		"event_type": "click",
		"user_id": "user-456",
		"page_url": "https://example.com",
		"timestamp": "` + time.Now().Format(time.RFC3339) + `"
	}`)

	// Expect InsertEvent to be called
	mockRepo.On("InsertEvent", mock.Anything, mock.AnythingOfType("*repository.Event")).Return(nil).Once()

	// Act
	err := service.ProcessEvent(context.Background(), eventJSON)

	// Assert
	assert.NoError(t, err)
	mockRepo.AssertExpectations(t)
}

func TestEventService_ProcessEvent_InvalidJSON(t *testing.T) {
	// Arrange
	mockProducer := new(MockKafkaProducer)
	mockRepo := new(MockClickHouseRepository)
	logger := createTestLogger()

	service := &EventService{
		repo:     mockRepo,
		producer: mockProducer,
		log:      logger,
	}

	invalidJSON := []byte(`{invalid json}`)

	// Act
	err := service.ProcessEvent(context.Background(), invalidJSON)

	// Assert
	assert.Error(t, err)
}

func TestEventService_ProcessEvent_DatabaseError(t *testing.T) {
	// Arrange
	mockProducer := new(MockKafkaProducer)
	mockRepo := new(MockClickHouseRepository)
	logger := createTestLogger()

	service := &EventService{
		repo:     mockRepo,
		producer: mockProducer,
		log:      logger,
	}

	eventJSON := []byte(`{
		"event_id": "test-123",
		"event_type": "click",
		"user_id": "user-456",
		"page_url": "https://example.com",
		"timestamp": "` + time.Now().Format(time.RFC3339) + `"
	}`)

	// Simulate database error
	mockRepo.On("InsertEvent", mock.Anything, mock.AnythingOfType("*repository.Event")).
		Return(assert.AnError).Once()

	// Act
	err := service.ProcessEvent(context.Background(), eventJSON)

	// Assert
	assert.Error(t, err)
	mockRepo.AssertExpectations(t)
}

// Helper function to create test logger
func createTestLogger() zerolog.Logger {
	return zerolog.Nop()
}

// Table-driven test example
func TestEventService_CreateSource_TableDriven(t *testing.T) {
	tests := []struct {
		name          string
		request       *repository.SourceRequest
		setupMock     func(*MockKafkaProducer)
		expectedError bool
		errorContains string
	}{
		{
			name: "valid request with 10 events",
			request: &repository.SourceRequest{
				Topic: "events",
				Count: 10,
			},
			setupMock: func(m *MockKafkaProducer) {
				m.On("SendEvent", mock.AnythingOfType("*kafka.Event")).Return(nil).Times(10)
			},
			expectedError: false,
		},
		{
			name: "empty topic",
			request: &repository.SourceRequest{
				Topic: "",
				Count: 5,
			},
			setupMock:     func(m *MockKafkaProducer) {},
			expectedError: true,
			errorContains: "topic",
		},
		{
			name: "zero count",
			request: &repository.SourceRequest{
				Topic: "events",
				Count: 0,
			},
			setupMock:     func(m *MockKafkaProducer) {},
			expectedError: true,
			errorContains: "count",
		},
		{
			name: "kafka producer error",
			request: &repository.SourceRequest{
				Topic: "events",
				Count: 5,
			},
			setupMock: func(m *MockKafkaProducer) {
				m.On("SendEvent", mock.AnythingOfType("*kafka.Event")).
					Return(assert.AnError).Once()
			},
			expectedError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Arrange
			mockProducer := new(MockKafkaProducer)
			mockRepo := new(MockClickHouseRepository)
			logger := createTestLogger()

			service := &EventService{
				repo:     mockRepo,
				producer: mockProducer,
				log:      logger,
			}

			tt.setupMock(mockProducer)

			// Act
			err := service.CreateSource(context.Background(), tt.request)

			// Assert
			if tt.expectedError {
				assert.Error(t, err)
				if tt.errorContains != "" {
					assert.Contains(t, err.Error(), tt.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}

			mockProducer.AssertExpectations(t)
		})
	}
}
