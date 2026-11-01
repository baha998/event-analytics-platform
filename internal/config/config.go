package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all application configuration
type Config struct {
	Storage    StorageConfig
	Server     ServerConfig
	ClickHouse ClickHouseConfig
	Kafka      KafkaConfig
	JWT        JWTConfig
	Logging    LoggingConfig
	Processor  ProcessorConfig
	Auth       AuthConfig
	CORS       CORSConfig
	RateLimit  RateLimitConfig
}

type StorageConfig struct{ Endpoint, AccessKey, SecretKey string }

// ServerConfig holds server configuration
type ServerConfig struct {
	Port         string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	Mode         string
}

// ClickHouseConfig holds ClickHouse connection configuration
type ClickHouseConfig struct {
	Host            string
	Port            string
	Database        string
	Username        string
	Password        string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	QueryTimeout    time.Duration
	RetryAttempts   int
	RetryBackoff    time.Duration
}

// KafkaConfig holds Kafka configuration
type KafkaConfig struct {
	Brokers       []string
	Topics        []string // Configurable topics for consumer
	TopicEvents   string   // Default topic for producer
	ConsumerGroup string
	BatchSize     int           // Batch size for processing (default 100)
	BatchTimeout  time.Duration // Timeout for batch collection
	MaxRetries    int           // Maximum retry attempts (default 3)
	RetryBackoff  time.Duration // Initial retry backoff
}

// JWTConfig holds JWT configuration
type JWTConfig struct {
	SecretKey     string
	TokenExpiry   time.Duration
	RefreshExpiry time.Duration
}

// AuthConfig holds simple static user credentials
type AuthConfig struct {
	AdminUsername  string
	AdminPassword  string
	ViewerUsername string
	ViewerPassword string
}

// CORSConfig holds allowed origins
type CORSConfig struct {
	GrafanaOrigin string
}

// RateLimitConfig holds rate limiting configuration
type RateLimitConfig struct {
	RequestsPerMinute int
	Burst             int
}

// LoggingConfig holds logging configuration
type LoggingConfig struct {
	Level  string
	Format string
}

// ProcessorConfig holds processor configuration
type ProcessorConfig struct {
	WindowDuration   time.Duration
	FlushInterval    time.Duration
	WorkerCount      int
	BatchWorkerCount int
	CronSchedule     string // Cron expression for daily batch job
	EnableMetrics    bool
}

// Load loads configuration from environment variables
func Load() *Config {
	return &Config{
		Storage: StorageConfig{Endpoint: getEnv("MINIO_ENDPOINT", "localhost:9002"), AccessKey: getEnv("MINIO_ACCESS_KEY", "admin"), SecretKey: getEnv("MINIO_SECRET_KEY", "admin123")},
		Server: ServerConfig{
			Port:         getEnv("SERVER_PORT", "8080"),
			ReadTimeout:  getDurationEnv("SERVER_READ_TIMEOUT", 15*time.Second),
			WriteTimeout: getDurationEnv("SERVER_WRITE_TIMEOUT", 15*time.Second),
			IdleTimeout:  getDurationEnv("SERVER_IDLE_TIMEOUT", 60*time.Second),
			Mode:         getEnv("GIN_MODE", "release"),
		},
		ClickHouse: ClickHouseConfig{
			Host:            getEnv("CLICKHOUSE_HOST", "clickhouse-server"),
			Port:            getEnv("CLICKHOUSE_PORT", "9000"),
			Database:        getEnv("CLICKHOUSE_DATABASE", "analytics"),
			Username:        getEnv("CLICKHOUSE_USER", "default"),
			Password:        getEnv("CLICKHOUSE_PASSWORD", "clickhouse123"),
			MaxOpenConns:    getIntEnv("CLICKHOUSE_MAX_OPEN_CONNS", 10),
			MaxIdleConns:    getIntEnv("CLICKHOUSE_MAX_IDLE_CONNS", 5),
			ConnMaxLifetime: getDurationEnv("CLICKHOUSE_CONN_MAX_LIFETIME", time.Hour),
			QueryTimeout:    getDurationEnv("CLICKHOUSE_QUERY_TIMEOUT", 10*time.Second),
			RetryAttempts:   getIntEnv("CLICKHOUSE_RETRY_ATTEMPTS", 3),
			RetryBackoff:    getDurationEnv("CLICKHOUSE_RETRY_BACKOFF", 100*time.Millisecond),
		},
		Kafka: KafkaConfig{
			Brokers:       getStringSliceEnv("KAFKA_BOOTSTRAP_SERVERS", []string{"kafka:9092"}),
			Topics:        getStringSliceEnv("KAFKA_TOPICS", []string{getEnv("KAFKA_TOPIC_EVENTS", "events")}),
			TopicEvents:   getEnv("KAFKA_TOPIC_EVENTS", "events"),
			ConsumerGroup: getEnv("KAFKA_CONSUMER_GROUP", "event-processor"),
			BatchSize:     getIntEnv("KAFKA_BATCH_SIZE", 100),
			BatchTimeout:  getDurationEnv("KAFKA_BATCH_TIMEOUT", 1*time.Second),
			MaxRetries:    getIntEnv("KAFKA_MAX_RETRIES", 3),
			RetryBackoff:  getDurationEnv("KAFKA_RETRY_BACKOFF", 100*time.Millisecond),
		},
		JWT: JWTConfig{
			SecretKey:     os.Getenv("JWT_SECRET_KEY"),
			TokenExpiry:   getDurationEnv("JWT_TOKEN_EXPIRY", 24*time.Hour),
			RefreshExpiry: getDurationEnv("JWT_REFRESH_EXPIRY", 7*24*time.Hour),
		},
		Logging: LoggingConfig{
			Level:  getEnv("LOG_LEVEL", "info"),
			Format: getEnv("LOG_FORMAT", "json"),
		},
		Processor: ProcessorConfig{
			WindowDuration:   getDurationEnv("PROCESSOR_WINDOW_DURATION", 1*time.Minute),
			FlushInterval:    getDurationEnv("PROCESSOR_FLUSH_INTERVAL", 1*time.Minute),
			WorkerCount:      getIntEnv("PROCESSOR_WORKER_COUNT", 5),
			BatchWorkerCount: getIntEnv("PROCESSOR_BATCH_WORKER_COUNT", 5),
			CronSchedule:     getEnv("PROCESSOR_CRON_SCHEDULE", "0 0 1 * * *"), // Daily at 1 AM
			EnableMetrics:    getBoolEnv("PROCESSOR_ENABLE_METRICS", true),
		},
		Auth: AuthConfig{
			AdminUsername:  getEnv("AUTH_ADMIN_USERNAME", "admin"),
			AdminPassword:  os.Getenv("AUTH_ADMIN_PASSWORD"),
			ViewerUsername: getEnv("AUTH_VIEWER_USERNAME", "viewer"),
			ViewerPassword: os.Getenv("AUTH_VIEWER_PASSWORD"),
		},
		CORS: CORSConfig{
			GrafanaOrigin: getEnv("GRAFANA_ORIGIN", "http://localhost:3000"),
		},
		RateLimit: RateLimitConfig{
			RequestsPerMinute: getIntEnv("RATE_LIMIT_RPM", 100),
			Burst:             getIntEnv("RATE_LIMIT_BURST", 120),
		},
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getIntEnv(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getDurationEnv(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func getStringSliceEnv(key string, defaultValue []string) []string {
	if value := os.Getenv(key); value != "" {
		// Split by comma
		values := []string{}
		for _, v := range splitString(value, ",") {
			if trimmed := trimSpace(v); trimmed != "" {
				values = append(values, trimmed)
			}
		}
		if len(values) > 0 {
			return values
		}
	}
	return defaultValue
}

func splitString(s, sep string) []string { return strings.Split(s, sep) }
func trimSpace(s string) string          { return strings.TrimSpace(s) }

func getBoolEnv(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func (c *Config) Validate() error {
	if len(c.JWT.SecretKey) < 32 {
		return fmt.Errorf("JWT_SECRET_KEY must contain at least 32 characters")
	}
	if c.Auth.AdminPassword == "" || c.Auth.ViewerPassword == "" {
		return fmt.Errorf("AUTH_ADMIN_PASSWORD and AUTH_VIEWER_PASSWORD are required")
	}
	if c.Kafka.BatchSize < 1 || c.Kafka.BatchTimeout <= 0 || c.Processor.WindowDuration <= 0 || c.Processor.FlushInterval <= 0 {
		return fmt.Errorf("batch sizes and processing durations must be positive")
	}
	return nil
}
