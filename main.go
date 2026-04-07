package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gin-gonic/gin"
	"github.com/segmentio/kafka-go"

	"devops-fall2025-23b031152/processor"
)

var (
	clickHouseConn clickhouse.Conn
	kafkaProducer  *processor.KafkaProducer
)

func main() {
	log.Println("Starting Real-Time Event Analytics API...")

	// Initialize ClickHouse connection
	if err := initClickHouse(); err != nil {
		log.Fatalf("Failed to initialize ClickHouse: %v", err)
	}
	defer clickHouseConn.Close()
	log.Println("✓ ClickHouse connection established")

	// Initialize Kafka producer
	if err := initKafkaProducer(); err != nil {
		log.Fatalf("Failed to initialize Kafka producer: %v", err)
	}
	defer kafkaProducer.Close()
	log.Println("✓ Kafka producer initialized")

	// Setup Gin router
	router := setupRouter()

	// Start HTTP server
	port := getEnv("SERVER_PORT", "8080")
	srv := &http.Server{
		Addr:         ":" + port,
		Handler:      router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in goroutine
	go func() {
		log.Printf("Server listening on port %s", port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("Server exited gracefully")
}

// setupRouter configures all API routes
func setupRouter() *gin.Engine {
	// Set Gin mode from environment
	ginMode := getEnv("GIN_MODE", "debug")
	gin.SetMode(ginMode)

	router := gin.Default()

	// Health check endpoint
	router.GET("/health", healthHandler)

	// API v1 routes
	v1 := router.Group("/api/v1")
	{
		// Source endpoint - triggers Kafka event production
		v1.POST("/sources", processor.CreateSourceHandler(kafkaProducer))

		// Query endpoint - retrieves aggregated data from ClickHouse
		v1.GET("/analytics/clicks", processor.GetClickAnalyticsHandler(clickHouseConn))
	}

	return router
}

// healthHandler returns service health status
func healthHandler(c *gin.Context) {
	// Check ClickHouse connection
	clickhouseHealthy := false
	if err := clickHouseConn.Ping(context.Background()); err == nil {
		clickhouseHealthy = true
	}

	// Check Kafka producer
	kafkaHealthy := kafkaProducer != nil

	status := "healthy"
	httpCode := http.StatusOK

	if !clickhouseHealthy || !kafkaHealthy {
		status = "degraded"
		httpCode = http.StatusServiceUnavailable
	}

	c.JSON(httpCode, gin.H{
		"status":     status,
		"timestamp":  time.Now().Unix(),
		"clickhouse": clickhouseHealthy,
		"kafka":      kafkaHealthy,
	})
}

// initClickHouse establishes connection to ClickHouse
func initClickHouse() error {
	host := getEnv("CLICKHOUSE_HOST", "clickhouse-server")
	port := getEnv("CLICKHOUSE_PORT", "9000")
	database := getEnv("CLICKHOUSE_DATABASE", "analytics")
	username := getEnv("CLICKHOUSE_USER", "default")
	password := getEnv("CLICKHOUSE_PASSWORD", "clickhouse123")

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", host, port)},
		Auth: clickhouse.Auth{
			Database: database,
			Username: username,
			Password: password,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})

	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	clickHouseConn = conn
	return nil
}

// initKafkaProducer initializes Kafka producer
func initKafkaProducer() error {
	bootstrapServers := getEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{bootstrapServers},
		Balancer:     &kafka.LeastBytes{},
		MaxAttempts:  3,
		BatchTimeout: 10 * time.Millisecond,
	})

	kafkaProducer = processor.NewKafkaProducer(writer)
	return nil
}

// getEnv retrieves environment variable with fallback
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
