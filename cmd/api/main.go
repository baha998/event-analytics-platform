package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"devops-fall2025-23b031152/internal/batch"
	"devops-fall2025-23b031152/internal/config"
	"devops-fall2025-23b031152/internal/handler"
	"devops-fall2025-23b031152/internal/middleware"
	"devops-fall2025-23b031152/internal/processor"
	"devops-fall2025-23b031152/internal/repository"
	"devops-fall2025-23b031152/internal/service"
	"devops-fall2025-23b031152/internal/storage"
	"devops-fall2025-23b031152/pkg/auth"
	"devops-fall2025-23b031152/pkg/kafka"
)

func main() {
	// Load configuration
	cfg := config.Load()
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	// Initialize logger
	logger := initLogger(cfg.Logging)
	log.Logger = logger

	logger.Info().Msg("Starting Real-Time Event Analytics API...")

	// Initialize ClickHouse
	clickHouseConn, err := initClickHouse(cfg.ClickHouse, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to initialize ClickHouse")
	}
	defer clickHouseConn.Close()
	logger.Info().Msg("✓ ClickHouse connection established")

	// Initialize Kafka producer
	kafkaProducer, err := kafka.NewProducer(kafka.ProducerConfig{
		Brokers:       cfg.Kafka.Brokers,
		Topic:         cfg.Kafka.TopicEvents,
		MaxRetries:    cfg.Kafka.MaxRetries,
		RetryBackoff:  cfg.Kafka.RetryBackoff,
		EnableMetrics: cfg.Processor.EnableMetrics,
	}, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to initialize Kafka producer")
	}
	defer kafkaProducer.Close()
	logger.Info().Msg("✓ Kafka producer initialized")

	// Initialize repositories
	eventRepo := repository.NewClickHouseRepository(clickHouseConn, logger)
	metricsRepo := repository.NewMetricsRepository(clickHouseConn, logger)
	auditRepo := repository.NewAuditRepository(clickHouseConn, logger)
	if err := auditRepo.EnsureSchema(context.Background()); err != nil {
		logger.Fatal().Err(err).Msg("Failed to ensure audit schema")
	}

	// Initialize stream processor for real-time aggregations
	streamProcessor := service.SetupClickStreamProcessor(
		metricsRepo,
		cfg.Processor.WindowDuration,
		cfg.Processor.FlushInterval,
		cfg.Processor.WorkerCount,
		cfg.Processor.EnableMetrics,
		logger,
	)
	streamProcessor.Start()
	defer streamProcessor.Stop()

	// Initialize batch processor for daily aggregations
	batchProcessor := processor.NewBatchProcessor(processor.BatchProcessorConfig{
		WorkerCount:   cfg.Processor.BatchWorkerCount,
		EnableMetrics: cfg.Processor.EnableMetrics,
	}, metricsRepo, logger)
	batchProcessor.Start()
	defer batchProcessor.Stop()

	// Initialize cron scheduler
	cronScheduler := processor.NewCronScheduler(batchProcessor, logger)
	if err := cronScheduler.ScheduleDailyAggregation(cfg.Processor.CronSchedule, "click_count"); err != nil {
		logger.Fatal().Err(err).Msg("Failed to schedule daily aggregation")
	}

	cronScheduler.Start()
	defer cronScheduler.Stop()

	// Initialize services
	eventService := service.NewEventService(eventRepo, kafkaProducer, logger)

	// Initialize Kafka consumer with batch processing
	batchHandler := func(events []*kafka.Event) error {
		// Process each event in the batch
		for _, event := range events {
			// Store individual event in ClickHouse
			eventJSON, err := kafka.MarshalEvent(event)
			if err != nil {
				logger.Error().Err(err).Str("event_id", event.EventID).Msg("Failed to marshal event")
				return err
			}

			if err := eventService.ProcessEvent(context.Background(), eventJSON); err != nil {
				logger.Error().Err(err).Str("event_id", event.EventID).Msg("Failed to process event")
				return err
			}

			// Process event for real-time aggregation
			if err := streamProcessor.ProcessEvent(context.Background(), event); err != nil {
				logger.Error().Err(err).Str("event_id", event.EventID).Msg("Failed to process event for aggregation")
				// Continue processing other events
				return err
			}
		}

		return nil
	}

	kafkaConsumer, err := kafka.NewConsumer(kafka.ConsumerConfig{
		Brokers:       cfg.Kafka.Brokers,
		Topics:        cfg.Kafka.Topics,
		GroupID:       cfg.Kafka.ConsumerGroup,
		BatchSize:     cfg.Kafka.BatchSize,
		BatchTimeout:  cfg.Kafka.BatchTimeout,
		MaxRetries:    cfg.Kafka.MaxRetries,
		RetryBackoff:  cfg.Kafka.RetryBackoff,
		EnableMetrics: cfg.Processor.EnableMetrics,
	}, batchHandler, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to initialize Kafka consumer")
	}
	defer kafkaConsumer.Close()

	// Start consumer
	if err := kafkaConsumer.Start(); err != nil {
		logger.Fatal().Err(err).Msg("Failed to start Kafka consumer")
	}
	logger.Info().Msg("✓ Kafka consumer started")

	// Initialize JWT manager
	jwtManager := auth.NewJWTManager(
		cfg.JWT.SecretKey,
		cfg.JWT.TokenExpiry,
		cfg.JWT.RefreshExpiry,
	)

	// Initialize handlers
	eventHandlerHTTP := handler.NewEventHandler(eventService, batchProcessor, auditRepo, logger)
	healthHandler := handler.NewHealthHandler(eventRepo, logger)
	authHandler := handler.NewAuthHandler(jwtManager, cfg.Auth, logger)

	minioClient, err := storage.NewMinIOClient(storage.MinIOConfig{Endpoint: cfg.Storage.Endpoint, AccessKeyID: cfg.Storage.AccessKey, SecretAccessKey: cfg.Storage.SecretKey}, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Invalid MinIO configuration")
	}
	exportDB := clickhouse.OpenDB(&clickhouse.Options{Addr: []string{cfg.ClickHouse.Host + ":" + cfg.ClickHouse.Port}, Auth: clickhouse.Auth{Database: cfg.ClickHouse.Database, Username: cfg.ClickHouse.Username, Password: cfg.ClickHouse.Password}})
	defer exportDB.Close()
	batchHTTP := handler.NewBatchHandler(batch.NewExporter(exportDB, minioClient, logger), batchProcessor, eventRepo, minioClient, auditRepo, logger)

	// Setup router
	router := setupRouter(cfg, eventHandlerHTTP, healthHandler, authHandler, batchHTTP, jwtManager, logger)

	// Start HTTP server
	srv := &http.Server{
		Addr:         ":" + cfg.Server.Port,
		Handler:      router,
		ReadTimeout:  cfg.Server.ReadTimeout,
		WriteTimeout: cfg.Server.WriteTimeout,
		IdleTimeout:  cfg.Server.IdleTimeout,
	}

	// Start server in goroutine
	go func() {
		logger.Info().Str("port", cfg.Server.Port).Msg("Server listening")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal().Err(err).Msg("Server failed to start")
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info().Msg("Shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		logger.Error().Err(err).Msg("Server forced to shutdown")
	}

	logger.Info().Msg("Server exited gracefully")
}

func setupRouter(
	cfg *config.Config,
	eventHandler *handler.EventHandler,
	healthHandler *handler.HealthHandler,
	authHandler *handler.AuthHandler,
	batchHandler *handler.BatchHandler,
	jwtManager *auth.JWTManager,
	logger zerolog.Logger,
) *gin.Engine {
	gin.SetMode(cfg.Server.Mode)
	router := gin.New()
	_ = router.SetTrustedProxies(nil)

	// 1. Recovery middleware FIRST (critical for handling panics)
	router.Use(gin.Recovery())

	// 2. Add request logging for debugging
	router.Use(func(c *gin.Context) {
		start := time.Now()
		path := c.Request.URL.Path
		method := c.Request.Method

		c.Next()

		logger.Info().
			Str("method", method).
			Str("path", path).
			Int("status", c.Writer.Status()).
			Dur("latency", time.Since(start)).
			Str("ip", c.ClientIP()).
			Msg("HTTP request")
	})

	// 3. Register health and metrics endpoints BEFORE other middleware
	// These should always work, even if other services are down
	router.GET("/health", healthHandler.Health)
	router.GET("/health/ready", healthHandler.Ready)
	router.HEAD("/health/ready", healthHandler.Ready)
	router.HEAD("/health", healthHandler.Health) // Docker health check uses HEAD
	router.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// 4. Now add CORS, rate limiting, and metrics middleware for other routes
	router.Use(middleware.CORSMiddleware(cfg.CORS.GrafanaOrigin))
	router.Use(middleware.RateLimitMiddleware(
		middleware.RateLimitFromConfig(cfg.RateLimit.RequestsPerMinute),
		cfg.RateLimit.Burst,
		logger,
	))
	router.Use(middleware.PrometheusMiddleware())

	// 5. Auth routes (no JWT required)
	router.POST("/api/auth/login", authHandler.Login)

	// 6. Protected API routes
	api := router.Group("/api")
	{
		// Protected routes with JWT auth
		api.Use(middleware.AuthMiddleware(jwtManager, logger))

		// POST /api/sources - requires admin role
		api.POST("/sources", middleware.RequireAdmin(logger), eventHandler.CreateSource)

		// POST /api/process - requires admin role
		api.POST("/process", middleware.RequireAdmin(logger), eventHandler.TriggerProcessing)

		// GET /api/query - requires viewer or admin role
		api.GET("/query", middleware.RequireViewer(logger), eventHandler.QueryEvents)
		admin := api.Group("")
		admin.Use(middleware.RequireAdmin(logger))
		admin.POST("/batch/export", batchHandler.ExportBatchEvents)
		admin.POST("/batch/aggregate", batchHandler.AggregateBatchData)
		admin.POST("/batch/cleanup", batchHandler.CleanupTempFiles)
		admin.POST("/cleanup/events", batchHandler.CleanupOldEvents)
		admin.POST("/cleanup/minio", batchHandler.CleanupMinIOFiles)
		admin.POST("/optimize/clickhouse", batchHandler.Optimize)

	}

	return router
}

func initClickHouse(cfg config.ClickHouseConfig, logger zerolog.Logger) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%s", cfg.Host, cfg.Port)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:     10 * time.Second,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	})

	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return conn, nil
}

func initLogger(cfg config.LoggingConfig) zerolog.Logger {
	var logger zerolog.Logger

	if cfg.Format == "console" {
		logger = zerolog.New(zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: time.RFC3339}).
			With().
			Timestamp().
			Logger()
	} else {
		logger = zerolog.New(os.Stdout).
			With().
			Timestamp().
			Logger()
	}

	// Set log level
	switch cfg.Level {
	case "debug":
		logger = logger.Level(zerolog.DebugLevel)
	case "info":
		logger = logger.Level(zerolog.InfoLevel)
	case "warn":
		logger = logger.Level(zerolog.WarnLevel)
	case "error":
		logger = logger.Level(zerolog.ErrorLevel)
	default:
		logger = logger.Level(zerolog.InfoLevel)
	}

	return logger
}
