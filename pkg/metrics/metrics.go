package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// HTTPRequestsTotal counts total HTTP requests
	HTTPRequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests",
		},
		[]string{"method", "endpoint", "status"},
	)

	// HTTPRequestDuration tracks HTTP request duration
	HTTPRequestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "endpoint"},
	)

	// KafkaMessagesProduced counts Kafka messages produced
	KafkaMessagesProduced = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_produced_total",
			Help: "Total number of Kafka messages produced",
		},
		[]string{"topic"},
	)

	// KafkaMessagesConsumed counts Kafka messages consumed
	KafkaMessagesConsumed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_messages_consumed_total",
			Help: "Total number of Kafka messages consumed",
		},
		[]string{"topic"},
	)

	// KafkaMessageProcessingErrors counts Kafka processing errors
	KafkaMessageProcessingErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_message_processing_errors_total",
			Help: "Total number of Kafka message processing errors",
		},
		[]string{"topic"},
	)

	// ClickHouseQueriesTotal counts ClickHouse queries
	ClickHouseQueriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "clickhouse_queries_total",
			Help: "Total number of ClickHouse queries",
		},
		[]string{"operation"},
	)

	// ClickHouseQueryDuration tracks ClickHouse query duration
	ClickHouseQueryDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "clickhouse_query_duration_seconds",
			Help:    "ClickHouse query duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation"},
	)

	// KafkaErrorsTotal counts Kafka errors
	KafkaErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_errors_total",
			Help: "Total number of Kafka errors",
		},
		[]string{"topic", "error_type"},
	)

	// EventsInserted counts events inserted into ClickHouse
	EventsInserted = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "events_inserted_total",
			Help: "Total number of events inserted into ClickHouse",
		},
	)

	// EventsProcessedTotal counts events processed
	EventsProcessedTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "events_processed_total",
			Help: "Total number of events processed",
		},
	)

	// ProcessingLatencySeconds tracks processing latency
	ProcessingLatencySeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "processing_latency_seconds",
			Help:    "Event processing latency in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	// ActiveConnections tracks the number of active HTTP connections
	ActiveConnections = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "active_connections",
			Help: "Number of active HTTP connections",
		},
	)

	// KafkaLag tracks the consumer lag for Kafka topics
	KafkaLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "kafka_lag",
			Help: "Kafka consumer lag in messages",
		},
		[]string{"topic", "partition"},
	)

	// EventsProcessedByType counts events processed by event type
	EventsProcessedByType = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "events_processed_by_type",
			Help: "Total number of events processed by event type",
		},
		[]string{"event_type"},
	)

	// AggregationErrorsTotal counts aggregation processing errors
	AggregationErrorsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "aggregation_errors_total",
			Help: "Total number of aggregation processing errors",
		},
	)

	// KafkaInvalidEventsTotal counts invalid events that failed validation
	KafkaInvalidEventsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_invalid_events_total",
			Help: "Total number of invalid Kafka events that failed validation",
		},
		[]string{"reason"},
	)

	// KafkaValidEventsTotal counts valid events that passed validation
	KafkaValidEventsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "kafka_valid_events_total",
			Help: "Total number of valid Kafka events that passed validation",
		},
	)

	// BatchExportDurationSeconds tracks batch export duration
	BatchExportDurationSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "batch_export_duration_seconds",
			Help:    "Batch export operation duration in seconds",
			Buckets: prometheus.DefBuckets,
		},
	)

	// BatchExportRowsTotal counts total rows exported in batches
	BatchExportRowsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "batch_export_rows_total",
			Help: "Total number of rows exported in batch operations",
		},
	)
)
