-- Create metrics table for storing aggregated metrics
-- This table uses MergeTree engine optimized for analytical queries
-- Uses Map type for dimensions to support flexible key-value pairs

CREATE TABLE IF NOT EXISTS analytics.metrics
(
    timestamp DateTime,
    metric_name String,
    dimensions Map(String, String),
    value Float64,
    
    -- Metadata columns
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, metric_name, dimensions['dimension'])
SETTINGS index_granularity = 8192;

-- Create secondary indexes for common queries
ALTER TABLE analytics.metrics ADD INDEX IF NOT EXISTS idx_metric_name metric_name TYPE set(100) GRANULARITY 4;

-- Sample query to verify table creation
-- SELECT * FROM analytics.metrics LIMIT 10;

