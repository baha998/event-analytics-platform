-- Migration: Update metrics table to use Map for dimensions
-- Date: 2024-01-02
-- Description: Changes dimensions from String to Map(String, String)

-- Note: ClickHouse doesn't support ALTER COLUMN for changing types directly
-- This migration creates a new table and migrates data

-- Create new metrics table with Map type
CREATE TABLE IF NOT EXISTS analytics.metrics_new
(
    timestamp DateTime,
    metric_name String,
    dimensions Map(String, String),
    value Float64,
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, metric_name, dimensions)
SETTINGS index_granularity = 8192;

-- Migrate data (if old table exists)
INSERT INTO analytics.metrics_new 
SELECT 
    timestamp,
    metric_name,
    map('dimension', dimension) as dimensions,
    value,
    ingestion_time
FROM analytics.metrics;

-- Drop old table
DROP TABLE IF EXISTS analytics.metrics;

-- Rename new table
RENAME TABLE analytics.metrics_new TO analytics.metrics;

