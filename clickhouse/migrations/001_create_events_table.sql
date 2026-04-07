-- Migration: Create events table with event_type and optimized partitioning
-- Date: 2024-01-01
-- Description: Creates events table with MergeTree engine, partitioned by date

CREATE TABLE IF NOT EXISTS analytics.events
(
    event_id String,
    event_type String,
    user_id String,
    page_url String,
    timestamp DateTime,
    
    -- Metadata columns
    ingestion_time DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
ORDER BY (timestamp, event_type, page_url, user_id)
SETTINGS index_granularity = 8192;

-- Create secondary indexes for common queries
ALTER TABLE analytics.events ADD INDEX IF NOT EXISTS idx_event_type event_type TYPE set(100) GRANULARITY 4;
ALTER TABLE analytics.events ADD INDEX IF NOT EXISTS idx_page_url page_url TYPE bloom_filter(0.01) GRANULARITY 4;
ALTER TABLE analytics.events ADD INDEX IF NOT EXISTS idx_user_id user_id TYPE bloom_filter(0.01) GRANULARITY 4;

-- Sample query to verify table creation
-- SELECT * FROM analytics.events LIMIT 10;

