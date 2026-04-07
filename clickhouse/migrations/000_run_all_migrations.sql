-- Migration Script Runner
-- This script applies all migrations in order

-- Migration 001: Create events table
\i clickhouse/migrations/001_create_events_table.sql

-- Migration 002: Create metrics table  
\i clickhouse/migrations/002_create_metrics_table.sql

-- Verify migrations
SELECT 'Events table created' as status WHERE EXISTS (
    SELECT 1 FROM system.tables WHERE database = 'analytics' AND name = 'events'
);

SELECT 'Metrics table created' as status WHERE EXISTS (
    SELECT 1 FROM system.tables WHERE database = 'analytics' AND name = 'metrics'
);

