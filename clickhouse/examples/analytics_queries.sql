-- Example Analytics Queries for ClickHouse
-- These queries demonstrate common analytics patterns

-- ============================================
-- TOP 10 PAGES BY CLICK COUNT
-- ============================================
SELECT 
    page_url,
    count() as click_count,
    count(DISTINCT user_id) as unique_users
FROM analytics.events
WHERE event_type = 'click'
    AND timestamp >= now() - INTERVAL 7 DAY
GROUP BY page_url
ORDER BY click_count DESC
LIMIT 10;

-- ============================================
-- HOURLY TRANSACTIONS
-- ============================================
SELECT 
    toStartOfHour(timestamp) as hour,
    count() as transaction_count,
    count(DISTINCT user_id) as unique_users,
    sum(CAST(dimensions['amount'] AS Float64)) as total_amount
FROM analytics.events
WHERE event_type = 'transaction'
    AND timestamp >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour DESC;

-- ============================================
-- TOP 10 PAGES BY CLICK COUNT (Parameterized)
-- ============================================
-- Parameters: start_time, end_time, limit
SELECT 
    page_url,
    count() as click_count,
    count(DISTINCT user_id) as unique_users
FROM analytics.events
WHERE event_type = 'click'
    AND timestamp >= {start_time:DateTime}
    AND timestamp <= {end_time:DateTime}
GROUP BY page_url
ORDER BY click_count DESC
LIMIT {limit:UInt64};

-- ============================================
-- HOURLY TRANSACTIONS (Parameterized)
-- ============================================
-- Parameters: start_time, end_time
SELECT 
    toStartOfHour(timestamp) as hour,
    count() as transaction_count,
    count(DISTINCT user_id) as unique_users
FROM analytics.events
WHERE event_type = 'transaction'
    AND timestamp >= {start_time:DateTime}
    AND timestamp <= {end_time:DateTime}
GROUP BY hour
ORDER BY hour DESC;

-- ============================================
-- USER ACTIVITY BY HOUR
-- ============================================
SELECT 
    toStartOfHour(timestamp) as hour,
    event_type,
    count() as event_count,
    count(DISTINCT user_id) as unique_users
FROM analytics.events
WHERE timestamp >= now() - INTERVAL 24 HOUR
GROUP BY hour, event_type
ORDER BY hour DESC, event_type;

-- ============================================
-- PAGE PERFORMANCE METRICS
-- ============================================
SELECT 
    page_url,
    count() as total_events,
    count(DISTINCT user_id) as unique_visitors,
    min(timestamp) as first_seen,
    max(timestamp) as last_seen
FROM analytics.events
WHERE timestamp >= now() - INTERVAL 7 DAY
GROUP BY page_url
ORDER BY total_events DESC
LIMIT 20;

-- ============================================
-- METRICS AGGREGATION BY DIMENSION
-- ============================================
SELECT 
    timestamp,
    metric_name,
    dimensions['page_url'] as page_url,
    sum(value) as total_value,
    avg(value) as avg_value,
    count() as metric_count
FROM analytics.metrics
WHERE timestamp >= now() - INTERVAL 24 HOUR
    AND metric_name = 'click_count'
GROUP BY timestamp, metric_name, dimensions['page_url']
ORDER BY timestamp DESC, total_value DESC;

-- ============================================
-- DAILY SUMMARY STATISTICS
-- ============================================
SELECT 
    toStartOfDay(timestamp) as day,
    event_type,
    count() as event_count,
    count(DISTINCT user_id) as unique_users,
    count(DISTINCT page_url) as unique_pages
FROM analytics.events
WHERE timestamp >= now() - INTERVAL 30 DAY
GROUP BY day, event_type
ORDER BY day DESC, event_type;

-- ============================================
-- TOP USERS BY EVENT COUNT
-- ============================================
SELECT 
    user_id,
    count() as total_events,
    countIf(event_type = 'click') as clicks,
    countIf(event_type = 'transaction') as transactions,
    min(timestamp) as first_seen,
    max(timestamp) as last_seen
FROM analytics.events
WHERE timestamp >= now() - INTERVAL 7 DAY
GROUP BY user_id
ORDER BY total_events DESC
LIMIT 20;

-- ============================================
-- EVENT TYPE DISTRIBUTION
-- ============================================
SELECT 
    event_type,
    count() as count,
    count() * 100.0 / (SELECT count() FROM analytics.events WHERE timestamp >= now() - INTERVAL 7 DAY) as percentage
FROM analytics.events
WHERE timestamp >= now() - INTERVAL 7 DAY
GROUP BY event_type
ORDER BY count DESC;

