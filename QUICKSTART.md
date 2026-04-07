# Quick Reference - Real-Time Event Analytics MVP

## Get Started

```bash
# 1. Navigate to project
cd ../project

# 2. Start all services (Kafka, ClickHouse, Prometheus, Grafana, Go API)
docker-compose up -d

# 3. Verify health
curl http://localhost:8080/health
```

## Test the Data Flow

```bash
# Send 50 events to Kafka
curl -X POST http://localhost:8080/api/v1/sources \
  -H "Content-Type: application/json" \
  -d '{"topic": "events", "count": 50, "interval": 100}'

# Query analytics
curl http://localhost:8080/api/v1/analytics/clicks
```
