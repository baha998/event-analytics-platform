#!/bin/sh
set -eu
query() {
  clickhouse-client --host clickhouse-server --password "$CLICKHOUSE_PASSWORD" --multiquery "$@"
}
query < /schema/01-create-database.sql

# Upgrade the original MVP schema while retaining its table as a backup.
old_type=$(query --query "SELECT type FROM system.columns WHERE database='analytics' AND table='events' AND name='timestamp'")
if [ -n "$old_type" ] && [ "$old_type" != DateTime ]; then
  backup_exists=$(query --query "EXISTS TABLE analytics.events_legacy")
  if [ "$backup_exists" = 1 ]; then
    echo 'events_legacy already exists; inspect the previous upgrade before retrying' >&2
    exit 1
  fi
  sed 's/analytics.events/analytics.events_canonical/g' /schema/02-create-events-table.sql | query
  query --query "INSERT INTO analytics.events_canonical (event_id,event_type,user_id,page_url,timestamp,ingestion_time) SELECT event_id,event_type,toString(user_id),page_url,toDateTime(timestamp),ingestion_time FROM analytics.events"
  query --query "RENAME TABLE analytics.events TO analytics.events_legacy, analytics.events_canonical TO analytics.events"
fi
query < /schema/02-create-events-table.sql
query < /schema/03-create-metrics-table.sql
