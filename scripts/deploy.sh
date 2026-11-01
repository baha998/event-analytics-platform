#!/usr/bin/env sh
set -eu
cd "$(dirname "$0")/.."
test -f .env || { echo "Copy .env.example to .env first" >&2; exit 1; }
docker compose up -d --build --wait zookeeper kafka clickhouse-server go-api
docker compose ps
