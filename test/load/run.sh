#!/usr/bin/env sh
set -eu
: "${JWT_TOKEN:?Set JWT_TOKEN to an admin token}"
RATE=${RATE:-1}
DURATION=${DURATION:-10s}
TARGET_URL=${TARGET_URL:-http://localhost:8080}
body=$(mktemp)
trap 'rm -f "$body"' EXIT
printf '%s' '{"topic":"events","count":1,"interval":0}' > "$body"
printf 'POST %s/api/sources\nContent-Type: application/json\nAuthorization: Bearer %s\n@%s\n' "$TARGET_URL" "$JWT_TOKEN" "$body" | vegeta attack -rate="$RATE" -duration="$DURATION" | vegeta report
