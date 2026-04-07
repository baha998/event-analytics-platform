#!/bin/bash

# Load Testing Script for Real-Time Event Analytics
# Uses Vegeta for load testing at 500 events/sec for 5 minutes

set -e

# Configuration
RATE=${RATE:-500}           # Requests per second
DURATION=${DURATION:-5m}    # Test duration
TARGET_URL=${TARGET_URL:-http://localhost:8080}
OUTPUT_DIR=${OUTPUT_DIR:-./test/load/results}
RESULTS_FILE="$OUTPUT_DIR/results_$(date +%Y%m%d_%H%M%S).bin"
REPORT_FILE="$OUTPUT_DIR/report_$(date +%Y%m%d_%H%M%S).txt"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== Real-Time Analytics Load Test ===${NC}"
echo "Rate: $RATE req/sec"
echo "Duration: $DURATION"
echo "Target: $TARGET_URL"
echo ""

# Create output directory
mkdir -p "$OUTPUT_DIR"

# Check if vegeta is installed
if ! command -v vegeta &> /dev/null; then
    echo -e "${RED}Error: vegeta is not installed${NC}"
    echo "Install with: go install github.com/tsenart/vegeta/v12@latest"
    exit 1
fi

# Check if target is reachable
echo -e "${YELLOW}Checking target availability...${NC}"
if ! curl -f -s "$TARGET_URL/health" > /dev/null; then
    echo -e "${RED}Error: Target $TARGET_URL/health is not reachable${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Target is reachable${NC}\n"

# Authenticate and get token
echo -e "${YELLOW}Authenticating...${NC}"
TOKEN=$(curl -s -X POST "$TARGET_URL/api/auth/login" \
    -H "Content-Type: application/json" \
    -d '{"username":"admin","password":"admin123"}' \
    | jq -r '.token')

if [ "$TOKEN" == "null" ] || [ -z "$TOKEN" ]; then
    echo -e "${RED}Error: Failed to authenticate${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Authentication successful${NC}\n"

# Generate targets file with realistic payload
echo -e "${YELLOW}Generating test targets...${NC}"
TARGETS_FILE="/tmp/vegeta_targets_$$.txt"

cat > "$TARGETS_FILE" <<EOF
POST $TARGET_URL/api/sources
Content-Type: application/json
Authorization: Bearer $TOKEN

{"topic":"events","count":1}
@EOF

echo -e "${GREEN}✓ Targets file created${NC}\n"

# Run load test
echo -e "${YELLOW}Starting load test...${NC}"
echo "This will run for $DURATION at $RATE req/sec"
echo ""

vegeta attack \
    -rate=$RATE \
    -duration=$DURATION \
    -targets="$TARGETS_FILE" \
    -output="$RESULTS_FILE" \
    -timeout=30s

echo -e "\n${GREEN}✓ Load test completed${NC}\n"

# Generate reports
echo -e "${YELLOW}Generating reports...${NC}"

# Text report
vegeta report "$RESULTS_FILE" > "$REPORT_FILE"

# Display summary
echo -e "${GREEN}=== Test Results ===${NC}\n"
cat "$REPORT_FILE"

# Generate histogram
HISTOGRAM_FILE="$OUTPUT_DIR/histogram_$(date +%Y%m%d_%H%M%S).txt"
vegeta report -type='hist[0,10ms,20ms,30ms,40ms,50ms,100ms,200ms,500ms,1s,2s]' "$RESULTS_FILE" > "$HISTOGRAM_FILE"

echo -e "\n${GREEN}=== Latency Histogram ===${NC}\n"
cat "$HISTOGRAM_FILE"

# Generate plot (HTML)
PLOT_FILE="$OUTPUT_DIR/plot_$(date +%Y%m%d_%H%M%S).html"
vegeta plot "$RESULTS_FILE" > "$PLOT_FILE"

echo -e "\n${GREEN}✓ Reports generated:${NC}"
echo "  - Results: $RESULTS_FILE"
echo "  - Report: $REPORT_FILE"
echo "  - Histogram: $HISTOGRAM_FILE"
echo "  - Plot: $PLOT_FILE"

# Extract key metrics
SUCCESS_RATE=$(grep "Success" "$REPORT_FILE" | awk '{print $NF}')
P50=$(grep "50th" "$REPORT_FILE" | awk '{print $NF}')
P95=$(grep "95th" "$REPORT_FILE" | awk '{print $NF}')
P99=$(grep "99th" "$REPORT_FILE" | awk '{print $NF}')
MAX=$(grep "Max" "$REPORT_FILE" | awk '{print $NF}')

echo -e "\n${GREEN}=== Key Metrics ===${NC}"
echo "Success Rate: $SUCCESS_RATE"
echo "P50 Latency: $P50"
echo "P95 Latency: $P95"
echo "P99 Latency: $P99"
echo "Max Latency: $MAX"

# Check if test passed
echo -e "\n${GREEN}=== Test Validation ===${NC}"

# Success rate should be >= 99%
SUCCESS_PCT=$(echo "$SUCCESS_RATE" | tr -d '%')
if (( $(echo "$SUCCESS_PCT >= 99" | bc -l) )); then
    echo -e "${GREEN}✓ Success rate >= 99%${NC}"
else
    echo -e "${RED}✗ Success rate < 99%${NC}"
fi

# P95 latency should be < 500ms
P95_MS=$(echo "$P95" | sed 's/ms//' | sed 's/s/*1000/' | bc)
if (( $(echo "$P95_MS < 500" | bc -l) )); then
    echo -e "${GREEN}✓ P95 latency < 500ms${NC}"
else
    echo -e "${YELLOW}⚠ P95 latency >= 500ms${NC}"
fi

# P99 latency should be < 1000ms
P99_MS=$(echo "$P99" | sed 's/ms//' | sed 's/s/*1000/' | bc)
if (( $(echo "$P99_MS < 1000" | bc -l) )); then
    echo -e "${GREEN}✓ P99 latency < 1000ms${NC}"
else
    echo -e "${YELLOW}⚠ P99 latency >= 1000ms${NC}"
fi

# Cleanup
rm -f "$TARGETS_FILE"

echo -e "\n${GREEN}=== Load Test Complete ===${NC}"
echo "Open $PLOT_FILE in a browser to view the latency plot"
