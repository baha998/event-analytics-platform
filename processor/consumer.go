package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/gin-gonic/gin"
)

// ClickAnalyticsResponse represents aggregated click data
type ClickAnalyticsResponse struct {
	PageURL    string `json:"page_url"`
	ClickCount int64  `json:"click_count"`
}

// GetClickAnalyticsHandler retrieves aggregated click data from ClickHouse
func GetClickAnalyticsHandler(conn clickhouse.Conn) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get optional query parameters
		startTime := c.DefaultQuery("start_time", "0")
		endTime := c.DefaultQuery("end_time", fmt.Sprintf("%d", time.Now().Unix()))
		limit := c.DefaultQuery("limit", "10")

		// Query ClickHouse for aggregated data
		query := `
			SELECT 
				page_url,
				count() as click_count
			FROM events
			WHERE timestamp >= toInt64(?) AND timestamp <= toInt64(?)
			GROUP BY page_url
			ORDER BY click_count DESC
			LIMIT ?
		`

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		rows, err := conn.Query(ctx, query, startTime, endTime, limit)
		if err != nil {
			log.Printf("Error querying ClickHouse: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Failed to query analytics data",
			})
			return
		}
		defer rows.Close()

		// Parse results
		var results []ClickAnalyticsResponse
		for rows.Next() {
			var result ClickAnalyticsResponse
			if err := rows.Scan(&result.PageURL, &result.ClickCount); err != nil {
				log.Printf("Error scanning row: %v", err)
				continue
			}
			results = append(results, result)
		}

		if err := rows.Err(); err != nil {
			log.Printf("Error iterating rows: %v", err)
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "Failed to parse analytics data",
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"data":  results,
			"count": len(results),
		})
	}
}

// InsertClickEvent inserts a click event into ClickHouse
func InsertClickEvent(conn clickhouse.Conn, event ClickEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
		INSERT INTO events (event_id, timestamp, page_url, user_id)
		VALUES (?, ?, ?, ?)
	`

	err := conn.Exec(ctx, query, event.EventID, event.Timestamp, event.PageURL, event.UserID)
	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	return nil
}

// BatchInsertClickEvents inserts multiple events in a single batch
func BatchInsertClickEvents(conn clickhouse.Conn, events []ClickEvent) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	batch, err := conn.PrepareBatch(ctx, "INSERT INTO events (event_id, timestamp, page_url, user_id)")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, event := range events {
		err := batch.Append(event.EventID, event.Timestamp, event.PageURL, event.UserID)
		if err != nil {
			return fmt.Errorf("failed to append event to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// UnmarshalClickEvent unmarshals JSON to ClickEvent
func UnmarshalClickEvent(data []byte) (*ClickEvent, error) {
	var event ClickEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return nil, fmt.Errorf("failed to unmarshal click event: %w", err)
	}
	return &event, nil
}
