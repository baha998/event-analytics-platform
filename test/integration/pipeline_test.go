package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"devops-fall2025-23b031152/pkg/kafka"
)

// Requires the core Compose stack and --profile storage.
func TestPipeline_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Compose and storage profile")
	}
	setting := func(key, fallback string) string {
		if v := os.Getenv(key); v != "" {
			return v
		}
		return fallback
	}
	base := setting("API_TEST_URL", "http://localhost:8080")
	client := &http.Client{Timeout: 30 * time.Second}
	var token string
	post := func(path string, body any, target any) {
		t.Helper()
		payload, err := json.Marshal(body)
		require.NoError(t, err)
		req, err := http.NewRequest(http.MethodPost, base+path, bytes.NewReader(payload))
		require.NoError(t, err)
		req.Header.Set("Content-Type", "application/json")
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := client.Do(req)
		require.NoError(t, err)
		defer response.Body.Close()
		require.Less(t, response.StatusCode, 300, "request %s", path)
		if target != nil {
			require.NoError(t, json.NewDecoder(response.Body).Decode(target))
		}
	}
	var login struct {
		Token string `json:"token"`
	}
	post("/api/auth/login", map[string]string{"username": setting("AUTH_ADMIN_USERNAME", "admin"), "password": setting("AUTH_ADMIN_PASSWORD", "admin123")}, &login)
	token = login.Token
	producer, err := kafka.NewProducer(kafka.ProducerConfig{Brokers: []string{setting("KAFKA_TEST_BROKER", "localhost:29092")}, Topic: "events"}, zerolog.Nop())
	require.NoError(t, err)
	defer producer.Close()
	page := "/integration/" + uuid.NewString()
	date := time.Now().UTC().AddDate(0, 0, -1)
	event := &kafka.Event{EventID: uuid.NewString(), EventType: "click", UserID: "pipeline-test", PageURL: page, Timestamp: date.Format(time.RFC3339)}
	require.NoError(t, producer.SendEvent(event))
	require.Eventually(t, func() bool {
		req, _ := http.NewRequest(http.MethodGet, base+"/api/query?page_url="+url.QueryEscape(page), nil)
		req.Header.Set("Authorization", "Bearer "+token)
		response, err := client.Do(req)
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var result struct {
			Data []struct {
				Count uint64 `json:"click_count"`
			} `json:"data"`
		}
		return json.NewDecoder(response.Body).Decode(&result) == nil && len(result.Data) == 1 && result.Data[0].Count == 1
	}, 30*time.Second, 500*time.Millisecond)
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{setting("CLICKHOUSE_TEST_ADDR", "localhost:9000")}, Auth: clickhouse.Auth{Database: "analytics", Username: "default", Password: setting("CLICKHOUSE_PASSWORD", "clickhouse123")}})
	require.NoError(t, err)
	defer conn.Close()
	for i := 0; i < 2; i++ {
		post("/api/process", map[string]string{"metric_name": "click_count", "date": date.Format("2006-01-02")}, nil)
		var total float64
		require.NoError(t, conn.QueryRow(context.Background(), "SELECT sum(value) FROM metrics WHERE metric_name='click_count_daily_summary' AND dimensions['dimension'] = ?", page).Scan(&total))
		require.Equal(t, float64(1), total, "daily recomputation must not double counts")
	}
	post("/api/batch/export", nil, nil)
	storage, err := minio.New(setting("MINIO_TEST_ENDPOINT", "localhost:9002"), &minio.Options{Creds: credentials.NewStaticV4(setting("MINIO_ACCESS_KEY", "admin"), setting("MINIO_SECRET_KEY", "admin123"), ""), Secure: false})
	require.NoError(t, err)
	object, err := storage.StatObject(context.Background(), "events", fmt.Sprintf("date=%s/events.parquet", date.Format("2006-01-02")), minio.StatObjectOptions{})
	require.NoError(t, err)
	require.Greater(t, object.Size, int64(0))
}
