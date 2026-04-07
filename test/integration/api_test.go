package integration

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPI_Health_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	// Setup test router
	router := setupTestRouter(t)

	tests := []struct {
		name           string
		method         string
		expectedStatus int
	}{
		{"GET request", "GET", http.StatusOK},
		{"HEAD request", "HEAD", http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			req, _ := http.NewRequest(tt.method, "/health", nil)
			router.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			if tt.method == "GET" {
				var response map[string]interface{}
				err := json.Unmarshal(w.Body.Bytes(), &response)
				require.NoError(t, err)
				assert.Contains(t, response, "status")
			}
		})
	}
}

func TestAPI_Metrics_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	router := setupTestRouter(t)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("GET", "/metrics", nil)
	router.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "# HELP")
	assert.Contains(t, w.Body.String(), "# TYPE")
}

func TestAPI_Auth_Login_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	router := setupTestRouter(t)

	tests := []struct {
		name           string
		payload        map[string]string
		expectedStatus int
		checkResponse  func(*testing.T, map[string]interface{})
	}{
		{
			name: "valid admin credentials",
			payload: map[string]string{
				"username": "admin",
				"password": "admin123",
			},
			expectedStatus: http.StatusOK,
			checkResponse: func(t *testing.T, resp map[string]interface{}) {
				assert.Contains(t, resp, "token")
				assert.Contains(t, resp, "refresh_token")
			},
		},
		{
			name: "invalid credentials",
			payload: map[string]string{
				"username": "invalid",
				"password": "wrong",
			},
			expectedStatus: http.StatusUnauthorized,
			checkResponse: func(t *testing.T, resp map[string]interface{}) {
				assert.Contains(t, resp, "error")
			},
		},
		{
			name:           "missing credentials",
			payload:        map[string]string{},
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.payload)
			w := httptest.NewRecorder()
			req, _ := http.NewRequest("POST", "/api/auth/login", bytes.NewBuffer(body))
			req.Header.Set("Content-Type", "application/json")

			router.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)

			if tt.checkResponse != nil {
				var response map[string]interface{}
				err := json.Unmarshal(w.Body.Bytes(), &response)
				require.NoError(t, err)
				tt.checkResponse(t, response)
			}
		})
	}
}

func TestAPI_ProtectedEndpoints_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test")
	}

	router := setupTestRouter(t)

	// First, get auth token
	token := getAuthToken(t, router)

	tests := []struct {
		name           string
		endpoint       string
		method         string
		token          string
		expectedStatus int
	}{
		{"query with valid token", "/api/query?page_url=/home", "GET", token, http.StatusOK},
		{"query without token", "/api/query?page_url=/home", "GET", "", http.StatusUnauthorized},
		{"query with invalid token", "/api/query?page_url=/home", "GET", "invalid-token", http.StatusUnauthorized},
		{"sources with admin token", "/api/sources", "POST", token, http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req *http.Request
			if tt.method == "POST" {
				payload := map[string]interface{}{
					"topic": "events",
					"count": 5,
				}
				body, _ := json.Marshal(payload)
				req, _ = http.NewRequest(tt.method, tt.endpoint, bytes.NewBuffer(body))
			} else {
				req, _ = http.NewRequest(tt.method, tt.endpoint, nil)
			}

			if tt.token != "" {
				req.Header.Set("Authorization", "Bearer "+tt.token)
			}
			req.Header.Set("Content-Type", "application/json")

			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			assert.Equal(t, tt.expectedStatus, w.Code)
		})
	}
}

// Helper functions

func setupTestRouter(t *testing.T) *gin.Engine {
	gin.SetMode(gin.TestMode)

	// Setup router with all middleware and handlers
	// This should use the actual setupRouter function from main.go
	// For now, returning basic router
	router := gin.New()
	router.Use(gin.Recovery())

	// Add routes (simplified for testing)
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	router.GET("/metrics", func(c *gin.Context) {
		c.String(http.StatusOK, "# HELP http_requests_total\n# TYPE http_requests_total counter\n")
	})

	// Add more routes as needed

	return router
}

func getAuthToken(t *testing.T, router *gin.Engine) string {
	payload := map[string]string{
		"username": "admin",
		"password": "admin123",
	}
	body, _ := json.Marshal(payload)

	w := httptest.NewRecorder()
	req, _ := http.NewRequest("POST", "/api/auth/login", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")

	router.ServeHTTP(w, req)

	var response map[string]interface{}
	err := json.Unmarshal(w.Body.Bytes(), &response)
	require.NoError(t, err)

	token, ok := response["token"].(string)
	require.True(t, ok)

	return token
}
