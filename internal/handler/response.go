package handler

import "github.com/gin-gonic/gin"

// APIError represents a consistent error payload.
type APIError struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Details interface{} `json:"details,omitempty"`
}

func respondError(c *gin.Context, status int, code, message string, details interface{}) {
	c.AbortWithStatusJSON(status, gin.H{
		"error": APIError{
			Code:    code,
			Message: message,
			Details: details,
		},
	})
}

func respondSuccess(c *gin.Context, status int, payload interface{}) {
	c.JSON(status, payload)
}
