package middleware

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/pkg/auth"
)

// AuthMiddleware creates JWT authentication middleware
func AuthMiddleware(jwtManager *auth.JWTManager, log zerolog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			log.Warn().Msg("Missing authorization header")
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"code":    "missing_authorization",
					"message": "Missing authorization header",
				},
			})
			c.Abort()
			return
		}

		parts := strings.Split(authHeader, " ")
		if len(parts) != 2 || parts[0] != "Bearer" {
			log.Warn().Str("header", authHeader).Msg("Invalid authorization header format")
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"code":    "invalid_authorization_format",
					"message": "Invalid authorization header format",
				},
			})
			c.Abort()
			return
		}

		token := parts[1]
		claims, err := jwtManager.ValidateToken(token)
		if err != nil {
			log.Warn().Err(err).Msg("Invalid token")
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"code":    "invalid_token",
					"message": "Invalid or expired token",
				},
			})
			c.Abort()
			return
		}

		// Set user info in context
		c.Set("username", claims.Username)
		c.Set("role", claims.Role)
		c.Set("claims", claims)

		c.Next()
	}
}

// RBACMiddleware creates role-based access control middleware
func RBACMiddleware(requiredRole string, log zerolog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		claims, exists := c.Get("claims")
		if !exists {
			log.Warn().Msg("Claims not found in context")
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"code":    "unauthorized",
					"message": "Authorization required",
				},
			})
			c.Abort()
			return
		}

		cl, ok := claims.(*auth.Claims)
		if !ok {
			log.Warn().Msg("Invalid claims type")
			c.JSON(http.StatusForbidden, gin.H{
				"error": gin.H{
					"code":    "invalid_claims",
					"message": "Invalid token claims",
				},
			})
			c.Abort()
			return
		}

		// Admin has access to everything
		if cl.IsAdmin() {
			c.Next()
			return
		}

		// Check if user has required role
		if !cl.HasRole(requiredRole) {
			log.Warn().
				Str("required_role", requiredRole).
				Str("user_role", cl.Role).
				Msg("Insufficient permissions")
			c.JSON(http.StatusForbidden, gin.H{
				"error": gin.H{
					"code":    "forbidden",
					"message": "Insufficient permissions",
				},
			})
			c.Abort()
			return
		}

		c.Next()
	}
}

// RequireAdmin requires admin role
func RequireAdmin(log zerolog.Logger) gin.HandlerFunc {
	return RBACMiddleware("admin", log)
}

// RequireViewer requires viewer role (or admin)
func RequireViewer(log zerolog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		claims, exists := c.Get("claims")
		if !exists {
			log.Warn().Msg("Claims not found in context")
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": gin.H{
					"code":    "unauthorized",
					"message": "Authorization required",
				},
			})
			c.Abort()
			return
		}

		cl, ok := claims.(*auth.Claims)
		if !ok {
			log.Warn().Msg("Invalid claims type")
			c.JSON(http.StatusForbidden, gin.H{
				"error": gin.H{
					"code":    "invalid_claims",
					"message": "Invalid token claims",
				},
			})
			c.Abort()
			return
		}

		// Admin or viewer can access
		if cl.IsAdmin() || cl.IsViewer() {
			c.Next()
			return
		}

		log.Warn().Str("user_role", cl.Role).Msg("Insufficient permissions")
		c.JSON(http.StatusForbidden, gin.H{
			"error": gin.H{
				"code":    "forbidden",
				"message": "Insufficient permissions",
			},
		})
		c.Abort()
	}
}
