package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/go-playground/validator/v10"
	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/internal/config"
	"devops-fall2025-23b031152/pkg/auth"
)

// AuthHandler manages authentication endpoints.
type AuthHandler struct {
	jwtManager *auth.JWTManager
	cfg        config.AuthConfig
	validate   *validator.Validate
	log        zerolog.Logger
}

// NewAuthHandler constructs a new AuthHandler.
func NewAuthHandler(jwtManager *auth.JWTManager, cfg config.AuthConfig, log zerolog.Logger) *AuthHandler {
	return &AuthHandler{
		jwtManager: jwtManager,
		cfg:        cfg,
		validate:   validator.New(),
		log:        log,
	}
}

type loginRequest struct {
	Username string `json:"username" validate:"required"`
	Password string `json:"password" validate:"required"`
}

type loginResponse struct {
	Token string `json:"token"`
	Role  string `json:"role"`
}

// Login handles POST /api/auth/login.
func (h *AuthHandler) Login(c *gin.Context) {
	var req loginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, http.StatusBadRequest, "invalid_request", "Invalid request body", err.Error())
		return
	}

	if err := h.validate.Struct(req); err != nil {
		respondError(c, http.StatusBadRequest, "validation_error", "Validation failed", err.Error())
		return
	}

	role, ok := h.validateCredentials(req.Username, req.Password)
	if !ok {
		h.log.Warn().Str("username", req.Username).Msg("Invalid credentials")
		respondError(c, http.StatusUnauthorized, "invalid_credentials", "Invalid username or password", nil)
		return
	}

	token, err := h.jwtManager.GenerateToken(req.Username, role)
	if err != nil {
		h.log.Error().Err(err).Msg("Failed to generate token")
		respondError(c, http.StatusInternalServerError, "token_generation_failed", "Could not generate token", nil)
		return
	}

	respondSuccess(c, http.StatusOK, loginResponse{
		Token: token,
		Role:  role,
	})
}

func (h *AuthHandler) validateCredentials(username, password string) (string, bool) {
	switch {
	case username == h.cfg.AdminUsername && password == h.cfg.AdminPassword:
		return "admin", true
	case username == h.cfg.ViewerUsername && password == h.cfg.ViewerPassword:
		return "viewer", true
	default:
		return "", false
	}
}
