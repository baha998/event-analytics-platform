package auth

import (
	"fmt"
	"os"
	"time"
)

// GenerateTokenCLI is a helper function to generate tokens from command line
// Usage: go run pkg/auth/token_gen.go <username> <role>
func GenerateTokenCLI() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run pkg/auth/token_gen.go <username> <role>")
		fmt.Println("Roles: admin, viewer")
		os.Exit(1)
	}

	username := os.Args[1]
	role := os.Args[2]

	if role != "admin" && role != "viewer" {
		fmt.Println("Error: role must be 'admin' or 'viewer'")
		os.Exit(1)
	}

	secretKey := os.Getenv("JWT_SECRET_KEY")
	if secretKey == "" {
		secretKey = "your-secret-key-change-in-production"
	}

	jwtManager := NewJWTManager(secretKey, 24*time.Hour, 7*24*time.Hour)
	token, err := jwtManager.GenerateToken(username, role)
	if err != nil {
		fmt.Printf("Error generating token: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Token for user '%s' with role '%s':\n", username, role)
	fmt.Println(token)
}
