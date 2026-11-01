package main

import (
	"devops-fall2025-23b031152/pkg/auth"
	"fmt"
	"os"
	"time"
)

// GenerateTokenCLI is a helper function to generate tokens from command line
// Usage: go run ./cmd/token-gen <username> <role>
func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: go run ./cmd/token-gen <username> <role>")
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
		fmt.Fprintln(os.Stderr, "JWT_SECRET_KEY is required")
		os.Exit(1)
	}

	jwtManager := auth.NewJWTManager(secretKey, 24*time.Hour, 7*24*time.Hour)
	token, err := jwtManager.GenerateToken(username, role)
	if err != nil {
		fmt.Printf("Error generating token: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Token for user '%s' with role '%s':\n", username, role)
	fmt.Println(token)
}
