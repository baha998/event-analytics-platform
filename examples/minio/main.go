package main

import (
	"context"
	"log"
	"os"

	"github.com/rs/zerolog"

	"devops-fall2025-23b031152/internal/storage"
)

func main() {
	// Initialize logger
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	// Create MinIO client with endpoint localhost:9002
	minioClient, err := storage.NewMinIOClient(storage.MinIOConfig{
		Endpoint:        "localhost:9002",
		AccessKeyID:     "admin",
		SecretAccessKey: "admin123",
		UseSSL:          false,
	}, logger)
	if err != nil {
		log.Fatalf("Failed to create MinIO client: %v", err)
	}

	ctx := context.Background()

	// Example 1: Create bucket
	bucketName := "analytics-data"
	if err := minioClient.CreateBucket(ctx, bucketName); err != nil {
		log.Fatalf("Failed to create bucket: %v", err)
	}
	log.Printf("Bucket '%s' ready", bucketName)

	// Example 2: Upload file
	// Create a test file first
	testFile := "test.txt"
	content := []byte("Hello, MinIO! This is a test file.")
	if err := os.WriteFile(testFile, content, 0644); err != nil {
		log.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(testFile)

	if err := minioClient.UploadFile(ctx, bucketName, "test.txt", testFile); err != nil {
		log.Fatalf("Failed to upload file: %v", err)
	}
	log.Printf("File uploaded successfully")

	log.Println("MinIO operations completed successfully!")
	log.Println("Access MinIO Console at: http://localhost:9001")
	log.Println("Login: admin / admin123")
}
