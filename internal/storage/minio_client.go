package storage

import (
	"context"
	"fmt"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/rs/zerolog"
)

// MinIOClient wraps MinIO client with convenience methods
type MinIOClient struct {
	client *minio.Client
	log    zerolog.Logger
}

// MinIOConfig holds MinIO connection configuration
type MinIOConfig struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	UseSSL          bool
}

// NewMinIOClient creates a new MinIO client
// Default endpoint is localhost:9002
func NewMinIOClient(config MinIOConfig, log zerolog.Logger) (*MinIOClient, error) {
	// Use default endpoint if not specified
	if config.Endpoint == "" {
		config.Endpoint = "localhost:9002"
	}

	// Initialize MinIO client
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, ""),
		Secure: config.UseSSL,
	})
	if err != nil {
		log.Error().Err(err).Msg("Failed to initialize MinIO client")
		return nil, fmt.Errorf("failed to initialize MinIO client: %w", err)
	}

	log.Info().
		Str("endpoint", config.Endpoint).
		Bool("ssl", config.UseSSL).
		Msg("MinIO client initialized successfully")

	return &MinIOClient{
		client: client,
		log:    log,
	}, nil
}

// CreateBucket creates a bucket if it doesn't exist
func (m *MinIOClient) CreateBucket(ctx context.Context, bucketName string) error {
	// Check if bucket already exists
	exists, err := m.client.BucketExists(ctx, bucketName)
	if err != nil {
		m.log.Error().
			Err(err).
			Str("bucket", bucketName).
			Msg("Failed to check bucket existence")
		return fmt.Errorf("failed to check bucket existence: %w", err)
	}

	if exists {
		m.log.Debug().
			Str("bucket", bucketName).
			Msg("Bucket already exists")
		return nil
	}

	// Create the bucket
	err = m.client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{})
	if err != nil {
		m.log.Error().
			Err(err).
			Str("bucket", bucketName).
			Msg("Failed to create bucket")
		return fmt.Errorf("failed to create bucket: %w", err)
	}

	m.log.Info().
		Str("bucket", bucketName).
		Msg("Bucket created successfully")

	return nil
}

// UploadFile uploads a file to MinIO bucket
func (m *MinIOClient) UploadFile(ctx context.Context, bucketName, objectName, filePath string) error {
	// Ensure bucket exists
	if err := m.CreateBucket(ctx, bucketName); err != nil {
		return err
	}

	// Upload the file
	info, err := m.client.FPutObject(ctx, bucketName, objectName, filePath, minio.PutObjectOptions{
		ContentType: "application/octet-stream",
	})
	if err != nil {
		m.log.Error().
			Err(err).
			Str("bucket", bucketName).
			Str("object", objectName).
			Str("file", filePath).
			Msg("Failed to upload file")
		return fmt.Errorf("failed to upload file: %w", err)
	}

	m.log.Info().
		Str("bucket", bucketName).
		Str("object", objectName).
		Int64("size", info.Size).
		Str("etag", info.ETag).
		Msg("File uploaded successfully")

	return nil
}
