package main

import (
	"fmt"
	"log"

	"devops-fall2025-23b031152/internal/batch"
)

func main() {
	// Create sample events
	events := []batch.EventParquet{
		{
			EventID:   "evt-001",
			EventType: "click",
			UserID:    "user123",
			PageURL:   "/home",
			Timestamp: "2025-12-13T10:00:00Z",
		},
		{
			EventID:   "evt-002",
			EventType: "click",
			UserID:    "user456",
			PageURL:   "/products",
			Timestamp: "2025-12-13T10:01:00Z",
		},
		{
			EventID:   "evt-003",
			EventType: "transaction",
			UserID:    "user123",
			PageURL:   "/checkout",
			Timestamp: "2025-12-13T10:05:00Z",
		},
		{
			EventID:   "evt-004",
			EventType: "click",
			UserID:    "user789",
			PageURL:   "/about",
			Timestamp: "2025-12-13T10:10:00Z",
		},
	}

	// Write events to Parquet file
	outputPath := "events.parquet"
	if err := batch.WriteEventsToParquet(events, outputPath); err != nil {
		log.Fatalf("Failed to write parquet file: %v", err)
	}

	fmt.Printf("✓ Successfully wrote %d events to %s\n", len(events), outputPath)
	fmt.Println("✓ Compression: Snappy")
	fmt.Println("✓ Format: Apache Parquet")

	// Example with custom batch size
	outputPath2 := "events_batched.parquet"
	if err := batch.WriteEventsToParquetWithBatching(events, outputPath2, 2); err != nil {
		log.Fatalf("Failed to write batched parquet file: %v", err)
	}

	fmt.Printf("✓ Successfully wrote %d events to %s (batch size: 2)\n", len(events), outputPath2)
}
