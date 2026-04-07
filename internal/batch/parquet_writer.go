package batch

import (
	"fmt"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

// EventParquet represents an event in Parquet format
type EventParquet struct {
	EventID   string `parquet:"name=event_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	EventType string `parquet:"name=event_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	UserID    string `parquet:"name=user_id, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	PageURL   string `parquet:"name=page_url, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	Timestamp string `parquet:"name=timestamp, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

// WriteEventsToParquet writes events to a Parquet file with Snappy compression
func WriteEventsToParquet(events []EventParquet, outputPath string) error {
	if len(events) == 0 {
		return fmt.Errorf("no events to write")
	}

	// Create local file writer
	fw, err := local.NewLocalFileWriter(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create file writer: %w", err)
	}
	defer fw.Close()

	// Create parquet writer with Snappy compression
	pw, err := writer.NewParquetWriter(fw, new(EventParquet), 4)
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Set compression type to Snappy
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// Write events in batches
	for _, event := range events {
		if err := pw.Write(event); err != nil {
			pw.WriteStop()
			return fmt.Errorf("failed to write event: %w", err)
		}
	}

	// Stop writing and flush data
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to finalize parquet file: %w", err)
	}

	return nil
}

// WriteEventsToParquetWithBatching writes events in configurable batch sizes
func WriteEventsToParquetWithBatching(events []EventParquet, outputPath string, batchSize int) error {
	if len(events) == 0 {
		return fmt.Errorf("no events to write")
	}

	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}

	// Create local file writer
	fw, err := local.NewLocalFileWriter(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create file writer: %w", err)
	}
	defer fw.Close()

	// Create parquet writer with specified parallelism
	parallelism := 4
	if len(events) < 10000 {
		parallelism = 1
	}

	pw, err := writer.NewParquetWriter(fw, new(EventParquet), int64(parallelism))
	if err != nil {
		return fmt.Errorf("failed to create parquet writer: %w", err)
	}

	// Set compression type to Snappy
	pw.CompressionType = parquet.CompressionCodec_SNAPPY

	// Write events in batches
	totalWritten := 0
	for i := 0; i < len(events); i += batchSize {
		end := i + batchSize
		if end > len(events) {
			end = len(events)
		}

		batch := events[i:end]
		for _, event := range batch {
			if err := pw.Write(event); err != nil {
				pw.WriteStop()
				return fmt.Errorf("failed to write event at index %d: %w", totalWritten, err)
			}
			totalWritten++
		}
	}

	// Stop writing and flush data
	if err := pw.WriteStop(); err != nil {
		return fmt.Errorf("failed to finalize parquet file: %w", err)
	}

	return nil
}
