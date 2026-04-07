package aggregation

import (
	"sync"
	"time"
)

// AggregationType represents the type of aggregation
type AggregationType string

const (
	AggregationCount AggregationType = "COUNT"
	AggregationSum   AggregationType = "SUM"
	AggregationAvg   AggregationType = "AVG"
)

// Window represents a time window for aggregation
type Window struct {
	Start    time.Time
	End      time.Time
	Duration time.Duration
}

// NewTumblingWindow creates a new tumbling window
func NewTumblingWindow(timestamp time.Time, duration time.Duration) Window {
	// Align to window boundary
	start := timestamp.Truncate(duration)
	return Window{
		Start:    start,
		End:      start.Add(duration),
		Duration: duration,
	}
}

// Contains checks if a timestamp falls within this window
func (w Window) Contains(timestamp time.Time) bool {
	return !timestamp.Before(w.Start) && timestamp.Before(w.End)
}

// Key returns a unique key for the window
func (w Window) Key() string {
	return w.Start.Format(time.RFC3339)
}

// AggregationResult represents the result of an aggregation
type AggregationResult struct {
	Timestamp   time.Time
	MetricName  string
	Dimension   string
	Value       float64
	WindowStart time.Time
	WindowEnd   time.Time
	Count       int64
}

// Aggregator handles aggregation logic
type Aggregator struct {
	mu sync.RWMutex
	// Map: window_key -> dimension -> aggregation data
	windows map[string]map[string]*AggregationData
}

// AggregationData holds intermediate aggregation data
type AggregationData struct {
	Count      int64
	Sum        float64
	Values     []float64 // For AVG calculation
	Window     Window
	LastUpdate time.Time
}

// NewAggregator creates a new aggregator
func NewAggregator() *Aggregator {
	return &Aggregator{
		windows: make(map[string]map[string]*AggregationData),
	}
}

// AddEvent adds an event to the aggregation
func (a *Aggregator) AddEvent(timestamp time.Time, dimension string, value float64, windowDuration time.Duration) {
	a.mu.Lock()
	defer a.mu.Unlock()

	window := NewTumblingWindow(timestamp, windowDuration)
	windowKey := window.Key()

	if a.windows[windowKey] == nil {
		a.windows[windowKey] = make(map[string]*AggregationData)
	}

	if a.windows[windowKey][dimension] == nil {
		a.windows[windowKey][dimension] = &AggregationData{
			Window: window,
			Values: make([]float64, 0),
		}
	}

	data := a.windows[windowKey][dimension]
	data.Count++
	data.Sum += value
	data.Values = append(data.Values, value)
	data.LastUpdate = timestamp
}

// GetAggregation retrieves aggregation result for a window and dimension
func (a *Aggregator) GetAggregation(windowKey, dimension string, aggType AggregationType, metricName string) *AggregationResult {
	a.mu.RLock()
	defer a.mu.RUnlock()

	if a.windows[windowKey] == nil {
		return nil
	}

	data, ok := a.windows[windowKey][dimension]
	if !ok {
		return nil
	}

	var value float64
	switch aggType {
	case AggregationCount:
		value = float64(data.Count)
	case AggregationSum:
		value = data.Sum
	case AggregationAvg:
		if data.Count > 0 {
			value = data.Sum / float64(data.Count)
		}
	}

	return &AggregationResult{
		Timestamp:   time.Now(),
		MetricName:  metricName,
		Dimension:   dimension,
		Value:       value,
		WindowStart: data.Window.Start,
		WindowEnd:   data.Window.End,
		Count:       data.Count,
	}
}

// FlushWindow flushes and removes a window, returning all aggregations
func (a *Aggregator) FlushWindow(windowKey string, aggType AggregationType, metricName string) []*AggregationResult {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.windows[windowKey] == nil {
		return nil
	}

	results := make([]*AggregationResult, 0, len(a.windows[windowKey]))
	for dimension, data := range a.windows[windowKey] {
		var value float64
		switch aggType {
		case AggregationCount:
			value = float64(data.Count)
		case AggregationSum:
			value = data.Sum
		case AggregationAvg:
			if data.Count > 0 {
				value = data.Sum / float64(data.Count)
			}
		}

		results = append(results, &AggregationResult{
			Timestamp:   time.Now(),
			MetricName:  metricName,
			Dimension:   dimension,
			Value:       value,
			WindowStart: data.Window.Start,
			WindowEnd:   data.Window.End,
			Count:       data.Count,
		})
	}

	// Remove flushed window
	delete(a.windows, windowKey)
	return results
}

// GetExpiredWindows returns window keys that have expired
func (a *Aggregator) GetExpiredWindows(windowDuration time.Duration) []string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	now := time.Now()
	expired := make([]string, 0)

	for windowKey, dimensions := range a.windows {
		if len(dimensions) == 0 {
			continue
		}
		// Get window from first dimension
		var window Window
		for _, data := range dimensions {
			window = data.Window
			break
		}
		// Window is expired if it's completely in the past
		if window.End.Add(windowDuration).Before(now) {
			expired = append(expired, windowKey)
		}
	}

	return expired
}

// GetWindowKeys returns all current window keys
func (a *Aggregator) GetWindowKeys() []string {
	a.mu.RLock()
	defer a.mu.RUnlock()

	keys := make([]string, 0, len(a.windows))
	for key := range a.windows {
		keys = append(keys, key)
	}
	return keys
}
