package generators

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"

	"devops-fall2025-23b031152/pkg/kafka"
)

// EventGenerator generates realistic test events
type EventGenerator struct {
	rand          *rand.Rand
	pages         []string
	userIDs       []string
	eventTypes    []string
	distributions map[string]float64
}

// NewEventGenerator creates a new event generator
func NewEventGenerator() *EventGenerator {
	return &EventGenerator{
		rand: rand.New(rand.NewSource(time.Now().UnixNano())),
		pages: []string{
			"/",
			"/home",
			"/products",
			"/product/item-1",
			"/product/item-2",
			"/product/item-3",
			"/cart",
			"/checkout",
			"/payment",
			"/confirmation",
			"/about",
			"/contact",
			"/blog",
			"/pricing",
			"/features",
			"/documentation",
			"/support",
			"/login",
			"/signup",
			"/account",
		},
		userIDs:    generateUserIDs(1000),
		eventTypes: []string{"click", "transaction"},
		distributions: map[string]float64{
			"click":       0.85, // 85% clicks
			"transaction": 0.15, // 15% transactions
		},
	}
}

// GenerateEvent generates a single realistic event
func (g *EventGenerator) GenerateEvent() *kafka.Event {
	eventType := g.selectEventType()

	return &kafka.Event{
		EventID:   uuid.New().String(),
		EventType: eventType,
		UserID:    g.selectUserID(),
		PageURL:   g.selectPageURL(eventType),
		Timestamp: time.Now().Format(time.RFC3339),
	}
}

// GenerateEvents generates n events
func (g *EventGenerator) GenerateEvents(count int) []*kafka.Event {
	events := make([]*kafka.Event, count)
	for i := 0; i < count; i++ {
		events[i] = g.GenerateEvent()
	}
	return events
}

// GenerateEventsWithRate generates events at a specific rate (events/sec) for a duration
func (g *EventGenerator) GenerateEventsWithRate(ratePerSec int, duration time.Duration) <-chan *kafka.Event {
	eventChan := make(chan *kafka.Event, ratePerSec*2)

	go func() {
		defer close(eventChan)

		interval := time.Second / time.Duration(ratePerSec)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		timeout := time.After(duration)

		for {
			select {
			case <-timeout:
				return
			case <-ticker.C:
				eventChan <- g.GenerateEvent()
			}
		}
	}()

	return eventChan
}

// GenerateBatchedEvents generates events in batches
func (g *EventGenerator) GenerateBatchedEvents(totalEvents, batchSize int) [][]*kafka.Event {
	batches := make([][]*kafka.Event, 0)

	for i := 0; i < totalEvents; i += batchSize {
		end := i + batchSize
		if end > totalEvents {
			end = totalEvents
		}

		batch := g.GenerateEvents(end - i)
		batches = append(batches, batch)
	}

	return batches
}

// Helper methods

func (g *EventGenerator) selectEventType() string {
	r := g.rand.Float64()
	cumulative := 0.0

	for eventType, prob := range g.distributions {
		cumulative += prob
		if r <= cumulative {
			return eventType
		}
	}

	return "click" // default
}

func (g *EventGenerator) selectUserID() string {
	// Realistic user distribution: some users are more active
	// Use Zipf distribution for realistic user activity
	idx := g.zipfDistribution(len(g.userIDs))
	return g.userIDs[idx]
}

func (g *EventGenerator) selectPageURL(eventType string) string {
	if eventType == "transaction" {
		// Transactions more likely on checkout/payment pages
		transactionPages := []string{"/cart", "/checkout", "/payment", "/confirmation"}
		return transactionPages[g.rand.Intn(len(transactionPages))]
	}

	// Clicks follow realistic browsing patterns
	// Homepage and category pages are more common
	weights := make([]int, len(g.pages))
	for i, page := range g.pages {
		switch {
		case page == "/" || page == "/home":
			weights[i] = 20 // Very common
		case page == "/products" || page == "/pricing" || page == "/features":
			weights[i] = 10 // Common
		case page == "/cart" || page == "/checkout":
			weights[i] = 3 // Less common
		default:
			weights[i] = 5 // Default
		}
	}

	return g.weightedSelect(g.pages, weights)
}

func (g *EventGenerator) zipfDistribution(n int) int {
	// Simplified Zipf distribution
	// Top 20% of users generate 80% of activity
	r := g.rand.Float64()

	if r < 0.8 {
		// 80% of events from top 20% of users
		return g.rand.Intn(n / 5)
	}
	// 20% of events from remaining 80% of users
	return n/5 + g.rand.Intn(n-n/5)
}

func (g *EventGenerator) weightedSelect(items []string, weights []int) string {
	total := 0
	for _, w := range weights {
		total += w
	}

	r := g.rand.Intn(total)
	cumulative := 0

	for i, w := range weights {
		cumulative += w
		if r < cumulative {
			return items[i]
		}
	}

	return items[0]
}

// GenerateUserBehaviorSequence generates a realistic sequence of events for a single user session
func (g *EventGenerator) GenerateUserBehaviorSequence() []*kafka.Event {
	events := make([]*kafka.Event, 0)
	userID := g.selectUserID()

	// Typical user journey
	journey := []string{
		"/home",
		"/products",
		"/product/item-1",
		"/product/item-2",
		"/cart",
		"/checkout",
		"/payment",
		"/confirmation",
	}

	baseTime := time.Now()

	for i, page := range journey {
		// Some users drop off before completing
		if g.rand.Float64() < 0.1*float64(i) { // Increasing drop-off rate
			break
		}

		eventType := "click"
		if page == "/confirmation" {
			eventType = "transaction"
		}

		event := &kafka.Event{
			EventID:   uuid.New().String(),
			EventType: eventType,
			UserID:    userID,
			PageURL:   page,
			Timestamp: baseTime.Add(time.Duration(i*2) * time.Second).Format(time.RFC3339),
		}

		events = append(events, event)

		// Random delay between 1-5 seconds
		baseTime = baseTime.Add(time.Duration(1+g.rand.Intn(4)) * time.Second)
	}

	return events
}

// Helper function to generate user IDs
func generateUserIDs(count int) []string {
	userIDs := make([]string, count)
	for i := 0; i < count; i++ {
		userIDs[i] = fmt.Sprintf("user-%06d", i+1)
	}
	return userIDs
}

// Config for event generation
type GeneratorConfig struct {
	EventsPerSecond int
	Duration        time.Duration
	EventTypeRatio  map[string]float64
	UserCount       int
}

// NewEventGeneratorWithConfig creates a generator with custom configuration
func NewEventGeneratorWithConfig(config GeneratorConfig) *EventGenerator {
	gen := NewEventGenerator()

	if config.EventTypeRatio != nil {
		gen.distributions = config.EventTypeRatio
	}

	if config.UserCount > 0 {
		gen.userIDs = generateUserIDs(config.UserCount)
	}

	return gen
}
