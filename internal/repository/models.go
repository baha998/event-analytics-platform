package repository

// Event represents an analytics event
type Event struct {
	EventID   string `json:"event_id" validate:"required,uuid4"`
	Timestamp int64  `json:"timestamp" validate:"required"`
	PageURL   string `json:"page_url" validate:"required,min=1"`
	UserID    int    `json:"user_id" validate:"required,gt=0"`
	Source    string `json:"source,omitempty" validate:"omitempty"`
}

// QueryRequest represents a query request
type QueryRequest struct {
	StartTime int64  `json:"start_time,omitempty" validate:"omitempty,gte=0"`
	EndTime   int64  `json:"end_time,omitempty" validate:"omitempty,gte=0"`
	PageURL   string `json:"page_url,omitempty" validate:"omitempty,min=1"`
	UserID    int    `json:"user_id,omitempty" validate:"omitempty,gt=0"`
	Limit     int    `json:"limit,omitempty" validate:"omitempty,gt=0"`
}

// QueryResult represents query results
type QueryResult struct {
	PageURL    string `json:"page_url"`
	ClickCount int64  `json:"click_count"`
	UserID     int    `json:"user_id,omitempty"`
}

// SourceRequest represents a source creation request
type SourceRequest struct {
	Topic    string `json:"topic" validate:"required,min=3"`
	Count    int    `json:"count" validate:"required,gt=0,lte=10000"`
	Interval int    `json:"interval" validate:"required,gte=10,lte=60000"`
}
