# Build stage
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application from cmd/api
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -ldflags '-extldflags "-static"' -o main ./cmd/api

# Final stage
FROM alpine:3.21

RUN apk --no-cache add ca-certificates wget

RUN adduser -D -u 10001 app
WORKDIR /app

# Copy the binary from builder
COPY --from=builder /build/main .
USER app

# Expose port
EXPOSE 8080

# Run the application
CMD ["./main"]
