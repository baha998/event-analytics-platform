#!/bin/bash

# Deployment script for Real-Time Event Analytics
# This script handles deployment with automatic rollback on failure

set -e  # Exit on error

# Configuration
DEPLOY_DIR="/opt/deployments/analytics"
BACKUP_DIR="/opt/deployments/backups"
IMAGE_NAME="${DOCKER_IMAGE:-project-go-api}"
IMAGE_TAG="${DOCKER_TAG:-latest}"
HEALTH_CHECK_URL="${HEALTH_URL:-http://localhost:8080/health}"
MAX_HEALTH_RETRIES=30
HEALTH_RETRY_DELAY=10

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Create directories if they don't exist
mkdir -p "$DEPLOY_DIR"
mkdir -p "$BACKUP_DIR"

# Backup current deployment
backup_deployment() {
    if [ -d "$DEPLOY_DIR" ]; then
        BACKUP_NAME="backup-$(date +%Y%m%d-%H%M%S)"
        log_info "Backing up current deployment to $BACKUP_NAME"
        cp -r "$DEPLOY_DIR" "$BACKUP_DIR/$BACKUP_NAME"
        
        # Keep only last 5 backups
        cd "$BACKUP_DIR"
        ls -t | tail -n +6 | xargs -r rm -rf
    fi
}

# Rollback to previous deployment
rollback_deployment() {
    log_error "Deployment failed! Rolling back..."
    
    LATEST_BACKUP=$(ls -t "$BACKUP_DIR" | head -1)
    
    if [ -n "$LATEST_BACKUP" ]; then
        log_info "Restoring from backup: $LATEST_BACKUP"
        rm -rf "$DEPLOY_DIR"
        cp -r "$BACKUP_DIR/$LATEST_BACKUP" "$DEPLOY_DIR"
        
        cd "$DEPLOY_DIR"
        docker-compose down
        docker-compose up -d
        
        log_info "Rollback completed"
    else
        log_error "No backup found for rollback!"
        exit 1
    fi
}

# Health check function
health_check() {
    local url=$1
    local retries=$2
    local delay=$3
    
    log_info "Performing health checks..."
    
    for i in $(seq 1 $retries); do
        if curl -f -s "$url" | grep -q "ok"; then
            log_info "✓ Health check passed!"
            return 0
        else
            log_warn "Health check attempt $i/$retries failed, retrying in ${delay}s..."
            sleep "$delay"
        fi
    done
    
    log_error "Health check failed after $retries attempts"
    return 1
}

# Check if services are healthy
check_services() {
    log_info "Checking service health status..."
    
    for i in $(seq 1 30); do
        UNHEALTHY=$(docker-compose ps | grep -c "unhealthy" || true)
        
        if [ "$UNHEALTHY" -eq 0 ]; then
            log_info "✓ All services are healthy!"
            return 0
        else
            log_warn "Waiting for services to become healthy... ($i/30)"
            sleep 10
        fi
    done
    
    log_error "Services failed to become healthy"
    docker-compose ps
    return 1
}

# Main deployment function
deploy() {
    log_info "Starting deployment..."
    log_info "Image: $IMAGE_NAME:$IMAGE_TAG"
    
    # Backup current deployment
    backup_deployment
    
    # Change to deployment directory
    cd "$DEPLOY_DIR"
    
    # Pull latest images
    log_info "Pulling latest Docker images..."
    docker-compose pull || {
        log_error "Failed to pull images"
        rollback_deployment
        exit 1
    }
    
    # Deploy services
    log_info "Deploying services..."
    docker-compose up -d --remove-orphans || {
        log_error "Failed to start services"
        rollback_deployment
        exit 1
    }
    
    # Wait for services to be healthy
    check_services || {
        rollback_deployment
        exit 1
    }
    
    # Perform health check
    health_check "$HEALTH_CHECK_URL" "$MAX_HEALTH_RETRIES" "$HEALTH_RETRY_DELAY" || {
        rollback_deployment
        exit 1
    }
    
    # Show deployment status
    log_info "Deployment Status:"
    docker-compose ps
    
    log_info "✓ Deployment completed successfully!"
}

# Stop deployment
stop() {
    log_info "Stopping services..."
    cd "$DEPLOY_DIR"
    docker-compose down
    log_info "Services stopped"
}

# Show logs
logs() {
    cd "$DEPLOY_DIR"
    docker-compose logs -f "${1:-go-api}"
}

# Show status
status() {
    cd "$DEPLOY_DIR"
    docker-compose ps
}

# Main script
case "${1:-deploy}" in
    deploy)
        deploy
        ;;
    rollback)
        rollback_deployment
        ;;
    stop)
        stop
        ;;
    logs)
        logs "$2"
        ;;
    status)
        status
        ;;
    health)
        health_check "$HEALTH_CHECK_URL" 5 2
        ;;
    *)
        echo "Usage: $0 {deploy|rollback|stop|logs|status|health}"
        echo ""
        echo "Commands:"
        echo "  deploy   - Deploy the application (default)"
        echo "  rollback - Rollback to previous deployment"
        echo "  stop     - Stop all services"
        echo "  logs     - Show logs (optional: service name)"
        echo "  status   - Show service status"
        echo "  health   - Check application health"
        exit 1
        ;;
esac
