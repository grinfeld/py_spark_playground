#!/bin/bash

# Docker utilities for PySpark with MinIO setup

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
}

# Function to check if Docker Compose is available
check_docker_compose() {
    if ! docker-compose --version > /dev/null 2>&1; then
        print_error "Docker Compose is not available. Please install Docker Compose and try again."
        exit 1
    fi
}

# Function to start services
start_services() {
    print_status "Starting PySpark with MinIO services..."
    check_docker
    check_docker_compose
    
    if [ "$1" = "jupyter" ]; then
        print_status "Starting with Jupyter Notebook..."
        docker-compose --profile jupyter up -d
    else
        docker-compose up -d
    fi
    
    print_success "Services started successfully!"
    print_status "MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
    print_status "Spark UI: http://localhost:4040"
    if [ "$1" = "jupyter" ]; then
        print_status "Jupyter Notebook: http://localhost:8888"
    fi
}

# Function to stop services
stop_services() {
    print_status "Stopping services..."
    docker-compose down
    print_success "Services stopped successfully!"
}

# Function to restart services
restart_services() {
    print_status "Restarting services..."
    docker-compose restart
    print_success "Services restarted successfully!"
}

# Function to view logs
view_logs() {
    if [ -z "$1" ]; then
        print_status "Showing all logs (Ctrl+C to exit)..."
        docker-compose logs -f
    else
        print_status "Showing logs for $1 (Ctrl+C to exit)..."
        docker-compose logs -f "$1"
    fi
}

# Function to check service status
check_status() {
    print_status "Checking service status..."
    docker-compose ps
}

# Function to clean up
cleanup() {
    print_warning "This will remove all containers, networks, and volumes!"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_status "Cleaning up..."
        docker-compose down -v --rmi all
        print_success "Cleanup completed!"
    else
        print_status "Cleanup cancelled."
    fi
}

# Function to access container shell
shell() {
    if [ -z "$1" ]; then
        print_status "Accessing PySpark application shell..."
        docker-compose exec pyspark-app bash
    else
        print_status "Accessing $1 shell..."
        docker-compose exec "$1" bash
    fi
}

# Function to show help
show_help() {
    echo "PySpark with MinIO Docker Utilities"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  start [jupyter]    Start services (optionally with Jupyter)"
    echo "  stop               Stop all services"
    echo "  restart            Restart all services"
    echo "  logs [service]     View logs (all or specific service)"
    echo "  status             Check service status"
    echo "  shell [service]    Access container shell"
    echo "  cleanup            Remove all containers, networks, and volumes"
    echo "  help               Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 start           # Start basic services"
    echo "  $0 start jupyter   # Start with Jupyter Notebook"
    echo "  $0 logs pyspark-app # View PySpark application logs"
    echo "  $0 shell minio     # Access MinIO container shell"
}

# Main script logic
case "${1:-help}" in
    start)
        start_services "$2"
        ;;
    stop)
        stop_services
        ;;
    restart)
        restart_services
        ;;
    logs)
        view_logs "$2"
        ;;
    status)
        check_status
        ;;
    shell)
        shell "$2"
        ;;
    cleanup)
        cleanup
        ;;
    help|*)
        show_help
        ;;
esac
