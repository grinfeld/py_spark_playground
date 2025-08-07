#!/bin/bash

# Docker utilities for distributed PySpark with MinIO setup

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

# Function to start distributed services
start_distributed_services() {
    print_status "Starting distributed PySpark with MinIO services..."
    check_docker
    check_docker_compose
    
    docker-compose -f docker-compose-distributed.yml up -d
    
    print_success "Distributed services started successfully!"
    print_status "MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
    print_status "Spark Master UI: http://localhost:8080"
    print_status "Spark Application UI: http://localhost:4040"
    print_status ""
    print_status "Cluster Status:"
    print_status "- 1 Spark Master"
    print_status "- 3 Spark Workers"
    print_status "- 1 Spark Application Driver"
    print_status "- 1 MinIO Server"
}

# Function to stop distributed services
stop_distributed_services() {
    print_status "Stopping distributed services..."
    docker-compose -f docker-compose-distributed.yml down
    print_success "Distributed services stopped successfully!"
}

# Function to restart distributed services
restart_distributed_services() {
    print_status "Restarting distributed services..."
    docker-compose -f docker-compose-distributed.yml restart
    print_success "Distributed services restarted successfully!"
}

# Function to view logs
view_logs() {
    if [ -z "$1" ]; then
        print_status "Showing all distributed logs (Ctrl+C to exit)..."
        docker-compose -f docker-compose-distributed.yml logs -f
    else
        print_status "Showing logs for $1 (Ctrl+C to exit)..."
        docker-compose -f docker-compose-distributed.yml logs -f "$1"
    fi
}

# Function to check service status
check_status() {
    print_status "Checking distributed service status..."
    docker-compose -f docker-compose-distributed.yml ps
}

# Function to clean up
cleanup() {
    print_warning "This will remove all distributed containers, networks, and volumes!"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_status "Cleaning up distributed services..."
        docker-compose -f docker-compose-distributed.yml down -v --rmi all
        print_success "Distributed cleanup completed!"
    else
        print_status "Cleanup cancelled."
    fi
}

# Function to access container shell
shell() {
    if [ -z "$1" ]; then
        print_status "Accessing Spark Master shell..."
        docker-compose -f docker-compose-distributed.yml exec spark-master bash
    else
        print_status "Accessing $1 shell..."
        docker-compose -f docker-compose-distributed.yml exec "$1" bash
    fi
}

# Function to scale workers
scale_workers() {
    if [ -z "$1" ]; then
        print_error "Please specify number of workers (e.g., $0 scale 5)"
        exit 1
    fi
    
    print_status "Scaling Spark workers to $1 instances..."
    docker-compose -f docker-compose-distributed.yml up -d --scale spark-worker-1=$1 --scale spark-worker-2=$1 --scale spark-worker-3=$1
    print_success "Workers scaled to $1 instances!"
}

# Function to show cluster info
cluster_info() {
    print_status "Distributed Spark Cluster Information:"
    echo ""
    echo "Services:"
    echo "  - Spark Master: http://localhost:8080"
    echo "  - Spark Workers: 3 instances"
    echo "  - Spark Application: http://localhost:4040"
    echo "  - MinIO Console: http://localhost:9001"
    echo ""
    echo "Network: spark-network"
    echo "Volumes:"
    echo "  - minio_data: MinIO persistent storage"
    echo "  - spark_logs: Spark logs"
    echo "  - spark_work: Spark work directory"
    echo ""
    print_status "Use 'docker-compose -f docker-compose-distributed.yml ps' to see running containers"
}

# Function to show help
show_help() {
    echo "Distributed PySpark with MinIO Docker Utilities"
    echo ""
    echo "Usage: $0 [COMMAND] [OPTIONS]"
    echo ""
    echo "Commands:"
    echo "  start              Start distributed Spark cluster"
    echo "  stop               Stop distributed services"
    echo "  restart            Restart distributed services"
    echo "  logs [service]     View logs (all or specific service)"
    echo "  status             Check service status"
    echo "  shell [service]    Access container shell"
    echo "  scale [number]     Scale Spark workers"
    echo "  info               Show cluster information"
    echo "  cleanup            Remove all containers, networks, and volumes"
    echo "  help               Show this help message"
    echo ""
    echo "Services:"
    echo "  spark-master       Spark Master node"
    echo "  spark-worker-1     Spark Worker 1"
    echo "  spark-worker-2     Spark Worker 2"
    echo "  spark-worker-3     Spark Worker 3"
    echo "  spark-app          Spark Application Driver"
    echo "  minio              MinIO Object Storage"
    echo "  minio-client       MinIO Client (setup)"
    echo ""
    echo "Examples:"
    echo "  $0 start           # Start distributed cluster"
    echo "  $0 logs spark-master # View Spark Master logs"
    echo "  $0 shell spark-worker-1 # Access worker shell"
    echo "  $0 scale 5         # Scale workers to 5 instances"
}

# Main script logic
case "${1:-help}" in
    start)
        start_distributed_services
        ;;
    stop)
        stop_distributed_services
        ;;
    restart)
        restart_distributed_services
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
    scale)
        scale_workers "$2"
        ;;
    info)
        cluster_info
        ;;
    cleanup)
        cleanup
        ;;
    help|*)
        show_help
        ;;
esac
