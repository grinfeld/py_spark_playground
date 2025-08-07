#!/bin/bash

# Airflow Docker Compose Management Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
COMPOSE_FILE="docker-compose-airflow.yml"
AIRFLOW_UI_URL="http://localhost:8080"
MINIO_UI_URL="http://localhost:9001"

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================${NC}"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
}

# Function to check if ports are available
check_ports() {
    local ports=("8080" "9000" "9001" "9083")
    for port in "${ports[@]}"; do
        if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1; then
            print_warning "Port $port is already in use. This might cause conflicts."
        fi
    done
}

# Function to start Airflow
start_airflow() {
    print_header "Starting Apache Airflow with Spark 4.0"
    
    check_docker
    check_ports
    
    print_status "Building and starting Airflow services..."
    docker-compose -f $COMPOSE_FILE up -d --build
    
    print_status "Waiting for services to be ready..."
    sleep 30
    
    print_status "Checking service status..."
    docker-compose -f $COMPOSE_FILE ps
    
    print_header "Access Information"
    echo -e "${GREEN}Airflow UI:${NC} $AIRFLOW_UI_URL"
    echo -e "${GREEN}  Username:${NC} admin"
    echo -e "${GREEN}  Password:${NC} admin"
    echo -e "${GREEN}MinIO Console:${NC} $MINIO_UI_URL"
    echo -e "${GREEN}  Username:${NC} minioadmin"
    echo -e "${GREEN}  Password:${NC} minioadmin"
    
    print_status "Airflow is starting up. Please wait a few minutes for all services to be ready."
}

# Function to stop Airflow
stop_airflow() {
    print_header "Stopping Apache Airflow"
    
    print_status "Stopping services..."
    docker-compose -f $COMPOSE_FILE down
    
    print_status "Airflow services stopped."
}

# Function to restart Airflow
restart_airflow() {
    print_header "Restarting Apache Airflow"
    
    stop_airflow
    sleep 5
    start_airflow
}

# Function to show logs
show_logs() {
    local service=${1:-"airflow-webserver"}
    
    print_header "Showing logs for $service"
    docker-compose -f $COMPOSE_FILE logs -f $service
}

# Function to show all logs
show_all_logs() {
    print_header "Showing all logs"
    docker-compose -f $COMPOSE_FILE logs -f
}

# Function to check status
check_status() {
    print_header "Airflow Service Status"
    docker-compose -f $COMPOSE_FILE ps
    
    echo ""
    print_header "Service Health Checks"
    
    # Check Airflow webserver
    if curl -s "$AIRFLOW_UI_URL/health" > /dev/null 2>&1; then
        print_status "✅ Airflow webserver is healthy"
    else
        print_error "❌ Airflow webserver is not responding"
    fi
    
    # Check MinIO
    if curl -s "$MINIO_UI_URL" > /dev/null 2>&1; then
        print_status "✅ MinIO console is accessible"
    else
        print_error "❌ MinIO console is not responding"
    fi
}

# Function to clean up
cleanup() {
    print_header "Cleaning up Airflow"
    
    print_warning "This will remove all containers, volumes, and data. Are you sure? (y/N)"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        print_status "Removing containers and volumes..."
        docker-compose -f $COMPOSE_FILE down -v --remove-orphans
        
        print_status "Removing Airflow images..."
        docker rmi $(docker images -q airflow-spark) 2>/dev/null || true
        
        print_status "Cleanup completed."
    else
        print_status "Cleanup cancelled."
    fi
}

# Function to show help
show_help() {
    print_header "Airflow Management Script"
    echo "Usage: $0 [COMMAND]"
    echo ""
    echo "Commands:"
    echo "  start       Start Airflow services"
    echo "  stop        Stop Airflow services"
    echo "  restart     Restart Airflow services"
    echo "  status      Show service status and health"
    echo "  logs        Show logs for all services"
    echo "  logs [SERVICE] Show logs for specific service"
    echo "  cleanup     Remove all containers and volumes"
    echo "  help        Show this help message"
    echo ""
    echo "Services:"
    echo "  airflow-webserver"
    echo "  airflow-scheduler"
    echo "  postgres"
    echo "  minio"
    
    echo ""
    echo "Examples:"
    echo "  $0 start"
    echo "  $0 logs airflow-webserver"
    echo "  $0 status"
}

# Main script logic
case "${1:-help}" in
    start)
        start_airflow
        ;;
    stop)
        stop_airflow
        ;;
    restart)
        restart_airflow
        ;;
    status)
        check_status
        ;;
    logs)
        if [ -n "$2" ]; then
            show_logs "$2"
        else
            show_all_logs
        fi
        ;;
    cleanup)
        cleanup
        ;;
    help|--help|-h)
        show_help
        ;;
    *)
        print_error "Unknown command: $1"
        echo ""
        show_help
        exit 1
        ;;
esac
