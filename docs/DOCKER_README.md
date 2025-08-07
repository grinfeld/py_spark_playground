# PySpark with MinIO Docker Setup

This project provides a complete Docker Compose setup for running PySpark applications with MinIO object storage.

## Architecture

The setup includes:
- **MinIO**: S3-compatible object storage
- **PySpark Application**: Your Spark application container
- **Jupyter Notebook** (optional): For interactive development
- **MinIO Client**: For initial setup and bucket creation

## Services

### 1. MinIO Object Storage
- **Port**: 9000 (API), 9001 (Console)
- **Credentials**: minioadmin/minioadmin
- **Buckets**: spark-data, spark-output (created automatically)

### 2. PySpark Application
- **Port**: 4040 (Spark UI)
- **Features**: 
  - Reads/writes data to MinIO
  - Processes sample data
  - Demonstrates S3A filesystem integration

### 3. Jupyter Notebook (Optional)
- **Port**: 8888
- **Features**: Interactive development environment

## Quick Start

### 1. Build and Start Services

```bash
# Start all services
docker-compose up -d

# Start with Jupyter (optional)
docker-compose --profile jupyter up -d
```

### 2. Access Services

- **MinIO Console**: http://localhost:9001
  - Username: minioadmin
  - Password: minioadmin

- **Spark UI**: http://localhost:4040

- **Jupyter Notebook**: http://localhost:8888 (if enabled)

### 3. View Logs

```bash
# View PySpark application logs
docker-compose logs -f pyspark-app

# View MinIO logs
docker-compose logs -f minio

# View all logs
docker-compose logs -f
```

### 4. Stop Services

```bash
docker-compose down
```

## Data Flow

1. **Input Data**: Mounted from `./data/` directory
2. **Processing**: PySpark processes data with MinIO integration
3. **Output**: Written to MinIO buckets and local `./output/` directory

## MinIO Buckets

The setup automatically creates:
- `spark-data`: For input data and processed results
- `spark-output`: For final output files

## Environment Variables

### PySpark Application
- `MINIO_ENDPOINT`: MinIO service endpoint
- `MINIO_ACCESS_KEY`: MinIO access key
- `MINIO_SECRET_KEY`: MinIO secret key
- `MINIO_BUCKET`: Default bucket name
- `SPARK_MASTER`: Spark master URL
- `SPARK_DRIVER_MEMORY`: Driver memory allocation
- `SPARK_EXECUTOR_MEMORY`: Executor memory allocation

## File Structure

```
py_spark/
├── data/                    # Input data (mounted as volume)
│   └── sample_data.csv
├── output/                  # Output directory (mounted as volume)
├── notebooks/               # Jupyter notebooks (mounted as volume)
├── main.py                  # Main PySpark application
├── Dockerfile               # Application container definition
├── docker-compose.yml       # Service orchestration
├── requirements.txt         # Python dependencies
└── .dockerignore           # Docker build exclusions
```

## Development Workflow

### 1. Local Development

```bash
# Start services
docker-compose up -d

# Make code changes
# The application will automatically restart due to restart policy
```

### 2. Interactive Development

```bash
# Start with Jupyter
docker-compose --profile jupyter up -d

# Access Jupyter at http://localhost:8888
```

### 3. Data Processing

The application demonstrates:
- Reading data from local files
- Processing with PySpark
- Writing results to MinIO
- Reading back from MinIO for verification

## Troubleshooting

### Common Issues

1. **MinIO Connection Issues**
   ```bash
   # Check MinIO health
   curl http://localhost:9000/minio/health/live
   
   # Check MinIO logs
   docker-compose logs minio
   ```

2. **Spark Application Issues**
   ```bash
   # Check application logs
   docker-compose logs pyspark-app
   
   # Restart application
   docker-compose restart pyspark-app
   ```

3. **Memory Issues**
   - Adjust `SPARK_DRIVER_MEMORY` and `SPARK_EXECUTOR_MEMORY` in docker-compose.yml
   - Increase Docker memory allocation

### Debugging

```bash
# Access container shell
docker-compose exec pyspark-app bash

# Check MinIO connectivity
docker-compose exec pyspark-app mc alias set myminio http://minio:9000 minioadmin minioadmin
docker-compose exec pyspark-app mc ls myminio
```

## Performance Tuning

### Spark Configuration

Adjust these settings in `docker-compose.yml`:

```yaml
environment:
  - SPARK_DRIVER_MEMORY=2g
  - SPARK_EXECUTOR_MEMORY=2g
  - SPARK_MASTER=local[4]  # Use more cores
```

### MinIO Configuration

For production, consider:
- Using persistent volumes for MinIO data
- Implementing proper authentication
- Setting up MinIO clustering

## Security Notes

- Default credentials are for development only
- Change MinIO credentials for production
- Consider using secrets management
- Implement proper network security

## Monitoring

### Health Checks

- MinIO: Automatic health check configured
- PySpark: Manual monitoring via logs
- Jupyter: No health check (optional service)

### Logs

```bash
# Follow all logs
docker-compose logs -f

# Follow specific service
docker-compose logs -f pyspark-app
```

## Scaling

### Horizontal Scaling

```bash
# Scale PySpark application
docker-compose up -d --scale pyspark-app=3
```

### Vertical Scaling

Adjust memory and CPU limits in `docker-compose.yml`:

```yaml
deploy:
  resources:
    limits:
      memory: 4G
      cpus: '2.0'
```

## Production Considerations

1. **Security**
   - Change default credentials
   - Use secrets management
   - Implement proper authentication

2. **Persistence**
   - Use named volumes for data
   - Implement backup strategies

3. **Monitoring**
   - Add proper logging
   - Implement metrics collection
   - Set up alerting

4. **Performance**
   - Optimize Spark configurations
   - Use appropriate MinIO deployment
   - Consider clustering

## Cleanup

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (WARNING: deletes all data)
docker-compose down -v

# Remove images
docker-compose down --rmi all
```
