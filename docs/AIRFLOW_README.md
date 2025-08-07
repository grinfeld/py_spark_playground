# Apache Airflow with Spark 4.0

This setup provides Apache Airflow 3.0.1 with integrated Spark 4.0, Apache Iceberg, and configurable storage backends (MinIO/S3).

## 🚀 Features

- **Apache Airflow 3.0.1** - Latest stable version with improved UI, better performance, and enhanced DAG parsing
- **Spark 4.0** - With Java 17 support
- **Apache Iceberg** - ACID transactions and schema evolution
- **Configurable Storage** - MinIO (development) or AWS S3 (production)
- **Multiple Catalogs** - Hive Metastore and AWS Glue
- **Two DAGs** - Different storage/catalog combinations
- **Docker Compose** - Easy local development

## 📋 Prerequisites

- Docker and Docker Compose
- At least 8GB RAM available
- Ports 8080, 9000, 9001, 9083 available

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Airflow UI    │    │  Airflow Sched  │    │   PostgreSQL    │
│   (Port 8080)   │    │   (Internal)    │    │   (Metadata)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │   Spark Jobs    │
                    │   (PySpark)     │
                    └─────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│     MinIO       │    │  S3Catalog      │    │   PostgreSQL    │
│  (Port 9000)    │    │  (Hadoop type)  │    │   (Airflow DB)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🎯 DAG Overview

### **spark_job_dag.py** (Airflow 3.0 Modern Style)
- **Configuration**: Environment variables (configurable)
- **Storage**: Configurable (MinIO/S3 via config_manager)
- **Catalog**: Configurable (Hive/Glue/Hadoop via config_manager)
- **Schedule**: Every hour
- **Data**: Employee analytics
- **Features**: Department stats, location analysis, performance metrics, Iceberg tables
- **Style**: Modern Airflow 3.0 decorators (@dag, @task)

### **spark_job_kubernetes_dag.py** (Kubernetes Deployment)
- **Configuration**: Environment variables (configurable)
- **Storage**: Configurable (MinIO/S3 via config_manager)
- **Catalog**: Configurable (Hive/Glue/Hadoop via config_manager)
- **Schedule**: Every hour
- **Data**: Employee analytics
- **Features**: Department stats, location analysis, performance metrics, Iceberg tables
- **Style**: KubernetesPodOperator for distributed execution
- **Resources**: 2Gi memory, 1 CPU request, 4Gi memory, 2 CPU limit

## 🔄 Airflow 3.0 Compatibility

### **Breaking Changes Addressed:**

1. **Removed `default_args`** - Deprecated in Airflow 3.0
   - Parameters now passed directly to DAG constructor
   - More explicit and cleaner configuration

2. **Modern Decorator Support** - Uses Airflow 3.0 best practices:
   - **Modern**: `spark_job_dag.py` - Uses `@dag` and `@task` decorators

3. **Import Compatibility** - All imports updated for Airflow 3.0
   - `from airflow.decorators import task` for modern approach
   - Traditional operators still work unchanged

### **Modern Airflow 3.0 Style:**

```python
@dag(
    dag_id='spark_job',
    start_date=datetime(2024, 1, 1),
    # ... other parameters
)
def spark_job_dag():
    @task
    def run_spark_job_task():
        # ... task logic
    run_spark_job_task()
```

## 🚀 Quick Start

### 1. Start Airflow Services

```bash
# Start all services
./airflow-utils.sh start

# Check status
./airflow-utils.sh status
```

### 2. Kubernetes Setup (Optional)

If you want to run Spark jobs in Kubernetes:

```bash
# Apply Kubernetes deployment
kubectl apply -f kubernetes/spark-job-deployment.yaml

# Check deployment status
kubectl get pods -l app=spark-job

# View logs
kubectl logs -l app=spark-job
```

### 2. Access the Interfaces

- **Airflow UI**: http://localhost:8080
  - Username: `admin`
  - Password: `admin`

- **MinIO Console**: http://localhost:9001
  - Username: `minioadmin`
  - Password: `minioadmin`

### 3. Monitor DAGs

1. Open Airflow UI
2. Navigate to DAGs tab
3. Enable the DAGs you want to run
4. Monitor execution in the Graph view

## 📊 DAG Details

### Configurable Spark Job DAG

**Configuration via Environment Variables:**
```bash
# Storage configuration
STORAGE_BUCKET=spark-data
STORAGE_ENDPOINT=http://minio:9000  # Optional (for MinIO)
STORAGE_ACCESS_KEY_ID=minioadmin     # Optional
STORAGE_SECRET_KEY=minioadmin        # Optional

# Catalog configuration
CATALOG_TYPE=hadoop                  # or 'hive', 'glue'
CATALOG_NAME=spark_catalog
CATALOG_WAREHOUSE_NAME=iceberg-warehouse
CATALOG_IO_IMPL=org.apache.iceberg.aws.s3.S3FileIO
```

**Analytics:**
- Employee data generation (1000 records)
- Department performance analysis
- Location-based salary analysis
- High performer identification
- Iceberg table creation and time travel
- Configurable storage and catalog backends

## 🔧 Management Commands

```bash
# Start services
./airflow-utils.sh start

# Stop services
./airflow-utils.sh stop

# Restart services
./airflow-utils.sh restart

# Check status
./airflow-utils.sh status

# View logs
./airflow-utils.sh logs

# View specific service logs
./airflow-utils.sh logs airflow-webserver

# Clean up everything
./airflow-utils.sh cleanup
```

## 📁 Project Structure

```
py_spark/
├── dags/                          # Airflow DAGs
│   ├── spark_job_dag.py          # Airflow 3.0 modern decorator DAG
│   ├── spark_job_kubernetes_dag.py # Kubernetes deployment DAG
│   └── utils.py                   # Portable path utilities
├── docker-compose-airflow.yml     # Airflow services
├── Dockerfile.airflow             # Custom Airflow image
├── airflow-utils.sh               # Management script
├── spark_job.py                   # Shared Spark job logic and SparkSession creation
├── config_manager.py              # Storage configuration
├── storage_utils.py               # Storage utilities
├── requirements.txt               # Python dependencies (including Kubernetes)
└── docs/AIRFLOW_README.md        # This file
```

## 🔍 Troubleshooting

### Common Issues

1. **Port Conflicts**
   ```bash
   # Check what's using the ports
   lsof -i :8080
   lsof -i :9000
   ```

2. **Memory Issues**
   ```bash
   # Increase Docker memory limit
   # Docker Desktop > Settings > Resources > Memory: 8GB+
   ```

3. **DAG Not Appearing**
   ```bash
   # Check DAG files are in correct location
   ls -la dags/
   
   # Check Airflow logs
   ./airflow-utils.sh logs airflow-scheduler
   ```

4. **Spark Job Failures**
   ```bash
   # Check Spark configuration
   ./airflow-utils.sh logs airflow-webserver
   
   # Verify Java version
   docker exec airflow-webserver java -version
   ```

### Log Locations

- **Airflow Logs**: `/opt/airflow/logs/`
- **DAG Logs**: `/opt/airflow/logs/dag_id/task_id/`
- **Spark Logs**: `/opt/airflow/logs/spark_*.log`

## 🔐 Security Notes

- Default credentials are for development only
- Change passwords in production
- Use Airflow Variables/Secrets for sensitive data
- Consider using AWS IAM roles for S3 access

## 🚀 Production Considerations

1. **Use Airflow Variables** for configuration
2. **Implement proper secrets management**
3. **Set up monitoring and alerting**
4. **Use external databases** (not PostgreSQL in Docker)
5. **Configure proper resource limits**
6. **Set up backup strategies**

## 📈 Monitoring

### Airflow Metrics
- DAG success/failure rates
- Task duration trends
- Resource utilization

### Spark Metrics
- Job execution times
- Memory usage
- I/O performance

### Storage Metrics
- MinIO/S3 usage
- Data volume growth
- Access patterns

## 🔄 CI/CD Integration

```yaml
# Example GitHub Actions workflow
name: Deploy Airflow DAGs
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      - name: Deploy DAGs
        run: |
          # Copy DAGs to Airflow instance
          scp dags/* user@airflow-server:/opt/airflow/dags/
```

## 📚 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Spark 4.0 Documentation](https://spark.apache.org/docs/4.0.0/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [MinIO Documentation](https://docs.min.io/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)

---

**Happy Data Engineering! 🚀**
