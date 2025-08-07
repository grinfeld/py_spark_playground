# py_spark

A Python Apache Spark 4.0 project for big data processing and analytics with MinIO integration.

## Overview

This project provides a foundation for building Apache Spark applications using PySpark. It includes sample code demonstrating basic Spark operations and best practices for Spark development.

## Features

- ✨ PySpark 4.0 DataFrame operations
- 🔧 Configurable Spark session management
- 📊 Sample data processing examples
- 🧪 Testing framework setup
- 📝 Code quality tools integration
- 🚀 Spark 4.0 performance optimizations
- 🔄 Enhanced Adaptive Query Execution (AQE)
- 📈 Improved window functions and aggregations
- 🧊 Apache Iceberg: ACID transactions and schema evolution
- 🔗 AWS Glue Integration: Managed data catalog
- 🐳 Docker Containerization: Easy deployment and scaling
- 📦 **Configurable Storage**: MinIO (development) or AWS S3 (production)
- 🔄 **Flexible Configuration**: Environment-based storage switching
- 🌪️ **Apache Airflow 3.0.1**: Workflow orchestration with improved UI and performance
- 📋 **Two Production DAGs**: MinIO+Hive and S3+Glue configurations

## Prerequisites

- Python 3.8 or higher
- **Java 17 or 21** (required for Spark 4.0)
- Apache Spark 4.0+ (will be installed via pip)

## Installation

### **Java Version Requirements**

Spark 4.0 requires **Java 17 or 21**:
- **Java 17** (recommended default)
- **Java 21** (also supported)
- ❌ **Java 8/11** are no longer supported in Spark 4.0

**Docker users**: The Dockerfile automatically installs Java 17.

**Local development**: Install Java 17 or 21 on your system:
```bash
# Ubuntu/Debian
sudo apt-get install openjdk-17-jdk

# macOS (using Homebrew)
brew install openjdk@17

# Windows
# Download from https://adoptium.net/
```

**Check your Java version**:
```bash
python check_java_version.py
```

1. **Clone or navigate to the project directory:**
   ```bash
   cd /Users/grinfeld/IdeaProjects/grinfeld/py_spark
   ```

2. **Create a virtual environment (recommended):**
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### **Apache Airflow Orchestration**

This project includes Apache Airflow 3.0.1 with a configurable Spark job DAG:

- **Configurable Spark Job DAG**: Employee analytics with environment-driven configuration

See [docs/AIRFLOW_README.md](docs/AIRFLOW_README.md) for detailed setup and usage instructions.

### **Storage Configuration**

This project supports both **MinIO** (development) and **AWS S3** (production) storage backends. See [docs/STORAGE_CONFIG.md](docs/STORAGE_CONFIG.md) for detailed configuration options.

### **Running with Airflow (Recommended)**

```bash
# Start Airflow with all services
./airflow-utils.sh start

# Access Airflow UI: http://localhost:8080
# Username: admin, Password: admin

# Monitor DAGs in the web interface
```

### **Running with MinIO (Development)**

```bash
# Set environment variables
export STORAGE_TYPE=minio
export STORAGE_ENDPOINT=http://localhost:9000
export STORAGE_ACCESS_KEY_ID=minioadmin
export STORAGE_SECRET_KEY=minioadmin
export STORAGE_BUCKET=spark-data

# Start with Docker Compose
./docker-utils.sh start

# Or run locally
python main.py
```

### **Running with AWS S3 (Production)**

```bash
# Set environment variables
export STORAGE_TYPE=s3
export STORAGE_BUCKET=my-spark-data-bucket
export AWS_REGION=us-east-1
export STORAGE_ACCESS_KEY_ID=your-access-key
export STORAGE_SECRET_KEY=your-secret-key

# Start with Docker Compose
./docker-utils.sh start

# Or run locally
python main.py
```

### **Sample Application Features**

The application demonstrates:
- Creating a Spark session with configurable storage
- Working with DataFrames and enhanced aggregations
- Apache Iceberg operations (ACID transactions, schema evolution)
- AWS Glue catalog integration
- Proper session cleanup

### Importing as a Module

```python
from py_spark.main import create_spark_session, sample_spark_job

# Create Spark session
spark = create_spark_session("my_app")

# Run your Spark operations
# ... your code here ...

# Don't forget to stop the session
spark.stop()
```

## Project Structure

```
py_spark/
├── __init__.py                    # Package initialization
├── main.py                        # Main application and sample code
├── config_manager.py              # Configuration management
├── storage_utils.py               # Storage utility functions
├── check_java_version.py          # Java version compatibility checker
├── dags/                          # Airflow DAGs
│   ├── spark_job_dag.py          # Airflow 3.0 modern decorator DAG
│   ├── spark_job_kubernetes_dag.py # Kubernetes deployment DAG
│   └── utils.py                   # Portable path utilities
├── spark_job.py                   # Shared Spark job logic and SparkSession creation
├── demo/                          # Demo scripts directory
│   ├── __init__.py               # Demo package init
│   ├── demo_*.py                 # Various demo scripts (15 files)
│   └── ...                       # Configuration management examples
├── requirements.txt               # Python dependencies (including Kubernetes)
├── README.md                     # This file
├── docs/                         # Documentation folder
│   ├── AIRFLOW_README.md         # Airflow setup guide
│   ├── KUBERNETES_README.md      # Kubernetes setup guide
│   ├── SPARK4_README.md          # Spark 4.0 features
│   ├── DISTRIBUTED_README.md     # Distributed Spark setup
│   ├── DOCKER_README.md          # Docker setup guide
│   └── ICEBERG_GLUE_README.md    # Iceberg and Glue setup
├── docs/STORAGE_CONFIG.md        # Storage configuration guide
├── env.minio.example             # MinIO environment template
├── env.s3.example                # AWS S3 environment template
├── docker-compose-airflow.yml    # Airflow services
├── Dockerfile.airflow            # Custom Airflow image
├── airflow-utils.sh              # Airflow management script
└── .gitignore                    # Git ignore rules
```

## Development

### Code Quality

The project includes several code quality tools:

```bash
# Format code
black py_spark/

# Lint code
flake8 py_spark/

# Type checking
mypy py_spark/
```

### Testing

Run tests using pytest:

```bash
pytest tests/
```

### Adding New Features

1. Create new modules in the `py_spark` package
2. Import and use them in `main.py` or create separate entry points
3. Add any new dependencies to `requirements.txt`
4. Write tests for new functionality

### Storage-Agnostic Development

The project uses a polymorphic storage abstraction layer that eliminates the need for storage-specific if statements:

```python
from config_manager import config_manager
from storage_utils import write_dataframe_to_storage, read_dataframe_from_storage

# Write data (works with MinIO or AWS S3)
write_dataframe_to_storage(df, "employees", "parquet")

# Read data (works with MinIO or AWS S3)
read_df = read_dataframe_from_storage(spark, "employees", "parquet")
```

### Polymorphic Architecture

The storage system uses inheritance and polymorphism for clean, extensible code:

```python
# Storage backends (simplified)
StorageBackend (Concrete)
└── Unified for MinIO and S3

# Catalog backends (polymorphic)
CatalogBackend (Abstract)
├── HiveCatalog
├── GlueCatalog
└── S3Catalog

# ConfigManager (composition)
ConfigManager
├── storage_backend: StorageBackend
└── catalog_backend: CatalogBackend
```

See `demo/demo_polymorphism.py` for a complete demonstration.

## Configuration

The Spark session can be configured by modifying the `create_spark_session` function in `main.py`. Common configurations include:

```python
spark = SparkSession.builder \
    .appName("my_app") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "1g") \
    .getOrCreate()
```

## Common Spark Operations

### Reading Data

```python
# Read CSV
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)

# Read JSON
df = spark.read.json("path/to/file.json")

# Read Parquet
df = spark.read.parquet("path/to/file.parquet")
```

### Data Processing

```python
from pyspark.sql.functions import col, sum, count, avg

# Filter data
filtered_df = df.filter(col("age") > 25)

# Group and aggregate
grouped_df = df.groupBy("department").agg(
    count("*").alias("count"),
    avg("salary").alias("avg_salary")
)

# Join DataFrames
joined_df = df1.join(df2, df1.id == df2.user_id, "inner")
```

## Troubleshooting

### Common Issues

1. **Java not found**: Ensure Java 8 or 11 is installed and `JAVA_HOME` is set
2. **Memory issues**: Adjust Spark memory configurations in the session builder
3. **Permission errors**: Check file permissions for input/output directories

### Performance Tips

- Use `spark.sql.adaptive.enabled=true` for automatic optimization
- Partition large datasets appropriately
- Cache frequently accessed DataFrames
- Use broadcast joins for small lookup tables

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run code quality checks
6. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Resources

- [Apache Spark 4.0 Documentation](https://spark.apache.org/docs/4.0.0/)
- [PySpark 4.0 API Reference](https://spark.apache.org/docs/4.0.0/api/python/)
- [Spark 4.0 SQL Guide](https://spark.apache.org/docs/4.0.0/sql-programming-guide.html)
- [Spark 4.0 Migration Guide](https://spark.apache.org/docs/4.0.0/migration-guide.html)
- [Spark 4.0 Performance Tuning](https://spark.apache.org/docs/4.0.0/tuning.html)
