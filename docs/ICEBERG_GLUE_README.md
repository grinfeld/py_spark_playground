# Apache Iceberg with AWS Glue Integration

This project now supports Apache Iceberg with AWS Glue integration, providing ACID transactions, schema evolution, and advanced data lake capabilities.

## 🏗️ Architecture Overview

### **Components**
- **Apache Iceberg**: Table format for large datasets with ACID transactions
- **AWS Glue**: Managed data catalog and ETL service
- **Hive Metastore**: Metadata management for Iceberg tables
- **MinIO**: S3-compatible object storage
- **Spark 4.0**: Processing engine with Iceberg integration

### **Data Flow**
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Spark     │───▶│   Iceberg   │───▶│   MinIO     │
│  (4.0)      │    │   Tables    │    │  Storage    │
└─────────────┘    └─────────────┘    └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Glue      │    │   Hive      │    │   S3A       │
│  Catalog    │    │ Metastore   │    │  Filesystem │
└─────────────┘    └─────────────┘    └─────────────┘
```

## 🚀 Key Features

### **Apache Iceberg**
- ✅ **ACID Transactions**: Atomic, consistent, isolated, durable operations
- ✅ **Schema Evolution**: Add, drop, rename columns safely
- ✅ **Time Travel**: Query data at specific points in time
- ✅ **Partition Evolution**: Change partition schemes without rewriting data
- ✅ **Hidden Partitioning**: Automatic partition management
- ✅ **Optimized File Layout**: Efficient storage and query performance

### **AWS Glue Integration**
- ✅ **Managed Catalog**: Centralized metadata management
- ✅ **Schema Discovery**: Automatic schema inference
- ✅ **Data Lineage**: Track data transformations
- ✅ **Security**: IAM-based access control
- ✅ **Integration**: Seamless AWS service integration

## 📊 Iceberg Operations

### **Table Creation**
```python
# Create Iceberg table
df.writeTo("spark_catalog.default.employees_iceberg").using("iceberg").createOrReplace()

# Insert data
df.writeTo("spark_catalog.default.employees_iceberg").append()

# Overwrite data
df.writeTo("spark_catalog.default.employees_iceberg").overwrite()
```

### **Schema Evolution**
```python
# Add new column
df_with_salary = df.withColumn("salary", col("age") * 1000)
df_with_salary.writeTo("spark_catalog.default.employees_iceberg").overwrite()

# Read evolved schema
evolved_df = spark.table("spark_catalog.default.employees_iceberg")
```

### **Time Travel**
```python
# Query data at specific timestamp
historical_df = spark.read.option("as-of-timestamp", "2024-01-01 10:00:00") \
    .table("spark_catalog.default.employees_iceberg")

# Query data at specific snapshot
snapshot_df = spark.read.option("snapshot-id", "123456789") \
    .table("spark_catalog.default.employees_iceberg")
```

### **Partition Management**
```python
# Partitioned table
df.writeTo("spark_catalog.default.employees_partitioned") \
    .using("iceberg") \
    .partitionedBy("role", "year(hire_date)") \
    .createOrReplace()
```

## 🔧 Configuration

### **Iceberg Catalog Configuration**
```yaml
# Spark Catalog (Hive-based)
spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.spark_catalog.type=hive
spark.sql.catalog.spark_catalog.uri=thrift://hive-metastore:9083
spark.sql.catalog.spark_catalog.warehouse=s3a://spark-data/iceberg-warehouse

# Glue Catalog
spark.sql.catalog.glue_catalog=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.warehouse=s3a://spark-data/glue-warehouse
spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
```

### **AWS Glue Configuration**
```yaml
# AWS Credentials
spark.hadoop.aws.region=us-east-1
spark.hadoop.aws.access.key=${AWS_ACCESS_KEY_ID}
spark.hadoop.aws.secret.key=${AWS_SECRET_ACCESS_KEY}

# Glue Catalog Settings
spark.sql.catalog.glue_catalog.s3.endpoint=http://minio:9000
spark.sql.catalog.glue_catalog.s3.access-key=${AWS_ACCESS_KEY_ID}
spark.sql.catalog.glue_catalog.s3.secret-key=${AWS_SECRET_ACCESS_KEY}
```

## 📁 File Structure

### **Iceberg Table Layout**
```
s3a://spark-data/iceberg-warehouse/
├── default/
│   └── employees_iceberg/
│       ├── data/
│       │   ├── 00000-0-1234567890-1234567890.parquet
│       │   └── 00001-0-1234567890-1234567890.parquet
│       ├── metadata/
│       │   ├── 00000-1234567890.metadata.json
│       │   ├── 00001-1234567890.metadata.json
│       │   └── version-hint.text
│       └── _delta_log/
│           └── 00000000000000000000.json
```

### **Glue Catalog Structure**
```
glue_catalog/
├── default/
│   ├── employees_glue/
│   │   ├── data/
│   │   └── metadata/
│   └── other_tables/
└── other_databases/
```

## 🛠️ Operations

### **Local Development**
```bash
# Start with Iceberg and Glue support
./docker-utils.sh start

# Check Iceberg tables
docker-compose exec pyspark-app spark-sql -e "SHOW TABLES IN spark_catalog.default"

# Check Glue catalog
docker-compose exec pyspark-app spark-sql -e "SHOW TABLES IN glue_catalog.default"
```

### **Distributed Development**
```bash
# Start distributed cluster with Iceberg
./docker-distributed-utils.sh start

# Access Hive Metastore
docker-compose -f docker-compose-distributed.yml exec hive-metastore beeline -u jdbc:hive2://localhost:10000
```

### **Iceberg Table Operations**
```sql
-- Create table
CREATE TABLE spark_catalog.default.employees_iceberg (
    name STRING,
    age INT,
    role STRING,
    hire_date DATE
) USING iceberg;

-- Insert data
INSERT INTO spark_catalog.default.employees_iceberg VALUES ('Alice', 25, 'Engineer', '2024-01-01');

-- Query with time travel
SELECT * FROM spark_catalog.default.employees_iceberg TIMESTAMP AS OF '2024-01-01 10:00:00';

-- Schema evolution
ALTER TABLE spark_catalog.default.employees_iceberg ADD COLUMNS (salary INT);
```

## 🔍 Monitoring and Debugging

### **Iceberg Table Information**
```python
# Get table metadata
table = spark.table("spark_catalog.default.employees_iceberg")
table_details = spark.sql("DESCRIBE EXTENDED spark_catalog.default.employees_iceberg")

# Get snapshots
snapshots = spark.sql("SELECT * FROM spark_catalog.default.employees_iceberg.snapshots")

# Get history
history = spark.sql("SELECT * FROM spark_catalog.default.employees_iceberg.history")
```

### **Glue Catalog Information**
```python
# List databases
databases = spark.sql("SHOW DATABASES IN glue_catalog")

# List tables
tables = spark.sql("SHOW TABLES IN glue_catalog.default")

# Table details
table_info = spark.sql("DESCRIBE EXTENDED glue_catalog.default.employees_glue")
```

## 📈 Performance Optimization

### **Iceberg Optimizations**
```yaml
# File size optimization
spark.sql.adaptive.advisoryPartitionSizeInBytes=128m
spark.sql.adaptive.coalescePartitions.minPartitionNum=1
spark.sql.adaptive.coalescePartitions.initialPartitionNum=200

# Compression
spark.sql.parquet.compression.codec=snappy
spark.sql.parquet.enable.dictionary=true
```

### **Glue Optimizations**
```yaml
# Connection pooling
spark.hadoop.aws.glue.connection.pool.size=10
spark.hadoop.aws.glue.connection.timeout=30000

# Metadata caching
spark.sql.catalog.glue_catalog.cache-enabled=true
spark.sql.catalog.glue_catalog.cache-ttl=3600
```

## 🔒 Security

### **AWS IAM Permissions**
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:GetTable",
                "glue:GetTables",
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:DeleteTable",
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": "*"
        }
    ]
}
```

### **MinIO Security**
```yaml
# Access control
MINIO_ROOT_USER=your_secure_user
MINIO_ROOT_PASSWORD=your_secure_password

# Bucket policies
mc policy set private myminio/iceberg-warehouse
mc policy set private myminio/glue-warehouse
```

## 🚨 Troubleshooting

### **Common Issues**

1. **Iceberg Table Not Found**
   ```bash
   # Check catalog configuration
   docker-compose exec pyspark-app spark-sql -e "SHOW CATALOGS"
   
   # Check table existence
   docker-compose exec pyspark-app spark-sql -e "SHOW TABLES IN spark_catalog.default"
   ```

2. **Glue Catalog Connection Issues**
   ```bash
   # Check AWS credentials
   docker-compose exec pyspark-app env | grep AWS
   
   # Test Glue connectivity
   docker-compose exec pyspark-app aws glue get-databases --region us-east-1
   ```

3. **Hive Metastore Issues**
   ```bash
   # Check metastore status
   docker-compose logs hive-metastore
   
   # Test connection
   docker-compose exec hive-metastore beeline -u jdbc:hive2://localhost:10000
   ```

### **Performance Issues**

1. **Slow Queries**
   ```yaml
   # Increase memory
   SPARK_DRIVER_MEMORY=2g
   SPARK_EXECUTOR_MEMORY=2g
   
   # Optimize partitions
   spark.sql.adaptive.advisoryPartitionSizeInBytes=256m
   ```

2. **Large Metadata**
   ```yaml
   # Enable metadata caching
   spark.sql.catalog.glue_catalog.cache-enabled=true
   spark.sql.catalog.glue_catalog.cache-ttl=7200
   ```

## 📚 Best Practices

### **Table Design**
1. **Choose Appropriate Partitioning**: Partition by frequently queried columns
2. **Optimize File Sizes**: Target 128MB-1GB per file
3. **Use Appropriate Data Types**: Choose efficient data types
4. **Plan Schema Evolution**: Design for future changes

### **Performance**
1. **Use Time Travel Sparingly**: Historical queries can be expensive
2. **Optimize Partition Schemes**: Avoid over-partitioning
3. **Monitor File Sizes**: Keep files within optimal range
4. **Use Appropriate Compression**: Balance size vs. query performance

### **Operations**
1. **Regular Maintenance**: Compact small files periodically
2. **Monitor Metadata**: Keep metadata size manageable
3. **Backup Strategies**: Implement data backup procedures
4. **Security**: Use least privilege access

## 🔄 Migration from Other Formats

### **From Parquet**
```python
# Read existing Parquet data
parquet_df = spark.read.parquet("s3a://bucket/existing-data.parquet")

# Write to Iceberg
parquet_df.writeTo("spark_catalog.default.new_iceberg_table").using("iceberg").createOrReplace()
```

### **From Delta Lake**
```python
# Read Delta table
delta_df = spark.read.format("delta").load("s3a://bucket/delta-table")

# Write to Iceberg
delta_df.writeTo("spark_catalog.default.migrated_table").using("iceberg").createOrReplace()
```

## 📈 Monitoring Metrics

### **Key Metrics to Monitor**
- **Table Size**: Total size of Iceberg tables
- **File Count**: Number of files per table
- **Query Performance**: Query execution times
- **Metadata Size**: Size of table metadata
- **Snapshot Count**: Number of table snapshots

### **Glue Metrics**
- **Catalog Operations**: Create/update/delete operations
- **Query Performance**: Time to retrieve metadata
- **Error Rates**: Failed catalog operations
- **Cache Hit Rate**: Metadata cache efficiency

## 🎯 Use Cases

### **Data Lake**
- **Raw Data Storage**: Store raw data with schema evolution
- **Data Versioning**: Track changes over time
- **Audit Trail**: Maintain data lineage and history

### **Analytics**
- **Interactive Queries**: Fast queries on large datasets
- **Time Travel**: Analyze data at specific points in time
- **Schema Evolution**: Adapt to changing data structures

### **ETL Pipelines**
- **Incremental Processing**: Process only new/changed data
- **ACID Transactions**: Ensure data consistency
- **Error Recovery**: Rollback failed operations

## 📚 Additional Resources

- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [AWS Glue Documentation](https://docs.aws.amazon.com/glue/)
- [Iceberg Spark Integration](https://iceberg.apache.org/spark/)
- [Glue Catalog Integration](https://iceberg.apache.org/aws/)
- [Iceberg Best Practices](https://iceberg.apache.org/best-practices/)
