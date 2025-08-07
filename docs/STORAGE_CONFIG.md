# Storage Configuration Guide

This project supports both **MinIO** (for local development) and **AWS S3** (for production) as storage backends. The configuration is controlled through environment variables.

## 🏗️ Storage Types

### **MinIO (Local Development)**
- S3-compatible object storage
- Runs locally in Docker containers
- Perfect for development and testing
- No AWS costs

### **AWS S3 (Production)**
- Native AWS S3 storage
- Scalable and reliable
- Production-ready with AWS features
- Pay-per-use pricing

## 📚 Catalog Types

### **Hive Catalog**
- Traditional Hive metastore
- Requires Hive metastore service
- Good for local development
- Uses Thrift protocol

### **Glue Catalog**
- AWS Glue Data Catalog
- Managed service by AWS
- Production-ready metadata management
- Integrates with AWS services

### **S3 Catalog**
- Direct S3-based catalog
- No external metastore required
- Simple and lightweight
- Good for basic Iceberg operations

## ⚙️ Configuration

### **Environment Variables**

| Variable | Required | Description |
|----------|----------|-------------|
| `STORAGE_BUCKET` | **Yes** | Bucket name for data storage |
| `CATALOG_TYPE` | **Yes** | Catalog type: 'hive', 'glue', or 's3' (no defaults) |
| `STORAGE_ENDPOINT` | No | Storage endpoint URL (auto-detects MinIO if set) |
| `STORAGE_ACCESS_KEY_ID` | No | Access key for authentication (optional) |
| `STORAGE_SECRET_KEY` | No | Secret key for authentication (optional) |
| `STORAGE_CREDENTIALS_PROVIDER` | No | Credentials provider class (optional) |
| `STORAGE_PATH_STYLE_ACCESS` | No | Path style access for S3A (true/false, optional) |
| `AWS_REGION` | No | AWS region for S3/Glue (optional) |
| `CATALOG_NAME` | No | Catalog name (optional) |
| `CATALOG_WAREHOUSE_NAME` | **Yes** | Warehouse directory name |
| `CATALOG_IO_IMPL` | `None` | `None` | IO implementation override (optional) |

## 🚀 Quick Start

### **MinIO Setup (Development)**

```bash
# Set environment variables
export STORAGE_TYPE=minio
export STORAGE_ENDPOINT=http://localhost:9000
export STORAGE_ACCESS_KEY_ID=minioadmin
export STORAGE_SECRET_KEY=minioadmin
export STORAGE_CREDENTIALS_PROVIDER=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
export STORAGE_PATH_STYLE_ACCESS=true
export STORAGE_BUCKET=spark-data

# Run with Docker Compose
./docker-utils.sh start
```

### **AWS S3 Setup (Production)**

```bash
# Set environment variables
export STORAGE_TYPE=s3
export STORAGE_BUCKET=my-spark-data-bucket
export AWS_REGION=us-east-1
export STORAGE_ACCESS_KEY_ID=your-access-key
export STORAGE_SECRET_KEY=your-secret-key
# export STORAGE_CREDENTIALS_PROVIDER=  # Leave empty to use Spark defaults
# export STORAGE_PATH_STYLE_ACCESS=  # Leave empty to use Spark defaults

# Run with Docker Compose
./docker-utils.sh start
```

## 🔧 Docker Configuration

### **MinIO Configuration**

```yaml
# docker-compose.yml
services:
  pyspark-app:
    environment:
      - STORAGE_TYPE=minio
      - STORAGE_ENDPOINT=http://minio:9000
      - STORAGE_ACCESS_KEY_ID=minioadmin
      - STORAGE_SECRET_KEY=minioadmin
      - STORAGE_CREDENTIALS_PROVIDER=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
      - STORAGE_PATH_STYLE_ACCESS=true
      - STORAGE_BUCKET=spark-data
      - AWS_REGION=us-east-1
      - CATALOG_TYPE=glue  # 'hive', 'glue', or 's3'
      - CATALOG_NAME=spark_catalog
```

### **AWS S3 Configuration**

```yaml
# docker-compose.yml
services:
  pyspark-app:
    environment:
      - STORAGE_TYPE=s3
      - STORAGE_BUCKET=my-spark-data-bucket
      - AWS_REGION=us-east-1
      - CATALOG_TYPE=glue  # 'hive', 'glue', or 's3'
      - CATALOG_NAME=spark_catalog
```

## 🔐 Authentication

### **MinIO Authentication**
- Uses access key and secret key
- Configured in MinIO server
- Default credentials: `minioadmin`/`minioadmin`

### **AWS S3 Authentication**
- **Option 1**: Access Key + Secret Key
  ```bash
  export STORAGE_ACCESS_KEY_ID=your-access-key
  export STORAGE_SECRET_KEY=your-secret-key
  ```

- **Option 2**: IAM Roles (EC2/ECS)
  - No environment variables needed
  - Uses instance metadata service

- **Option 3**: AWS CLI Profile
  ```bash
  export AWS_PROFILE=my-profile
  ```

## 📁 Data Paths

### **MinIO Paths**
```
s3a://spark-data/
├── warehouse/           # Spark warehouse
├── iceberg-warehouse/   # Iceberg tables
├── glue-warehouse/      # Glue catalog
├── employees.parquet    # Sample data
├── employees.csv        # Sample data
├── role_stats.parquet   # Aggregated data
└── ranked_employees.parquet
```

### **AWS S3 Paths**
```
s3a://my-spark-data-bucket/
├── warehouse/           # Spark warehouse
├── iceberg-warehouse/   # Iceberg tables
├── glue-warehouse/      # Glue catalog
├── employees.parquet    # Sample data
├── employees.csv        # Sample data
├── role_stats.parquet   # Aggregated data
└── ranked_employees.parquet
```

## 🔄 Switching Between Storage Types

### **From MinIO to AWS S3**

1. **Update Environment Variables**
   ```bash
   export STORAGE_TYPE=s3
   export STORAGE_BUCKET=your-s3-bucket
export STORAGE_ACCESS_KEY_ID=your-access-key
export STORAGE_SECRET_KEY=your-secret-key
   ```

2. **Create S3 Bucket**
   ```bash
   aws s3 mb s3://your-s3-bucket
   ```

3. **Restart Application**
   ```bash
   ./docker-utils.sh restart
   ```

### **From AWS S3 to MinIO**

1. **Update Environment Variables**
   ```bash
   export STORAGE_TYPE=minio
   export STORAGE_ENDPOINT=http://localhost:9000
export STORAGE_ACCESS_KEY_ID=minioadmin
export STORAGE_SECRET_KEY=minioadmin
export STORAGE_BUCKET=spark-data
   ```

2. **Start MinIO Services**
   ```bash
   ./docker-utils.sh start
   ```

## 🧪 Testing Storage Configuration

### **MinIO Health Check**
```bash
curl http://localhost:9000/minio/health/live
```

### **AWS S3 Health Check**
```bash
aws s3 ls s3://your-bucket-name
```

### **Application Health Check**
```bash
# Check storage accessibility
python -c "
import os
from config_manager import config_manager
storage_info = storage_manager.get_storage_info()
print(f'Storage type: {storage_info["type"]}')
"
```

## 🔍 Troubleshooting

### **Common MinIO Issues**

1. **MinIO not accessible**
   ```bash
   # Check if MinIO is running
   docker ps | grep minio
   
   # Check MinIO logs
   docker logs minio
   ```

2. **Authentication failed**
   ```bash
   # Verify credentials
   mc alias set myminio http://localhost:9000 minioadmin minioadmin
   mc ls myminio
   ```

### **Common AWS S3 Issues**

1. **Credentials not found**
   ```bash
   # Check AWS credentials
   aws sts get-caller-identity
   
   # Set credentials
   export STORAGE_ACCESS_KEY_ID=your-key
export STORAGE_SECRET_KEY=your-secret
   ```

2. **Bucket not found**
   ```bash
   # List buckets
   aws s3 ls
   
   # Create bucket
   aws s3 mb s3://your-bucket-name
   ```

3. **Permission denied**
   ```bash
   # Check IAM permissions
   aws iam get-user
   
   # Required permissions:
   # - s3:ListBucket
   # - s3:GetObject
   # - s3:PutObject
   # - s3:DeleteObject
   ```

## 📊 Performance Considerations

### **MinIO Performance**
- Local network latency
- Limited by local storage
- Good for development/testing

### **AWS S3 Performance**
- Network latency to AWS
- Unlimited scalability
- Production-ready performance

## 💰 Cost Considerations

### **MinIO Costs**
- Free for local development
- Only Docker resource usage

### **AWS S3 Costs**
- Storage: $0.023 per GB/month
- Requests: $0.0004 per 1,000 requests
- Data transfer: $0.09 per GB (outbound)

## 🔒 Security Best Practices

### **MinIO Security**
- Change default credentials
- Use HTTPS in production
- Implement access policies

### **AWS S3 Security**
- Use IAM roles when possible
- Enable bucket encryption
- Implement least privilege access
- Use VPC endpoints for private access
