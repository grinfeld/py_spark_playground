# Distributed PySpark with MinIO Docker Setup

This setup provides a production-ready distributed Spark cluster with MinIO object storage, suitable for large-scale data processing workloads.

## 🏗️ Architecture

### **Distributed Spark Cluster**
- **1 Spark Master**: Cluster coordination and resource management
- **3 Spark Workers**: Distributed computation nodes
- **1 Spark Application Driver**: Job submission and monitoring
- **1 MinIO Server**: S3-compatible object storage
- **1 MinIO Client**: Automatic bucket setup

### **Network Topology**
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Spark Master  │    │  Spark Worker 1 │    │  Spark Worker 2 │
│   (Port 8080)   │◄──►│                 │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│  Spark Worker 3 │    │ Spark App Driver│    │   MinIO Server  │
│                 │    │   (Port 4040)   │    │  (Port 9001)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 🚀 Quick Start

### **Start Distributed Cluster**
```bash
# Start all services
./docker-distributed-utils.sh start

# Check cluster status
./docker-distributed-utils.sh status

# View logs
./docker-distributed-utils.sh logs spark-master
```

### **Access Points**
- **Spark Master UI**: http://localhost:8080
- **Spark Application UI**: http://localhost:4040
- **MinIO Console**: http://localhost:9001 (minioadmin/minioadmin)

## 📊 Cluster Configuration

### **Resource Allocation**
- **Spark Master**: 1 CPU, 1GB RAM
- **Spark Workers**: 1 CPU, 1GB RAM each (3 total)
- **Spark Driver**: 1 CPU, 1GB RAM
- **MinIO**: 1 CPU, 512MB RAM

### **Ports**
- **8080**: Spark Master Web UI
- **7077**: Spark Master RPC
- **4040**: Spark Application UI
- **9000**: MinIO API
- **9001**: MinIO Console

## 🔧 Services Overview

### **1. Spark Master**
- **Container**: `spark-master`
- **Role**: Cluster coordinator
- **Features**:
  - Resource scheduling
  - Worker management
  - Job coordination
  - Web UI for cluster monitoring

### **2. Spark Workers**
- **Containers**: `spark-worker-1`, `spark-worker-2`, `spark-worker-3`
- **Role**: Distributed computation
- **Features**:
  - Task execution
  - Data caching
  - Resource allocation
  - Fault tolerance

### **3. Spark Application Driver**
- **Container**: `spark-app-distributed`
- **Role**: Job submission and monitoring
- **Features**:
  - Application submission
  - Progress tracking
  - Result collection
  - MinIO integration

### **4. MinIO Object Storage**
- **Container**: `minio-distributed`
- **Role**: S3-compatible storage
- **Features**:
  - Data persistence
  - S3A filesystem support
  - Automatic bucket creation
  - Web-based management

## 📁 Data Flow

### **Input Processing**
1. **Local Data**: Mounted from `./data/` directory
2. **MinIO Storage**: Automatic bucket creation (`spark-data`, `spark-output`)
3. **Distributed Processing**: Tasks distributed across workers
4. **Output Storage**: Results saved to MinIO and local `./output/`

### **Storage Strategy**
```
Input Data → Spark Workers → MinIO Storage
     ↓              ↓              ↓
Local Files → Distributed Processing → S3-Compatible Storage
```

## 🛠️ Management Commands

### **Basic Operations**
```bash
# Start cluster
./docker-distributed-utils.sh start

# Stop cluster
./docker-distributed-utils.sh stop

# Restart cluster
./docker-distributed-utils.sh restart

# Check status
./docker-distributed-utils.sh status
```

### **Monitoring & Debugging**
```bash
# View all logs
./docker-distributed-utils.sh logs

# View specific service logs
./docker-distributed-utils.sh logs spark-master
./docker-distributed-utils.sh logs spark-worker-1

# Access container shell
./docker-distributed-utils.sh shell spark-master
./docker-distributed-utils.sh shell spark-worker-1
```

### **Scaling Operations**
```bash
# Scale workers (not recommended for this setup)
./docker-distributed-utils.sh scale 5

# Show cluster information
./docker-distributed-utils.sh info
```

### **Cleanup**
```bash
# Remove all containers and volumes
./docker-distributed-utils.sh cleanup
```

## 🔍 Monitoring & Troubleshooting

### **Health Checks**

1. **Spark Master Health**
   ```bash
   # Check if master is running
   curl http://localhost:8080
   
   # Check worker registration
   docker-compose -f docker-compose-distributed.yml logs spark-master
   ```

2. **Worker Health**
   ```bash
   # Check worker status
   docker-compose -f docker-compose-distributed.yml logs spark-worker-1
   
   # Check resource usage
   docker stats spark-worker-1
   ```

3. **MinIO Health**
   ```bash
   # Check MinIO status
   curl http://localhost:9000/minio/health/live
   
   # Check bucket creation
   docker-compose -f docker-compose-distributed.yml logs minio-client
   ```

### **Common Issues**

1. **Worker Not Connecting to Master**
   ```bash
   # Check network connectivity
   docker-compose -f docker-compose-distributed.yml exec spark-worker-1 ping spark-master
   
   # Check master logs
   docker-compose -f docker-compose-distributed.yml logs spark-master
   ```

2. **Application Not Starting**
   ```bash
   # Check application logs
   docker-compose -f docker-compose-distributed.yml logs spark-app
   
   # Check MinIO connectivity
   docker-compose -f docker-compose-distributed.yml exec spark-app curl http://minio:9000/minio/health/live
   ```

3. **Memory Issues**
   ```bash
   # Check memory usage
   docker stats
   
   # Increase memory in docker-compose-distributed.yml
   # SPARK_WORKER_MEMORY=2G
   # SPARK_DRIVER_MEMORY=2g
   ```

## ⚡ Performance Optimization

### **Spark Configuration**

Adjust these settings in `docker-compose-distributed.yml`:

```yaml
environment:
  - SPARK_WORKER_MEMORY=2G
  - SPARK_WORKER_CORES=2
  - SPARK_DRIVER_MEMORY=2g
  - SPARK_EXECUTOR_MEMORY=2g
  - SPARK_EXECUTOR_CORES=2
```

### **MinIO Configuration**

For production workloads:
- Use persistent volumes for MinIO data
- Implement proper authentication
- Consider MinIO clustering for high availability

### **Network Optimization**

```yaml
# Add to docker-compose-distributed.yml
networks:
  spark-network:
    driver: bridge
    driver_opts:
      com.docker.network.driver.mtu: 1400
```

## 🔒 Security Considerations

### **Production Security**

1. **Authentication**
   ```bash
   # Change default credentials
   MINIO_ROOT_USER=your_secure_user
   MINIO_ROOT_PASSWORD=your_secure_password
   ```

2. **Network Security**
   ```yaml
   # Restrict network access
   networks:
     spark-network:
       driver: bridge
       internal: true  # No external access
   ```

3. **Resource Limits**
   ```yaml
   deploy:
     resources:
       limits:
         memory: 4G
         cpus: '2.0'
   ```

## 📈 Scaling Strategies

### **Horizontal Scaling**

For larger workloads, consider:
- Adding more worker containers
- Using external Spark cluster (YARN, Kubernetes)
- Implementing load balancing

### **Vertical Scaling**

Increase resources per container:
```yaml
environment:
  - SPARK_WORKER_MEMORY=4G
  - SPARK_WORKER_CORES=4
  - SPARK_DRIVER_MEMORY=4g
  - SPARK_EXECUTOR_MEMORY=4g
```

## 🔄 Deployment Workflows

### **Development Workflow**

1. **Start Cluster**
   ```bash
   ./docker-distributed-utils.sh start
   ```

2. **Develop Application**
   - Modify `main.py`
   - Test with sample data
   - Monitor via Spark UI

3. **Deploy Changes**
   ```bash
   docker-compose -f docker-compose-distributed.yml build spark-app
   docker-compose -f docker-compose-distributed.yml restart spark-app
   ```

### **Production Workflow**

1. **Environment Setup**
   - Configure production credentials
   - Set up monitoring
   - Implement logging

2. **Deployment**
   ```bash
   # Deploy with production config
   docker-compose -f docker-compose-distributed.yml -f docker-compose.prod.yml up -d
   ```

3. **Monitoring**
   - Set up alerting
   - Monitor resource usage
   - Track job performance

## 📋 Comparison: Local vs Distributed

| Feature | Local Setup | Distributed Setup |
|---------|-------------|-------------------|
| **Complexity** | Simple | Moderate |
| **Resources** | Single container | Multiple containers |
| **Scalability** | Limited | High |
| **Fault Tolerance** | None | Built-in |
| **Production Ready** | No | Yes |
| **Resource Usage** | Low | High |
| **Setup Time** | Fast | Moderate |

## 🎯 Use Cases

### **Local Setup** (`docker-compose.yml`)
- Development and testing
- Small datasets
- Learning and experimentation
- Quick prototyping

### **Distributed Setup** (`docker-compose-distributed.yml`)
- Production workloads
- Large datasets
- High-performance computing
- Fault-tolerant processing
- Team development

## 🚨 Important Notes

1. **Resource Requirements**: Distributed setup requires more CPU and memory
2. **Network Dependencies**: All services must be able to communicate
3. **Startup Time**: Distributed cluster takes longer to start up
4. **Data Persistence**: Use volumes for important data
5. **Monitoring**: Always monitor cluster health in production

## 📚 Additional Resources

- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [MinIO Documentation](https://docs.min.io/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [Spark Cluster Mode](https://spark.apache.org/docs/latest/cluster-overview.html)
