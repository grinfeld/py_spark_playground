# Kubernetes Setup for Spark Jobs

This guide explains how to run Spark jobs in Kubernetes using Airflow.

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Airflow UI    │    │  Kubernetes     │    │   Spark Job     │
│   (Port 8080)   │    │   Cluster       │    │   Pod           │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                       │                       │
          └───────────────────────┼───────────────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │   KubernetesPodOperator   │
                    │   (Airflow Task)         │
                    └───────────────────────────┘
                                  │
                    ┌─────────────▼─────────────┐
                    │   Spark Job Container     │
                    │   (Shared spark_job.py)   │
                    └───────────────────────────┘
```

## 📋 Prerequisites

- Kubernetes cluster (local or cloud)
- kubectl configured
- Docker registry access
- Airflow with Kubernetes provider (included in requirements.txt)

## 🚀 Quick Start

### 1. Build and Push Docker Image

```bash
# Build the Spark image
docker build -t your-registry/py-spark:latest .

# Push to registry
docker push your-registry/py-spark:latest
```

### 2. Deploy to Kubernetes

```bash
# Apply the deployment
kubectl apply -f kubernetes/spark-job-deployment.yaml

# Check status
kubectl get pods -l app=spark-job
```

### 3. Configure Airflow Variables

Set these variables in Airflow UI or via CLI:

```bash
# Storage configuration
STORAGE_BUCKET=spark-data
CATALOG_TYPE=hadoop
CATALOG_NAME=spark_catalog
CATALOG_WAREHOUSE_NAME=iceberg-warehouse

# Optional MinIO configuration
STORAGE_ENDPOINT=http://minio-service:9000
STORAGE_ACCESS_KEY_ID=minioadmin
STORAGE_SECRET_KEY=minioadmin
STORAGE_PATH_STYLE_ACCESS=true
CATALOG_IO_IMPL=org.apache.iceberg.aws.s3.S3FileIO
```

### 4. Enable Kubernetes DAG

1. Open Airflow UI
2. Navigate to DAGs
3. Enable `spark_job_kubernetes` DAG
4. Monitor execution

## 📊 DAG Comparison

| Feature | Local DAG | Kubernetes DAG |
|---------|-----------|----------------|
| **Execution** | Local Airflow worker | Kubernetes pod |
| **Resources** | Shared with Airflow | Isolated (2Gi/1CPU) |
| **Scalability** | Limited | Kubernetes scaling |
| **Isolation** | Shared environment | Container isolation |
| **Monitoring** | Airflow logs | Kubernetes logs |
| **Shared Code** | ✅ spark_job.py | ✅ spark_job.py |

## 🔧 Configuration

### Environment Variables

The Kubernetes DAG uses Airflow variables for configuration:

```python
env_vars={
    'STORAGE_BUCKET': '{{ var.value.STORAGE_BUCKET }}',
    'CATALOG_TYPE': '{{ var.value.CATALOG_TYPE }}',
    'CATALOG_NAME': '{{ var.value.CATALOG_NAME }}',
    'CATALOG_WAREHOUSE_NAME': '{{ var.value.CATALOG_WAREHOUSE_NAME }}',
    # ... other variables
}
```

### Resource Limits

```yaml
resources:
  requests:
    memory: "2Gi"
    cpu: "1000m"
  limits:
    memory: "4Gi"
    cpu: "2000m"
```

### Shared Code

Both DAGs use the same `spark_job.py` module:

```python
# In Kubernetes pod
import sys
sys.path.insert(0, '/opt/airflow/dags')
from spark_job import run_spark_job
result = run_spark_job()
```

## 🐛 Troubleshooting

### Common Issues

1. **Image Pull Errors**
   ```bash
   # Check if image exists
   docker pull your-registry/py-spark:latest
   
   # Update image tag in deployment
   kubectl set image deployment/spark-job-deployment spark-job=your-registry/py-spark:latest
   ```

2. **Resource Limits**
   ```bash
   # Check pod status
   kubectl describe pod -l app=spark-job
   
   # Check resource usage
   kubectl top pods -l app=spark-job
   ```

3. **Environment Variables**
   ```bash
   # Check Airflow variables
   airflow variables list
   
   # Set variable
   airflow variables set STORAGE_BUCKET spark-data
   ```

### Logs

```bash
# Kubernetes logs
kubectl logs -l app=spark-job

# Airflow task logs
# Check in Airflow UI -> DAG -> Task -> Logs
```

## 🔄 Migration from Local to Kubernetes

### Step 1: Update DAG Reference

```python
# Before: Local execution
@task
def run_spark_job_local():
    return run_spark_job()

# After: Kubernetes execution
spark_kubernetes_task = KubernetesPodOperator(
    task_id='spark_job_kubernetes',
    # ... configuration
)
```

### Step 2: Configure Resources

```yaml
# Add resource limits
resources:
  requests:
    memory: "2Gi"
    cpu: "1000m"
  limits:
    memory: "4Gi"
    cpu: "2000m"
```

### Step 3: Set Environment Variables

```bash
# Set Airflow variables
airflow variables set STORAGE_BUCKET spark-data
airflow variables set CATALOG_TYPE hadoop
# ... other variables
```

## 🎯 Benefits

### **Isolation**
- Each job runs in its own pod
- No interference between jobs
- Clean resource management

### **Scalability**
- Kubernetes can scale pods automatically
- Better resource utilization
- Cluster-wide resource management

### **Monitoring**
- Kubernetes native monitoring
- Pod-level metrics
- Better debugging capabilities

### **Code Reuse**
- Same `spark_job.py` module
- Same configuration approach
- Same business logic

## 🔒 Security

### **Secrets Management**

For production, use Kubernetes secrets:

```yaml
# Create secret
kubectl create secret generic spark-secrets \
  --from-literal=STORAGE_ACCESS_KEY_ID=your-key \
  --from-literal=STORAGE_SECRET_KEY=your-secret

# Reference in deployment
env:
- name: STORAGE_ACCESS_KEY_ID
  valueFrom:
    secretKeyRef:
      name: spark-secrets
      key: STORAGE_ACCESS_KEY_ID
```

### **Network Policies**

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: spark-job-network-policy
spec:
  podSelector:
    matchLabels:
      app: spark-job
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: airflow
```

## 📈 Production Considerations

1. **Resource Planning**: Monitor and adjust resource limits
2. **Image Registry**: Use private registry for production images
3. **Secrets**: Use Kubernetes secrets for sensitive data
4. **Monitoring**: Set up proper monitoring and alerting
5. **Backup**: Regular backups of Airflow metadata
6. **Security**: Implement proper RBAC and network policies
