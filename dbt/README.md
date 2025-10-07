# dbt - Data Transformation Layer

## Overview

This directory contains the dbt (data build tool) project for the Spark-Iceberg data platform. 
The project is designed to work with Apache Spark and Apache Iceberg tables, supporting both AWS Glue Catalog and Hadoop Catalog (S3/MinIO) backends.

dbt transforms raw data into analytics-ready datasets using SQL and Jinja templating, with support for incremental models, testing, and documentation.

## Features

- **Multi-Catalog Support**: Configured for both AWS Glue and Hadoop (MinIO/S3) catalogs
- **Apache Iceberg Tables**: All models use Iceberg table format for ACID transactions
- **Flexible Connection Methods**: Supports Session, Thrift, HTTP, and ODBC connection methods to Spark
- **Environment-Based Configuration**: Uses environment variables for flexible deployment
- **Kubernetes-Ready**: Configured to run Spark jobs on Kubernetes clusters
- **Staging and Marts Layers**: Implements standard data warehouse architecture

## Prerequisites

Before using dbt in this project, ensure you have:

1. **dbt-core** with **dbt-spark** adapter installed (see [Dockerfile.dbt](../Dockerfile.dbt))
2. **Apache Spark** cluster or Spark Thrift Server running or Kubernetes Cluster
3. **Apache Iceberg** libraries available in Spark  (see [Dockerfile.spark](../Dockerfile.spark))
4. **Access to object storage**: MinIO or AWS S3
5. **Catalog service**: AWS Glue or Hadoop-compatible catalog
6. **Environment variables** properly configured (see Configuration section)

## Project Structure

```
dbt/
├── README.md                          # This file
├── dbt_project.yml                    # dbt project configuration
├── profiles.yml                       # Connection profiles (Glue and Hadoop)
├── packages.yml                       # dbt package dependencies
├── models/                            # SQL transformation models
│   ├── schema.yml                     # Model documentation and tests
│   ├── staging/                       # Staging layer models
│   │   └── stg_customers.sql          # Customer staging model
│   └── marts/                         # Business logic / marts layer
│       └── company_employee_count.sql # Aggregated company metrics
├── macros/                            # Custom SQL macros
│   ├── upload_sample_data.sql         # Macro for loading sample data
│   └── log_effective_conf.sql         # Macro for logging Spark configuration
├── analysis/                          # Ad-hoc analysis queries (not built)
├── tests/                             # Custom data tests
├── seeds/                             # CSV files to load as tables
└── snapshots/                         # Slowly changing dimension snapshots
```

## Configuration

### dbt_project.yml

The main project configuration file defines:

- **Project name**: `spark_iceberg_app`
- **Profile**: `hadoop` (can be changed to `glue`)
- **Model paths**: Directories for models, tests, seeds, macros, etc.
- **Model configuration**:
  - Staging models: Materialized as Iceberg tables
  - Marts models: Materialized as Iceberg tables
  - Schema: Dynamically set from `SCHEMA_CUSTOMERS` environment variable

### profiles.yml

Contains two main profile configurations:

#### 1. Hadoop Profile (S3/MinIO Catalog)

Uses Hadoop-compatible catalog with MinIO or S3 storage.

**Key features**:
- REST catalog or Hadoop catalog support
- Works with local MinIO or cloud S3
- Thrift/HTTP connection methods
- Development-friendly configuration

#### 2. Glue Profile (AWS Glue Catalog)

**Disclaimer:** Honestly, I haven't tested this configuration, so may be, you'll need to adjust some configuration  

Uses AWS Glue Data Catalog with Iceberg tables on S3.

**Key features**:
- Glue catalog integration for metadata
- S3A connector for data storage
- Spark adaptive query execution enabled
- Kubernetes execution support

#### 3. Hive Profile (Hive Catalog)

**Disclaimer:** Honestly, I haven't tested this configuration, so may be, you'll need to adjust some configuration

Uses Hive Catalog with Iceberg tables on S3.

**Key features**:
- Hive catalog integration for metadata
- S3A connector for data storage
- Spark adaptive query execution enabled
- Kubernetes execution support

### packages.yml

Declares dbt package dependencies:
- **dbt_utils** (v1.3.1): Provides utility macros and tests

## Environment Variables

### Required Variables

| Variable                | Description                  | Example               |
|-------------------------|------------------------------|-----------------------|
| `SCHEMA_CUSTOMERS`      | Target schema/database name  | `customers_db`        |
| `SPARK_CONN_HOST`       | Spark Thrift Server hostname | `spark-thrift-server` |
| `SPARK_CONN_PORT`       | Spark Thrift Server port     | `10000`               |
| `STORAGE_ENDPOINT`      | S3/MinIO endpoint            | `http://minio:9000`   |
| `STORAGE_ACCESS_KEY_ID` | S3/MinIO access key          | `minioadmin`          |
| `STORAGE_SECRET_KEY`    | S3/MinIO secret key          | `minioadmin`          |
| `STORAGE_BUCKET`        | S3/MinIO bucket name         | `warehouse`           |
| `CATALOG_NAME`          | Iceberg catalog name         | `mycatalog`           |

### Optional Variables

| Variable                    | Description                          | Default     |
|-----------------------------|--------------------------------------|-------------|
| `SPARK_CONNECTION_METHOD`   | Connection method (thrift/http/odbc) | `thrift`    |
| `SPARK_MASTER_URL`          | Spark master URL                     | `local[*]`  |
| `SPARK_NAMESPACE`           | Kubernetes namespace for Spark       | `py-spark`  |
| `SPARK_IMAGE_VERSION`       | Docker image tag for Spark           | `latest`    |
| `SPARK_EXECUTOR_INSTANCES`  | Number of Spark executors            | `3`         |
| `AWS_REGION`                | AWS region                           | `us-east-1` |
| `STORAGE_PATH_STYLE_ACCESS` | Use path-style S3 access             | `true`      |
| `STORAGE_SSL_ENABLED`       | Enable SSL for storage               | `false`     |

### Setting Environment Variables

For **local development** (the one this project is intended for), you can use environment files:

1. [env.minio.example](../env.minio.example) - the one the project was tested with
2. [env.glue.example](../env.glue.example) - I expect you'll need to adjust (add/remove) some env variables.

For **Docker/Kubernetes**, environment variables should be configured in:
- Docker Compose files
- Kubernetes ConfigMaps and Secrets
- Airflow DAG definitions

## Usage

It's part of the project, so it's built with docker:
1. [docker-compose-airflow.yml](../docker-compose-airflow.yml)
2. [k8s and helm](../helm)

__Note:__ I didn't add support for running dbt locally on machine, 
 but you can do this easily (hahaha) by adding all relevant env variables on your machine, add spark-cluster, minIO, etc and run dbt command.
You can try: ``export $(cat env.minio.example | xargs)``

## [Dockerfile.dbt](../Dockerfile.dbt)

the [Dockerfile.dbt](../Dockerfile.dbt) looks little bit complicated. The reason was I wanted to re-use [Dockerfile.dbt](../Dockerfile.dbt) for 
 both [docker-compose](../docker-compose-airflow.yml) and [k8s](../helm) use cases.

So for using Airflow and DBT in [docker-compose](../docker-compose-airflow.yml), I needed to allow using ssh.

This part in [Dockerfile.dbt](../Dockerfile.dbt) , please, never do it in such way
```dockerfile
RUN mkdir -p /root/.ssh && chmod 700 /root/.ssh && mkdir -p /run/sshd
COPY ./dockerkey.pub /root/.ssh/authorized_keys
# Set correct permissions for SSH key
RUN chmod 600 /root/.ssh/authorized_keys
RUN sed -i 's/#PubkeyAuthentication yes/PubkeyAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication no/' /etc/ssh/sshd_config && \
    sed -i 's/UsePAM yes/UsePAM no/' /etc/ssh/sshd_config && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config
# ---- end of permissions part

RUN sed -i 's/#PermitUserEnvironment no/PermitUserEnvironment yes/' /etc/ssh/sshd_config
```
and the same for [Dockerfile.airflow](../Dockerfile.airflow) 

```dockerfile
# Create .ssh directory for airflow user, set ownership/permissions, copy key, and set key permissions
RUN mkdir -p /home/airflow/.ssh && \
    chown airflow:root /home/airflow/.ssh && \
    chmod 700 /home/airflow/.ssh
COPY --chown=airflow:root dockerkey /home/airflow/.ssh/id_rsa
RUN chmod 600 /home/airflow/.ssh/id_rsa
```

and the same reason why I created [start_dbt.sh](../scripts/start_dbt.sh) - script for [Dockerfile.dbt](../Dockerfile.dbt) - support for different scenarios.

## spark configuration

I made the [profiles](profiles.yml) more complicated, but if you want to run it only with [docker-compose](../docker-compose-airflow.yml), 
you don't need all these code `spark.kubernetes` prefix, just remove all this from the file:
```yaml
        "spark.master": "{{ env_var('SPARK_MASTER_URL') if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else 'spark://spark-master:7077' }}"
        # --- K8s configuration not empty only if SPARK_MASTER_URL starts with k8s:// ---
        "spark.kubernetes.namespace": "{{ env_var('SPARK_NAMESPACE', 'py-spark') if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else '' }}"
        "spark.kubernetes.container.image": "{{ 'py-spark-spark:' + env_var('SPARK_IMAGE_VERSION', 'latest') if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else '' }}"
        "spark.executor.instances": "{{ env_var('SPARK_EXECUTOR_INSTANCES', '3') if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else '1' }}"
        "spark.kubernetes.authenticate.driver.serviceAccountName": "{{ env_var('SPARK_SERVICE_ACCOUNT', 'spark') if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else '' }}"
        "spark.kubernetes.container.image.pullPolicy": "IfNotPresent"
        "spark.kubernetes.driver.podTemplateFile": "{{ 'local:///spark/templates/spark_k8s_driver_template.yaml' if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else '' }}"
        "spark.kubernetes.executor.podTemplateFile": "{{ 'local:///spark/templates/spark_k8s_executor_template.yaml' if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else '' }}"
        "spark.app.name": "{{ env_var('SPARK_APP_NAME', 'dbt-spark-default') if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else '' }}"
        "spark.app.id": "{{ env_var('SPARK_JOB_ID', 'default-job') if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else '' }}"
        "spark.driver.bindAddress": "{{ env_var('SPARK_DRIVER_BIND_ADDRESS', '0.0.0.0') if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else '0.0.0.0' }}"
        "spark.driver.host": "{{ env_var('SPARK_DRIVER_HOST') if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else 'dbt' }}"
        "spark.port.maxRetries": "100"
        "spark.eventLog.enabled": "{{ 'false' if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else 'true' }}"
```

but if you run Airflow and DBT from kubernetes only, maybe better to remove all Ninja `if`s from section above and leave hardcoded parameters and/or values from env variables.

## Models

### Staging Layer (`models/staging/`)

Staging models perform initial data cleaning and standardization:

- **stg_customers.sql**: Transforms raw customer data
  - Cleans and validates customer records
  - Standardizes field names and types
  - Materialized as Iceberg table

### Marts Layer (`models/marts/`)

Marts models contain business logic and aggregations:

- **company_employee_count.sql**: Aggregates employee counts by company
  - Joins customer and company data
  - Calculates metrics
  - Materialized as Iceberg table

## Macros

Custom macros are located in `macros/` directory:

### upload_sample_data.sql

Macro for uploading sample data to Iceberg tables. Useful for testing and development.

### log_effective_conf.sql

Macro for logging the effective Spark configuration. Helps debug connection and catalog issues.

**Usage**:
```sql
{{ log_effective_conf() }}
```

__Note:__ I didn't use this one in the latest iteration, since it made me problems with k8s setup with defining configMap per dag execution and I didn't want to deal with it.

### Kubernetes / Airflow

dbt runs are orchestrated by Airflow DAGs (see `dags/dbt_k8s_dag.py`). The DAG:
1. Spins up a Kubernetes pod with dbt image
2. Mounts dbt project directory
3. Sets environment variables
4. Executes dbt commands
5. Cleans up resources

## Troubleshooting

### Common Issues

#### Issue: "Not enough permissions in k8s cluster"

Actually, this one can appear in many faces. 

**Solution**: You need to ensure that you have all relevant permissions for pods, services, logs, etc and create, update, patch and so on. I mean Good luck with this.

#### Issue: "Could not find profile named 'hadoop'"
**Solution**: Ensure `profiles.yml` exists in the dbt directory and contains the profile:
```bash
ls -la dbt/profiles.yml
```

#### Issue: "Table not found in catalog"
**Solution**: 
1. Verify catalog configuration in `profiles.yml`
2. Check schema exists:
```sql
SHOW DATABASES;
```
3. Ensure environment variable `SCHEMA_CUSTOMERS` is set

#### Issue: "S3 Access Denied" or "MinIO Connection Error"
**Solution**:
1. Verify storage credentials in environment variables
2. Check bucket exists and is accessible
3. Verify endpoint URL is correct (include protocol: `http://` or `https://`)

#### Issue: "Iceberg table format not supported"
**Solution**: Ensure Spark has Iceberg runtime JARs loaded:
- Check `spark.sql.extensions` includes `IcebergSparkSessionExtensions`
- Verify `spark.sql.catalog.<catalog-name>` is configured
- Ensure Iceberg JARs are in Spark classpath

### Debugging

#### 1. Check dbt Configuration

```bash
dbt debug --profiles-dir .
```

Review the output for configuration issues.

#### 2. View Compiled SQL

Compiled SQL is generated in `target/compiled/`:

```bash
cat target/compiled/spark_iceberg_app/models/staging/stg_customers.sql
```

#### 3. Enable dbt Logging

```bash
dbt run --profiles-dir . --debug
```

This provides verbose output including SQL statements and Spark logs.

#### 4. Check Spark Logs

When running on Kubernetes, check pod logs:

```bash
kubectl logs <dbt-pod-name> -n py-spark
```

#### 5. Verify Spark Configuration

Use the `log_effective_conf` macro to see actual Spark settings:

```sql
{{ log_effective_conf() }}
```

## Integration with Airflow

The dbt project is integrated with Apache Airflow for scheduled execution. See:
- **DAG**: `dags/dbt_k8s_dag.py` with [Kubernetes](../helm)
- **DAG**: `dags/dbt_dag.py` with [docker-compose](../docker-compose-airflow.yml)

The Airflow DAG handles:
- Environment variable injection
- Kubernetes pod creation
- dbt command execution
- Error handling and retries
- Logging and monitoring

## Additional Resources

- [Building K8s with Helm in this project](../helm)
- [dbt Documentation](https://docs.getdbt.com/)
- [dbt-spark Adapter](https://github.com/dbt-labs/dbt-spark)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [Apache Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html)
