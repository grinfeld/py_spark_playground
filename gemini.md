# Airflow 3 – Creating a Default User, FAB Auth Manager, DAG Processor, and Triggerer

## Creating a Default User in Airflow 3

In Airflow 3, the `airflow users create` CLI command only exists if you're using the **FAB auth manager**.
By default, Airflow 3 uses **SimpleAuthManager**, which does **not** have this CLI.

### Option 1 — SimpleAuthManager (default)

Define users via `airflow.cfg` or environment variables.

**Example airflow.cfg:**
```ini
[core]
simple_auth_manager_users = admin:admin
simple_auth_manager_passwords_file = /opt/airflow/passwords.json
```

**Example env vars:**
```bash
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS="admin:admin"
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE="/opt/airflow/passwords.json"
```

Passwords are generated on startup and logged, or stored in the given JSON file.

### Option 2 — FAB Auth Manager (with `airflow users create`)

Requirements:
- Airflow ≥ 3.0.2
- Python < 3.13 (3.12 recommended)

**Steps:**
```bash
pip install "apache-airflow-providers-fab"
export AIRFLOW__CORE__AUTH_MANAGER="airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
airflow db upgrade
airflow fab-db migrate  # Create FAB-specific tables
airflow users create   --username admin   --firstname A   --lastname D   --email admin@example.com   --role Admin   --password '***'
```

---

## FAB Auth Manager "session" table missing

If using FAB with `database` session backend, you may get:

```
psycopg2.errors.UndefinedTable: relation "session" does not exist
```

### Fix A — Use secure cookies (no DB table)
```bash
export AIRFLOW__FAB__SESSION_BACKEND=securecookie
```

### Fix B — Create the table
```bash
export AIRFLOW__FAB__SESSION_BACKEND=database
airflow db upgrade
airflow fab-db migrate
```

---

## Airflow 3 Health Check UI

**MetaDatabase** ✅ – metadata DB healthy  
**Scheduler** ✅ – scheduler running  
**Triggerer** ❌ – triggerer process not running  
**Dag Processor** ❌ – DAG processor process not running  

### DAG Processor

A separate process in Airflow 3 for parsing DAG files and storing metadata in DB.

**Local run:**
```bash
airflow dag-processor
```

**Docker Compose:**
```yaml
dag-processor:
  <<: *airflow-common
  command: dag-processor
```

### Triggerer

A separate process that handles **deferrable operators** by waiting asynchronously for events.

**Local run:**
```bash
airflow triggerer
```

**Docker Compose:**
```yaml
triggerer:
  <<: *airflow-common
  command: triggerer
```

---

## Example Docker Compose for Airflow 3 with DAG Processor & Triggerer

```yaml
version: '3'

x-airflow-common:
  &airflow-common
  image: apache/airflow:3.0.0-python3.12
  environment:
    &airflow-common-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
  volumes:
    - ./dags:/opt/airflow/dags
    - ./logs:/opt/airflow/logs
    - ./plugins:/opt/airflow/plugins
  depends_on:
    postgres:
      condition: service_healthy

services:
  postgres:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
      start_period: 5s
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data

  webserver:
    <<: *airflow-common
    command: webserver
    ports:
      - "8080:8080"

  scheduler:
    <<: *airflow-common
    command: scheduler

  triggerer:
    <<: *airflow-common
    command: triggerer

  dag-processor:
    <<: *airflow-common
    command: dag-processor

  airflow-init:
    <<: *airflow-common
    command: bash -c "airflow db init && airflow users create                       --username admin                       --firstname Admin                       --lastname User                       --role Admin                       --email admin@example.com                       --password admin"

volumes:
  postgres-db-volume:
```

---

**Summary:**  
- SimpleAuthManager = default, define users in config.  
- FAB Auth Manager = install provider, run `fab-db migrate`, then `airflow users create`.  
- DAG Processor = parses DAG files in separate process.  
- Triggerer = handles async deferrable operators.

---

## Docker Compose (`docker-compose-airflow.yml`) Review

This is a review of the main `docker-compose-airflow.yml` file, focusing on improving maintainability, security, and stability for a production-ready environment.

### 1. Code Quality: Reduce Repetition with YAML Anchors

**Problem:** The configuration for Airflow services (`build`, `volumes`, `environment`, etc.) and Spark workers was repeated multiple times, making the file difficult to read and maintain.

**Solution:** Use YAML anchors (`x-airflow-common`, `x-spark-worker-common`) to define common configurations once and reuse them across multiple services. This significantly cleans up the file.

### 2. Critical Fix: Make the `airflow-init` Service Robust

**Problem:** The `airflow-init` service had several critical issues:
- **Broken Command:** The `airflow users create` command was not correctly chained with `&&`, causing it to fail.
- **Not Idempotent:** The service would fail on restarts because it tried to create a user that already existed.
- **Incorrect Healthcheck:** The healthcheck pointed to a webserver that doesn't run in the `init` container.
- **Unused Environment Variables:** The `_AIRFLOW...` variables are from older community images and have no effect here.

**Solution:** The command was replaced with a robust, idempotent one that checks if the user exists before attempting creation. The incorrect healthcheck and unused environment variables were removed.

### 3. Critical Security Fix: Use a Static Fernet Key

**Problem:** The `AIRFLOW__CORE__FERNET_KEY` was set to `''`. When blank, Airflow generates a new key on every startup. If a container restarts, the new key cannot decrypt secrets (like connection passwords) created with the old key, leading to task failures.

**Solution:** A static Fernet key must be generated once and used for all Airflow services to ensure secrets are always accessible.

### 4. Configuration Fix: Run the Airflow Webserver UI

**Problem:** The `airflow-webserver` service was started with `command: api-server`, which only runs the REST API, not the user interface.

**Solution:** The command was changed to `webserver` to run the full Airflow UI.

### Consolidated `docker-compose-airflow.yml` Improvements

The following diff applies all the recommended changes.

```diff
--- a/Users/grinfeld/IdeaProjects/grinfeld/py_spark/docker-compose-airflow.yml
+++ b/Users/grinfeld/IdeaProjects/grinfeld/py_spark/docker-compose-airflow.yml
@@ -1,20 +1,33 @@
+x-airflow-common:
+  &airflow-common
+  build:
+    context: .
+    dockerfile: Dockerfile.airflow
+  env_file:
+    - ./env.minio.example
+  environment:
+    &airflow-common-env
+    AIRFLOW__CORE__EXECUTOR: LocalExecutor
+    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
+    # CRITICAL: Generate a static key with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
+    AIRFLOW__CORE__FERNET_KEY: 'YOUR_STATIC_GENERATED_KEY_HERE'
+    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
+    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
+    AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'
+    AIRFLOW__CORE__ENABLE_XCOM_PICKLING: 'true'
+    AIRFLOW__CORE__AUTH_MANAGER: "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
+    AIRFLOW__FAB__SESSION_BACKEND: "securecookie"
+    # PySpark and Java environment
+    JAVA_HOME: /usr/local/openjdk-17
+    SPARK_HOME: /opt/spark
+    SPARK_MASTER_URL: spark://spark-master:7077
+  volumes:
+    - ./dags:/opt/airflow/dags
+    - ./logs:/opt/airflow/logs
+    - ./plugins:/opt/airflow/plugins
+    - ./data:/opt/airflow/data
+    - ./requirements.txt:/opt/airflow/requirements.txt
+  networks:
+    - spark-network
+  restart: always
+
 services:
   # Apache Airflow
   airflow-webserver:
-    build:
-      context: .
-      dockerfile: Dockerfile.airflow
+    <<: *airflow-common
     depends_on:
       - airflow-init
-    command: api-server
-    env_file:
-      - ./env.minio.example
-    environment:
-      &airflow-common-env
-      AIRFLOW__CORE__EXECUTOR: LocalExecutor
-      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
-      AIRFLOW__CORE__FERNET_KEY: ''
-      AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
-      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
-      AIRFLOW__SCHEDULER__ENABLE_HEALTH_CHECK: 'true'
-      AIRFLOW__CORE__ENABLE_XCOM_PICKLING: 'true'
-      AIRFLOW__CORE__AUTH_MANAGER: "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"
-      AIRFLOW__FAB__SESSION_BACKEND: "securecookie"
-      # PySpark and Java environment
-      JAVA_HOME: /usr/local/openjdk-17
-      SPARK_HOME: /opt/spark
-      SPARK_MASTER_URL: spark://spark-master:7077
-    volumes:
-      - ./dags:/opt/airflow/dags
-      - ./logs:/opt/airflow/logs
-      - ./plugins:/opt/airflow/plugins
-      - ./data:/opt/airflow/data
-      - ./requirements.txt:/opt/airflow/requirements.txt
+    command: webserver
     ports:
       - "8080:8080"
     healthcheck:
@@ -22,111 +35,40 @@
       interval: 10s
       timeout: 10s
       retries: 5
-    restart: always
-    networks:
-      - spark-network
 
   airflow-scheduler:
-    build:
-      context: .
-      dockerfile: Dockerfile.airflow
+    <<: *airflow-common
     depends_on:
       - airflow-init
     command: scheduler
-    env_file:
-      - ./env.minio.example
-    environment:
-      <<: *airflow-common-env
-      SPARK_MASTER_URL: spark://spark-master:7077
-    volumes:
-      - ./dags:/opt/airflow/dags
-      - ./logs:/opt/airflow/logs
-      - ./plugins:/opt/airflow/plugins
-      - ./data:/opt/airflow/data
-      - ./requirements.txt:/opt/airflow/requirements.txt
     healthcheck:
       test: ["CMD-SHELL", 'airflow jobs check --job-type SchedulerJob --hostname "$${HOSTNAME}"']
       interval: 10s
       timeout: 10s
       retries: 5
-    restart: always
-    networks:
-      - spark-network
 
   airflow-dag-processor:
-    build:
-      context: .
-      dockerfile: Dockerfile.airflow
+    <<: *airflow-common
     depends_on:
       - airflow-init
     command: dag-processor
-    env_file:
-      - ./env.minio.example
-    environment:
-      <<: *airflow-common-env
-      SPARK_MASTER_URL: spark://spark-master:7077
-    volumes:
-      - ./dags:/opt/airflow/dags
-      - ./logs:/opt/airflow/logs
-      - ./plugins:/opt/airflow/plugins
-      - ./data:/opt/airflow/data
-      - ./requirements.txt:/opt/airflow/requirements.txt
-    restart: always
-    networks:
-      - spark-network
 
   airflow-trigger:
-    build:
-      context: .
-      dockerfile: Dockerfile.airflow
+    <<: *airflow-common
     depends_on:
       - airflow-init
     command: triggerer
-    env_file:
-      - ./env.minio.example
-    environment:
-      <<: *airflow-common-env
-      SPARK_MASTER_URL: spark://spark-master:7077
-    volumes:
-      - ./dags:/opt/airflow/dags
-      - ./logs:/opt/airflow/logs
-      - ./plugins:/opt/airflow/plugins
-      - ./data:/opt/airflow/data
-      - ./requirements.txt:/opt/airflow/requirements.txt
-    restart: always
-    networks:
-      - spark-network
 
   airflow-init:
-    build:
-      context: .
-      dockerfile: Dockerfile.airflow
+    <<: *airflow-common
     depends_on:
       - postgres
-    environment:
-      <<: *airflow-common-env
-      _AIRFLOW_DB_MIGRATE: 'true'
-      _AIRFLOW_WWW_USER_CREATE: 'true'
-      _AIRFLOW_WWW_USER_USERNAME: ${_AIRFLOW_WWW_USER_USERNAME:-admin}
-      _AIRFLOW_WWW_USER_PASSWORD: ${_AIRFLOW_WWW_USER_PASSWORD:-admin}
-      _PIP_ADDITIONAL_REQUIREMENTS: ${_PIP_ADDITIONAL_REQUIREMENTS:-}
-    volumes:
-      - ./dags:/opt/airflow/dags
-      - ./logs:/opt/airflow/logs
-      - ./plugins:/opt/airflow/plugins
-      - ./data:/opt/airflow/data
-      - ./requirements.txt:/opt/airflow/requirements.txt
+    # This command is now idempotent and correctly formatted.
+    # It checks if the user exists before trying to create one.
     command: >
       bash -c "
-        airflow db migrate && \
-        airflow fab-db migrate \
-        airflow users create \
-                  --username admin \
-                  --firstname FIRST_NAME \
-                  --lastname LAST_NAME \
-                  --role Admin \
-                  --email admin@example.org \
-                  --password admin
+        airflow db upgrade &&
+        airflow fab-db migrate &&
+        (airflow users list | grep -q 'admin' || airflow users create --username admin --firstname FIRST_NAME --lastname LAST_NAME --role Admin --email admin@example.org --password admin)
       "
-    healthcheck:
-      test: ["CMD", "curl", "--fail", "http://localhost:8080/health"]
-      interval: 10s
-      timeout: 10s
-      retries: 5
     restart: on-failure
-    networks:
-      - spark-network
 
   # PostgreSQL for Airflow metadata
   postgres:
@@ -166,6 +108,27 @@
     networks:
       - spark-network
 
+x-spark-worker-common:
+  &spark-worker-common
+  build:
+    context: .
+    dockerfile: Dockerfile.spark
+  depends_on:
+    - spark-master
+    - minio
+  env_file:
+    - ./env.minio.example
+  environment:
+    SPARK_MODE: worker
+    SPARK_MASTER_URL: spark://spark-master:7077
+    SPARK_WORKER_MEMORY: 1G
+    SPARK_WORKER_CORES: 1
+    SPARK_RPC_AUTHENTICATION_ENABLED: no
+    SPARK_RPC_ENCRYPTION_ENABLED: no
+    SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED: no
+    SPARK_SSL_ENABLED: no
+  volumes: &spark-volumes
+    - ./data:/app/data:ro
+    - ./output:/app/output
+    - spark_logs:/opt/spark/logs
+    - spark_work:/opt/spark/work
+  networks:
+    - spark-network
+  restart: unless-stopped
+  command: >
+    sh -c "
+      sleep 15 &&
+      /opt/spark/sbin/start-worker.sh spark://spark-master:7077 &&
+      tail -f /opt/spark/logs/spark-*-org.apache.spark.deploy.worker.Worker-*.out
+    "
+
   # Spark Master
   spark-master:
     build:
@@ -184,8 +147,7 @@
     ports:
       - "8081:8080"  # Spark Master UI (8080 inside container)
       - "7077:7077"  # Spark Master RPC
-    volumes:
-      - ./data:/app/data:ro
-      - ./output:/app/output
-      - spark_logs:/opt/spark/logs
-      - spark_work:/opt/spark/work
+    volumes: *spark-volumes
     networks:
       - spark-network
     restart: unless-stopped
@@ -197,71 +159,19 @@
 
   # Spark Worker 1
   spark-worker-1:
-    build:
-      context: .
-      dockerfile: Dockerfile.spark
+    <<: *spark-worker-common
     container_name: spark-worker-1
-    depends_on:
-      - spark-master
-      - minio
-    env_file:
-      - ./env.minio.example
-    environment:
-      &spark-worker-common-env
-      SPARK_MODE: worker
-      SPARK_MASTER_URL: spark://spark-master:7077
-      SPARK_WORKER_MEMORY: 1G
-      SPARK_WORKER_CORES: 1
-      SPARK_RPC_AUTHENTICATION_ENABLED: no
-      SPARK_RPC_ENCRYPTION_ENABLED: no
-      SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED: no
-      SPARK_SSL_ENABLED: no
-    volumes:
-      - ./data:/app/data:ro
-      - ./output:/app/output
-      - spark_logs:/opt/spark/logs
-      - spark_work:/opt/spark/work
-    networks:
-      - spark-network
-    restart: unless-stopped
-    command: >
-      sh -c "
-        sleep 20 &&
-        /opt/spark/sbin/start-worker.sh spark://spark-master:7077 &&
-        tail -f /opt/spark/logs/spark-*-org.apache.spark.deploy.worker.Worker-*.out
-      "
 
   # Spark Worker 2
   spark-worker-2:
-    build:
-      context: .
-      dockerfile: Dockerfile.spark
+    <<: *spark-worker-common
     container_name: spark-worker-2
-    depends_on:
-      - spark-master
-      - minio
-    env_file:
-      - ./env.minio.example
-    environment:
-      <<: *spark-worker-common-env
-    volumes:
-      - ./data:/app/data:ro
-      - ./output:/app/output
-      - spark_logs:/opt/spark/logs
-      - spark_work:/opt/spark/work
-    networks:
-      - spark-network
-    restart: unless-stopped
-    command: >
-      sh -c "
-        sleep 20 &&
-        /opt/spark/sbin/start-worker.sh spark://spark-master:7077 &&
-        tail -f /opt/spark/logs/spark-*-org.apache.spark.deploy.worker.Worker-*.out
-      "
 
   # Spark Worker 3
   spark-worker-3:
-    build:
-      context: .
-      dockerfile: Dockerfile.spark
+    <<: *spark-worker-common
     container_name: spark-worker-3
-    depends_on:
-      - spark-master
-      - minio
-    env_file:
-      - ./env.minio.example
-    environment:
-      <<: *spark-worker-common-env
-    volumes:
-      - ./data:/app/data:ro
-      - ./output:/app/output
-      - spark_logs:/opt/spark/logs
-      - spark_work:/opt/spark/work
-    networks:
-      - spark-network
-    restart: unless-stopped
-    command: >
-      sh -c "
-        sleep 20 &&
-        /opt/spark/sbin/start-worker.sh spark://spark-master:7077 &&
-        tail -f /opt/spark/logs/spark-*-org.apache.spark.deploy.worker.Worker-*.out
-      "
 
 
 volumes:

---

## August 26, 2025 - Spark/Iceberg Debugging Session

### 1. `[REQUIRES_SINGLE_PART_NAMESPACE]`

**Problem:** The initial Spark job failed with `pyspark.errors.exceptions.captured.AnalysisException: [REQUIRES_SINGLE_PART_NAMESPACE] spark_catalog requires a single-part namespace`.

**Root Cause:** The default Spark session uses an in-memory catalog that doesn't understand multi-part identifiers like `database.table`. The Spark session was not configured for Iceberg.

**Solution:** The Spark session creation was updated to include the necessary Iceberg extensions and catalog configurations. The table creation logic was refactored to use the modern `df.writeTo(...).create()` API.

### 2. `py4j.protocol.Py4JJavaError: ... Unable to load credentials` (Driver)

**Problem:** After configuring the Iceberg catalog, the job failed with `software.amazon.awssdk.core.exception.SdkClientException: Unable to load credentials from any of the providers in the chain`.

**Root Cause:** The AWS SDK for Java, used by Iceberg to communicate with MinIO, could not find any access credentials. The Hadoop-specific properties (`spark.hadoop.fs.s3a.access.key`) are not read by the newer AWS SDK v2 used by Iceberg's `S3FileIO`.

**Solution:** The Airflow DAG (`spark_iceberg_dag.py`) was updated to pass credentials as environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`) using the `env_vars` parameter of the `SparkSubmitOperator`. This allows the AWS SDK to find them automatically.

### 3. Generic `createTable` Error

**Problem:** The job failed with a generic error originating from `org.apache.spark.sql.catalog.Catalog.createTable`.

**Root Cause:** The Spark script was still using the older, less reliable V1 data source API (`spark.catalog.createTable`) to create the Iceberg table.

**Solution:** The Spark script (`iceberg_spark.py`) was refactored to exclusively use the modern V2 API (`df.writeTo(...).create()` and `df.writeTo(...).append()`), which is atomic and more robust for Iceberg operations.

### 4. `Unable to load credentials` (Executor)

**Problem:** The job failed again with `Unable to load credentials`, but this time the error occurred in a task running on a Spark executor.

**Root Cause:** Credentials configured for the Spark driver (via `env_vars`) are not automatically propagated to the Spark executor processes. Each executor runs in its own environment and needs its own credentials.

**Solution:** The Airflow DAG was updated to use the `spark.executorEnv.*` properties within the `--conf` dictionary. This correctly instructs Spark to set the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables on each executor container.

### 5. `The AWS Access Key Id you provided does not exist in our records`

**Problem:** The job failed with a `403 Forbidden` error from S3: `The AWS Access Key Id you provided does not exist in our records`.

**Root Cause:** This error message is from the real AWS S3 service, indicating the Spark job was trying to connect to `s3.amazonaws.com` instead of the local MinIO instance. Iceberg's internal S3 client (`S3FileIO`) does not automatically inherit the endpoint from the Hadoop configuration (`spark.hadoop.fs.s3a.endpoint`). It requires its own explicit endpoint configuration.

**Solution:** The `ConfigManager` (`dags/utils/config_manager.py`) was updated to add the `s3.endpoint` property directly to the Iceberg catalog configuration (e.g., `spark.sql.catalog.my-catalog.s3.endpoint=http://minio:9000`). This explicitly tells Iceberg's client to use MinIO.

### 6. Clarification on Credential vs. Endpoint Properties

**Question:** The user correctly noted that the `s3.endpoint` property was being recognized, but the `s3.access-key` property was not.

**Explanation:** This is a key subtlety. Iceberg's `S3FileIO` client (using AWS SDK v2) **does** recognize the custom `s3.endpoint` property. However, the AWS SDK v2 **does not** recognize custom credential properties like `s3.access-key`. It strictly follows its standard credential provider chain (looking for environment variables, Java properties, etc.). This is why setting `spark.executorEnv.AWS_ACCESS_KEY_ID` is the correct and necessary solution for providing credentials to executors.

### 7. `DataSourceRDD` I/O Error

**Problem:** The job failed with a low-level I/O error deep inside Spark's `DataSourceRDD`, indicating a problem accessing the data files.

**Root Cause:** A configuration mismatch in `env.minio.example`. The `STORAGE_BUCKET` was defined as `spark-data`, but the `CATALOG_WAREHOUSE_PATH` was set to `s3a://my-catalog`. This incorrectly instructed Spark to use a bucket named `my-catalog` which did not exist.

**Solution:** The `CATALOG_WAREHOUSE_PATH` was corrected to `s3a://spark-data/my-catalog`, ensuring the warehouse path pointed to a directory *inside* the correct bucket.

### 8. `java.lang.AbstractMethodError`

**Problem:** The job failed with `java.lang.AbstractMethodError: Receiver class ... does not define or inherit an implementation...`.

**Root Cause:** This error is a classic sign of a dependency version conflict on the Spark classpath. The `--packages` list was loading multiple, incompatible versions of Iceberg libraries, specifically `iceberg-spark-runtime`, `iceberg-aws-bundle`, and the unnecessary `iceberg-hive-runtime`.

**Solution:** The `packages` list in the `SparkSubmitOperator` was simplified and unified to a clean, consistent set of libraries known to be compatible.

```diff
--- a/Users/grinfeld/IdeaProjects/grinfeld/py_spark/dags/spark_iceberg_dag.py
+++ b/Users/grinfeld/IdeaProjects/grinfeld/py_spark/dags/spark_iceberg_dag.py
@@ -43,24 +43,21 @@
 )
 def spark_job_dag():
     """Airflow 3.0 DAG using task decorators."""
-
-    logging.info(os.getcwd())
-    main_file = f"{os.getcwd()}/dags/spark/iceberg_spark.py"
-    logging.info(f"Got catalog config: {config_manager.get_catalog_configs()}")
-    logging.info(f"Got catalog name: {config_manager.catalog_config.catalog_name}")
     spark_submit = SparkSubmitOperator(
         task_id='spark_job_submit',
-        application=main_file,
+        # Use a path relative to the DAGs folder for robustness
+        application="spark/iceberg_spark.py",
         name='spark_job',
         deploy_mode='client',
-        packages="org.apache.hadoop:hadoop-aws:3.3.4"
-                ",software.amazon.awssdk:bundle:2.32.29"
-                ",com.amazonaws:aws-java-sdk-bundle:1.12.262"
-                ",org.apache.iceberg:iceberg-aws-bundle:1.4.2"
-                ",org.apache.iceberg:iceberg-hive-runtime:1.4.2"
-                ",org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2"
-        ,
+        # Use a clean, unified, and stable set of packages.
+        # - All iceberg versions are the same (1.5.0 is a known stable version).
+        # - Removed unnecessary iceberg-hive-runtime for hadoop catalog.
+        # - Removed redundant aws sdk bundles.
+        packages="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
+                 "org.apache.iceberg:iceberg-aws-bundle:1.5.0,"
+                 "org.apache.hadoop:hadoop-aws:3.3.6",
         conf=configs,
         env_vars={
             "AWS_ACCESS_KEY_ID": config_manager.storage_config.access_key,

```
```