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

```