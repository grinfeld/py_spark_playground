
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
