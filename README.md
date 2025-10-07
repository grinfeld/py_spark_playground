# Airflow, PySpark, Iceberg and DuckDB: build local environment with k8s or Docker Compose with AI (almost) 

This is attempt to create working project with AI for pySpark with Airflow, Iceberg, DuckDB and DBT.

To skip introduction go to [detailed explanation](#detailed-description-for-this-project).

Initially, I asked Cursor.ai to create sample project with different options using latest Airflow 3.x and Spark 4.x.
With option to run with minIO instead of s3 storage.

You can see [cursor_init_chat.md](cursor_init_chat.md). The structure looked nice and initial code at average level, but nothing worked
despite "tests" cursor claimed it passed. After few attempts, I tried to talk with Gemini ([gemini.md](gemini.md)) - improved few things, but still failed to get something running.
Then tried getting help from chatGpt ([chatGpt_airflow3.md](chatGpt_airflow3.md)).

Then with help of reading documentation, refactoring by myself, and very specific/concrete questions to both chatGpt and Gemini, 
I finally succeeded to run app and to submit it spark master.

Funny things: 
* all 3 AIs suggested some mixed configuration for airflow 3.x (the latest) and previous 2.x. So airflow failed to start.
* all 3 AIs tried to put configuration for spark 4 and Iceberg, but only direct question to Gemini (after I found in documentation) if Iceberg works with Spark 4, and it answered that still in work

So downgraded spark to 3.5.6.

Finally, despite getting few wrong suggestions from Gemini and ChatGPT about right spark configurations, 
succeeded to run airflow, to show data and to write it to minIO with and without Iceberg.

As bonus, I added simple DAG using __DuckDB__ without any spark.

__UPDATE__:
1. Shortly after finishing this Apache Iceberg released libraries to support spark 4.x, so I updated all dependencies.
2. Again, shortly after publishing the blog, Airflow released version 3.1.0, so I updated it. too.
3. I decided it could be nice to make all dags to work with k8s, so I added relevant dags for k8s and added script for creating local k8s.

For more details, you can read my blog article :) [Airflow, PySpark, Iceberg: build local environment with AI (almost)](https://mikerusoft.medium.com/airflow-pyspark-iceberg-build-local-environment-with-ai-almost-6fcf608b44e2)
<a href="https://mikerusoft.medium.com/airflow-pyspark-iceberg-build-local-environment-with-ai-almost-6fcf608b44e2" target="_new">
![Airflow, PySpark, Iceberg: build local environment with AI (almost)](https://miro.medium.com/v2/resize:fit:1400/format:webp/1*yRVmIRJ5TTJu7KKaQoc_JA.jpeg "Airflow, PySpark, Iceberg: build local environment with AI (almost)")
</a>

# Detailed description for this project

**Disclaimer:** 
This structure is only for learning and/or development purposes.
Don't use it as is in production - adjust it.
Some of the components use an insecure approach.
I added support for different profiles distinguished by environment variables,
but honestly, I have never tested it except the `hadoop` with `minIO`.
Be smart, update/change it accordingly to your requirements and needs.

## Project Overview

The purpose for this project to create local/dev playground for running Airflow, Spark and DBT.
- Workflow orchestration with Airflow
- Distributed data processing with Spark with and without Spark
- Data transformation and modeling with dbt
- Lightweight analytics with DuckDB

## Key Features

- **Multi-Environment Support**: Run locally with Docker Compose or deploy to Kubernetes using Helm
- **Multiple Catalog Backends**: Support for Iceberg catalogs (Hadoop, AWS Glue, Hive Metastore -  I didn't test the last two)
- **Flexible Storage Options**: Works with MinIO (local S3-compatible storage) or AWS S3
- **Production-Ready Patterns**: Demonstrates best practices for containerization, configuration management, and orchestration

## Architecture Components

### Orchestration Layer
- **Apache Airflow 3.x** - Workflow orchestration platform
    - Web UI (API Server) - User interface and REST API
    - Scheduler - Orchestration brain
    - DAG Processor - DAG parsing service
    - Triggerer - Async task management
    - PostgreSQL - Metadata database
    - LocalExecutor - Job execution (development mode)

### Processing Layer
1. **Apache Spark 4.x** - Distributed data processing
    - Spark Master - Resource management (for docker-compose only)
    - Spark Workers - Distributed computation
    - Spark History Server - Job monitoring and debugging (for docker-compose only)
2. **DuckDB** on Airflow Workers

### Data Layer
- **Apache Iceberg** - Table format for data lakes
- **MinIO** - S3-compatible object storage
- **Multiple Catalog Options**: Hadoop, AWS Glue, Hive Metastore

### Transformation Layer
- **dbt (data build tool)** - SQL-based data transformation and modeling
    - Staging models - Data cleaning and standardization
    - Marts models - Business logic and aggregations

## Deployment Options

This project can be built for two types of local environments:

### 1. Kubernetes with Helm
For production-like environments, deploy to Kubernetes using the provided Helm charts and management script.

The quickest way to get started. Run the entire stack locally using Docker:
```bash
cd helm/
./my_helm -t=v1 --build
```

See **[helm/README.md](helm/README.md)** for detailed Kubernetes deployment instructions, but here the little summary:
It builds Postgres DB, MinIO pod and uploads the example file,
creates airflow containers using Airflow official helm chart
and installs [Kubeflow Spark Operator](https://www.kubeflow.org/docs/components/spark-operator/overview/),
creates rbac roles, role bindings and other staff. I decided to skip here for Spark History Server :).

**Important note about Spark:**
1. All spark jobs use kubernetes controller as spark master
2. There are two use cases for Spark and k8s:
    * Using built-in Apache Spark Kubernetes integration in [dbt_k8s_dag.py](dags/dbt_k8s_dag.py) where spark-driver on dbt container asks k8s controller for executors.
    * Using [Kubeflow Spark Operator](https://www.kubeflow.org/docs/components/spark-operator/overview/) and SparkKubernetesOperator in Airflow (under the hood it uses built-in Apache Spark Kubernetes integration with few more tricks).


### 2. Docker Compose
The quickest way to get started. Run the entire stack locally using Docker:
```bash
./docker_compose.sh up -d --build
```
This creates:
* Airflow Web UI (known in the new version as api-server) - It is responsible for showing you a nice UI and allowing other systems to interact via REST.
* Airflow Scheduler - This one is actual orchestration. The brain part of it. Decides when and what to run according to the scheduling defined by the developers of DAGs.
* DAG Processor - the new part in Airflow 3.x. Once this functionality was a part of Schedular in Airflow 2.x. Now it is the service of its own. Its responsibility to parse DAG,
  It allows decoupling the DAG parsing and updating the DAG code without interrupting the work of all other Airflow services.
* Triggerer - this one is a new part of Airflow 3.x, too. If in previous Airflow you have had hundreds of jobs, you are familiar with the pain point of triggering/waiting/crashing DAGs.
  This new service is like an async queue processor to manage thousands of tasks without tying up a worker slot.
* Postgres - the place where Airflow stores all its data, statuses, etc.
* Executors - the place where your job is done. In the current project, I am using LocalExecutor that runs on a Scheduler instance, but in prod you'll use something more flexible like CeleryExecutor or KubernetesExecutor.
1. For example, in the case of DBT DAG, the actual job is done on the executor itself
2. In the case of Spark, the executor is a service where the Spark submit operation is executed, and the job itself is done on the Spark cluster (spark-master, spark-workers)
* airflow-init - container that runs initial scripts, creates users, etc. It dies immediately after it has finished.
* Spark Master
* Three Spark Executors
* Spark Histroy Server
* DBT container

**Access Points:**
- Airflow UI: http://localhost:8080 (admin/admin)
- Spark Master UI: http://localhost:8081
- Spark History Server: http://localhost:18080
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)

## Dockerfiles

I wanted to create dockerfiles such way where I can use them for both the [Docker Compose](docker-compose-airflow.yml) and the [Kubernetes](helm/).
It means, you can find inside dockerfiles the parts that relevant only for running via [Docker Compose](docker-compose-airflow.yml) or  and the [Kubernetes](helm/).

For example, for k8s all containers mount `templates` folder. The idea to use those templates during Apache Spark k8s operations. It's possible to use those templates from s3, I just tried to make it more simple. But I uploaded templates to minIO if you want to try this approach.

1. **[Dockerfile.airflow](Dockerfile.airflow)**
    * The part which is dealing with private/public key is relevant only for [Docker Compose](docker-compose-airflow.yml) scenario (and don't use it in production either).
2. **[Dockerfile.dbt](Dockerfile.dbt)**
    * The part which is dealing with private/public key and ssh server permissions is relevant only for [Docker Compose](docker-compose-airflow.yml) scenario, too (and don't use it in production either).
    * The part with adding `spark` user to container is because in k8s dbt container become spark-driver, and it's reason why I add SPARK_DRIVER_HOST into dbt profiles and dbt_template, since executors should communicate with driver.
3. **[Dockerfile.spark](Dockerfile.spark)**

To crease time of building images and since [Dockerfile.airflow](Dockerfile.airflow) and [Dockerfile.dbt](Dockerfile.dbt) both use [Dockerfile.spark](Dockerfile.spark), those two images use multi-staging build based on already existed spark image.
It means, you always should ensure that `py-spark-spark` exists before building other two. If you don't want to deal with, just use the scripts I have created:
1. For Docker Compose [docker_compose.sh](README.md#2-docker-compose)
2. For k8s and Helm: [helm/my_helm.sh](README.md#1-kubernetes-with-helm)

## Sample DAGs

The project includes several example DAGs demonstrating different patterns:

### [Empty DAG](dags/empty_dag.py)

Simple logging example (runs with both Docker Compose and Kubernetes)

### [DuckDB DAG](dags/duck_db_dag.py)

Lightweight analytics without Spark (runs with both Docker Compose and Kubernetes).
It reads file from the Object Storage, performs some simple grouping by query and stores result back to the Object Storage.

### Spark Job Without Iceberg

Simple job that creates dataframe, runs some sql operation and writes data to the Object Storage.

* [Docker Compose](dags/spark_job_dag.py) - uses remote Spark cluster (part of [docker-compose-airflow.yml](docker-compose-airflow.yml))
* [k8s](dags/spark_job_k8s_dag.py) - uses Airflow `SparkKubernetesOperator` to trigger creation (via Kubernetes Controller and [Kubeflow Spark Operator](https://www.kubeflow.org/docs/components/spark-operator/overview/)) Spark Cluster: driver and executors.

### Spark Job With Iceberg

Spark job reads data from the Object Storage, performs some grouping by operation and stores data in the Iceberg table (backed by Object Storage) and catalog implementation (hadoop, glue or hive).

* [Docker Compose](dags/spark_iceberg_dag.py) - uses remote Spark cluster (part of [docker-compose-airflow.yml](docker-compose-airflow.yml))
* [k8s](dags/spark_job_iceberg_k8s_dag.py) - uses Airflow `SparkKubernetesOperator` to trigger creation (via Kubernetes Controller and [Kubeflow Spark Operator](https://www.kubeflow.org/docs/components/spark-operator/overview/)) Spark Cluster: driver and executors.

### DBT and Spark (dbt[spark] provider)

Same as previous, but with using DBT. Really overengineered use case, but it's for learning purpose - pardon me.
Here the example again for too much code because of my wish to use the same staff for both Docker Compose and k8s.
Inside the [dbt/profiles.yml](dbt/profiles.yml) you can see all spark configuration relevant for k8s.
If you use Docker Compose only, remote Spark Cluster, you can delete all staff similar to this if statement:
```python
"{{ env_var('SPARK_DRIVER_HOST') if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else 'dbt' }}"
```
otherwise you just can remove everything after `if` as following example:
```python
"{{ env_var('SPARK_DRIVER_HOST')"
```
* [Docker Compose](dags/dbt_dag.py) - but don't use it in such way. Very unsecured.
* [k8s](dags/dbt_k8s_dag.py)

To get little bit more info, look [dbt here](dbt/README.md).

## Project Structure

```
py_spark/
├── README.md                     # Detailed technical documentation
├── dbt/                          # dbt data transformation project
│   └── README.md                 # dbt-specific documentation
├── helm/                         # Kubernetes/Helm deployment
│   └── README.md                 # Helm deployment guide
├── dags/                         # Airflow DAG definitions
├── docker-compose-airflow.yml    # Docker Compose configuration
├── Dockerfile.airflow            # Airflow container image
├── Dockerfile.spark              # Spark container image
├── Dockerfile.dbt                # dbt container image
├── scripts/                      # Helper scripts
├── data/                         # Sample data files
└── requirements.txt              # Python dependencies
```

## Getting Started

### Prerequisites
- Docker and Docker Compose (for local development)
- Kubernetes cluster (based on Docker Desktop) and Helm (for Kubernetes deployment)
- Python 3.11+ (if running components outside containers)

### Quick Start - Docker Compose

1. **Clone the repository**
   ```bash
   cd /path/to/py_spark
   ```

2. **Build and start all services**
   ```bash
   ./docker_compose.sh up -d --build
   ```

3. **Access Airflow UI**
    - Navigate to http://localhost:8080
    - Login with admin/admin
    - Enable and trigger sample DAGs

4. **Monitor Spark jobs**
    - Spark Master UI: http://localhost:8081
    - Spark History Server: http://localhost:18080

5. **Check MinIO storage**
    - MinIO Console: http://localhost:9001
    - Login with minioadmin/minioadmin

### Quick Start - Kubernetes Deployment

**Access Airflow UI**
- Port Forward airflow-api-server: `kubectl port-forward svc/airflow-api-server 8080:8080 --namespace py-spark`
- Navigate to http://localhost:8080
- Login with admin/admin
- Enable and trigger sample DAGs

For Kubernetes deployment instructions, see **[helm/README.md](helm/README.md)**.

## Configuration

### Environment Variables for Docker Compose
The project uses environment variables for configuration. Example files are provided:
- `env.minio.example` - Configuration for MinIO storage (tested)
- `env.glue.example` - Configuration for AWS Glue Catalog (not tested)

### Catalog Options
The platform supports multiple Iceberg catalog implementations:
- **Hadoop Catalog** - File-based catalog (default for local development)
- **AWS Glue Catalog** - AWS managed catalog service (not tested)
- **Hive Metastore** - Traditional Hive catalog (not tested)

## Documentation

### Detailed Component Documentation

- **[Main README](README.md)** - Comprehensive technical documentation covering:
    - Detailed architecture and component descriptions
    - Dockerfile explanations for each service
    - DAG implementation details
    - Configuration and troubleshooting

- **[Helm Deployment Guide](helm/README.md)** - Kubernetes deployment documentation:
    - Helm chart management with `my_helm.sh` script
    - Service-by-service deployment options
    - Upgrade and maintenance procedures
    - Kubernetes-specific troubleshooting

- **[dbt Documentation](dbt/README.md)** - Data transformation layer guide:
    - dbt project structure and models
    - Multi-catalog configuration (Hadoop, Glue, Hive)
    - Environment variable reference
    - Spark configuration for dbt
    - Integration with Airflow and Kubernetes

## Important Notes

### Development vs Production

**This project is designed for learning and development purposes.**

For production use, you should:
- Replace insecure SSH-based communication patterns
- Implement proper secrets management (not plain environment variables)
- Use production-grade executors (CeleryExecutor or KubernetesExecutor)
- Configure proper resource limits and monitoring
- Use external object storage (S3) instead of MinIO
- Implement proper authentication and authorization
- Set up proper logging and alerting

### Technology Versions

- **Airflow**: 3.1.0
- **Spark**: 4.0.0
- **Iceberg**: 1.10.0
- **Python**: 3.11
- **dbt-core**: Latest with dbt-spark adapter
- **PostgreSQL**: Latest (Airflow metadata)

## AI-Assisted Development

This project was developed with assistance from AI tools (Cursor.ai, IntelliJ Junie, Intellij Gemini plg-in, ChatGPT, Gemini, Claude).
While AI provided a good starting structure, significant manual refinement was required to create a working system.
The project serves as a realistic example of AI-assisted development, including both its potential and limitations.

## Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [dbt Documentation](https://docs.getdbt.com/)
- [MinIO Documentation](https://min.io/docs/)
