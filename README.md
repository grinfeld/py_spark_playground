# Airflow, PySpark, Iceberg, and DuckDB: build local environment with k8s or Docker Compose with AI (almost) 

This is an attempt to create a working project with AI for PySpark with Airflow, Iceberg, DuckDB, and DBT.

To skip the introduction, go to [detailed explanation](#detailed-description-for-this-project).

Initially, I asked Cursor.ai to create a sample project with different options using the latest Airflow 3.x and Spark 4.x.
With the option to run with minIO instead of S3 storage.

You can see [cursor_init_chat.md](cursor_init_chat.md). The structure looked nice, and the initial code was at an average level, but nothing worked
despite the "tests" cursor claiming it passed. After a few attempts, I tried to talk with Gemini ([gemini.md](gemini.md)) - improved a few things, but still failed to get something running.
Then, I tried getting help from ChatGPT ([chatGpt_airflow3.md](chatGpt_airflow3.md)).

Then, with the help of reading documentation, refactoring by myself, and very specific/concrete questions to both ChatGPT and Gemini, 
I finally succeeded in running the app and submitting it to the Spark master.

Funny things: 
* all 3 AIs suggested some mixed configuration for airflow 3.x (the latest) and previous 2.x. So airflow failed to start.
* all 3 AIs tried to put configuration for Spark 4 and Iceberg, but only asked a direct question to Gemini (after I found in the documentation) if Iceberg works with Spark 4, and it answered that it is still in work

So, downgraded Spark to 3.5.6.

Finally, despite getting a few wrong suggestions from Gemini and ChatGPT about the right spark configurations, 
succeeded in running Airflow, to show data, and to write it to minIO with and without Iceberg.

As a bonus, I added a simple DAG using __DuckDB__ without any Spark.

__UPDATE__:
1. Shortly after finishing this, Apache Iceberg released libraries to support Spark 4.x, so I updated all dependencies.
2. Again, shortly after publishing the blog, Airflow released version 3.1.0, so I updated it, too.
3. I decided it could be nice to make all dags to work with k8s, so I added relevant dags for k8s and added a script for creating local k8s.

So, you can read my blog article about the journey :) [Airflow, PySpark, Iceberg: build local environment with AI (almost)](https://mikerusoft.medium.com/airflow-pyspark-iceberg-build-local-environment-with-ai-almost-6fcf608b44e2) or just proceed to the project description [here](REAME.md#Detailed-description-for-this-project).
<a href="https://mikerusoft.medium.com/airflow-pyspark-iceberg-build-local-environment-with-ai-almost-6fcf608b44e2" target="_new">
![Airflow, PySpark, Iceberg: build local environment with AI (almost)](https://miro.medium.com/v2/resize:fit:1400/format:webp/1*yRVmIRJ5TTJu7KKaQoc_JA.jpeg "Airflow, PySpark, Iceberg: build local environment with AI (almost)")
</a>

# Detailed description for this project

**Disclaimer:** 
This structure is only for learning and/or development purposes.
Don't use it as is in production - adjust it.
Some of the components use an insecure approach.
I added support for different profiles distinguished by environment variables,
However, I have never tested it except for `hadoop` with `minIO`.
Please be smart, update/change it according to your needs.

## Project Overview

The purpose of this project is to create a local/dev playground for running Airflow, Spark, and DBT.
- Workflow orchestration with Airflow
- Distributed data processing with Spark, with and without Spark
- Data transformation and modeling with dbt
- Lightweight analytics with DuckDB

## Key Features

- **Multi-Environment Support**: Run locally with Docker Compose or deploy to Kubernetes using Helm
- **Multiple Catalog Backends**: Support for Iceberg catalogs (Hadoop, AWS Glue, Hive Metastore -  I didn't test the last two, but they should work, since I had similar [project it worked](https://github.com/grinfeld/iceberg))
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
    - Spark Master - Resource management (for Docker Compose only)
    - Spark Workers - Distributed computation
    - Spark History Server - Job monitoring and debugging (for Docker Compose only)
2. **DuckDB** on Airflow Workers

### Data Layer
- **Apache Iceberg** - Table format for data lakes
- **MinIO** - S3-compatible object storage
- **Multiple Catalog Options**: Hadoop, AWS Glue, Hive Metastore

### Streaming
- **Kafka Cluster** - single node kafka cluster for streaming data
* Includes Kafka-UI to view and manage Kafka Cluster

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

See **[helm/README.md](helm/README.md)** for detailed Kubernetes deployment instructions, but here is a little summary:
It builds a Postgres DB, a MinIO pod, and uploads the example file, creates Airflow containers using the Airflow official helm chart, and installs [Kubeflow Spark Operator](https://www.kubeflow.org/docs/components/spark-operator/overview/),
creates RBAC roles, role bindings, and other staff. I decided to skip Spark History Server in k8s :).

__Note:__ If I remember correctly, to add the Spark History Server, you just need to change [spark-defaults.conf](spark-defaults.conf) to point to the S3 bucket. Something like that:
```
spark.eventLog.dir              s3://bucket/spark_events # spark to write events to
spark.history.fs.logDirectory   s3://bucket/spark_events # history server to read events from
```
and of course, create Spark History deployment.

**Important note about Spark:**
1. All Spark jobs use the Kubernetes controller as the Spark master
2. There are two use cases for Spark and k8s:
    * Using built-in Apache Spark Kubernetes integration in [dbt_k8s_dag.py](dags/dbt_k8s_dag.py) where spark-driver on dbt container asks k8s controller for executors.
    * Using [Kubeflow Spark Operator](https://www.kubeflow.org/docs/components/spark-operator/overview/) and SparkKubernetesOperator in Airflow.


### 2. Docker Compose
The quickest way to get started. Run the entire stack locally using Docker:
```bash
./docker_compose.sh up -d --build
```
This creates:
* Airflow Web UI (known in the new version as api-server) - It is responsible for showing you a nice UI and allowing other systems to interact via REST.
* Airflow Scheduler - This one is actual orchestration. The brain part of it. Decides when and what to run according to the scheduling defined by the developers of DAGs.
* DAG Processor - the new part in Airflow 3.x. Once, this functionality was a part of Schedular in Airflow 2.x. Now this service is responsible for parsing DAGs,
  It allows decoupling the DAG parsing and updating the DAG code without interrupting the work of all other Airflow services.
* Triggerer - this one is a new part of Airflow 3.x, too. If you have had hundreds of jobs in previous Airflow, you are familiar with the pain point of triggering/waiting/crashing DAGs.
  This new service functions like an asynchronous queue processor, managing thousands of tasks without occupying a worker slot.
* Postgres - the place where Airflow stores all its data, statuses, etc.
* Airflow Executors - the place where your job is done. In the current project, I am using LocalExecutor, which runs on a Scheduler instance. However, in production, you'll use something more flexible, such as CeleryExecutor or KubernetesExecutor.
  
______ 1. For example, in the case of DBT dag, the Airflow executor (worker) calls DBT container via ssh.

______ 2. In the case of Spark, the Airflow executor (worker) is a service where the Spark submit operation is executed (meaning, this is Spark Driver), the job itself is done on the Spark cluster (spark-master, spark-workers).

* airflow-init - container that runs initial scripts, creates users, etc. It dies immediately after it has finished.
* Spark Master - The Master performs tasks like scheduling, monitoring, and resource allocation.
* 3 Spark Workers - worker runs the executor process where the individual tasks are assigned by the Spark Driver.
* Spark History Server - the server to view and analyze spark jobs that have already finished.
* DBT container

**Access Points:**
- Airflow UI: http://localhost:8080 (admin/admin)
- Spark Master UI: http://localhost:8081
- Spark History Server: http://localhost:18080
- MinIO Console: http://localhost:9001 (minioadmin/minioadmin)

## Dockerfiles

I wanted to create dockerfiles in such a way that I can use them for both the [Docker Compose](docker-compose-airflow.yml) and the [Kubernetes](helm/).
It means, you can find inside dockerfiles the parts that relevant only for running via [Docker Compose](docker-compose-airflow.yml) or  and the [Kubernetes](helm/).

For example, for k8s, all containers mount the `templates` folder. The idea is to use those templates during Apache Spark k8s operations. It's possible to use those templates from S3; I just tried to simplify the process. However, I have uploaded templates to minIO, so you can try this approach if you'd like.

1. **[Dockerfile.airflow](Dockerfile.airflow)**
    * The part that deals with private/public key is relevant only for [Docker Compose](docker-compose-airflow.yml) scenario (and don't use it in production either).
2. **[Dockerfile.dbt](Dockerfile.dbt)**
    * The part that deals with private/public key and SSH server permissions is relevant only for [Docker Compose](docker-compose-airflow.yml) scenario, too (and don't use it in production either).
    * The part with adding `spark` user to container is because in k8s dbt container becomes Spark Driver, and it's the reason why I add SPARK_DRIVER_HOST into dbt profiles and dbt_template, since executors should communicate with Spark Driver.
3. **[Dockerfile.spark](Dockerfile.spark)**

To reduce the time of building images and since [Dockerfile.airflow](Dockerfile.airflow) and [Dockerfile.dbt](Dockerfile.dbt) both use [Dockerfile.spark](Dockerfile.spark), those two images use a multi-stage build based on already existing spark image.
It means you should always ensure that `py-spark-spark` exists before building the other two. If you don't want to deal with it, just use the scripts I have created:
1. For Docker Compose [docker_compose.sh](README.md#2-docker-compose)
2. For k8s and Helm: [helm/my_helm.sh](README.md#1-kubernetes-with-helm)

## Sample DAGs

The project includes several example DAGs demonstrating different patterns:

### [Empty DAG](dags/empty_dag.py)

Simple logging example (runs with both Docker Compose and Kubernetes)

### [DuckDB DAG](dags/duck_db_dag.py)

Lightweight analytics without Spark (runs with both Docker Compose and Kubernetes).
It reads a file from Object Storage, performs simple grouping by query, and stores the result back in Object Storage.

### Spark Job Without Iceberg

Simple job that creates a dataframe, runs some SQL operation, and writes data to the Object Storage.

* [Docker Compose](dags/spark_job_dag.py) - uses remote Spark cluster (part of [docker-compose-airflow.yml](docker-compose-airflow.yml))
* [k8s](dags/spark_job_k8s_dag.py) - uses Airflow `SparkKubernetesOperator` to trigger creation (via Kubernetes Controller and [Kubeflow Spark Operator](https://www.kubeflow.org/docs/components/spark-operator/overview/)) Spark Cluster: driver and executors.

### Spark Job With Iceberg

Spark job reads data from the Object Storage, performs some grouping by operation, and stores data in the Iceberg table (backed by Object Storage) and catalog implementation (Hadoop, Glue, or Hive).

* [Docker Compose](dags/spark_iceberg_dag.py) - uses remote Spark cluster (part of [docker-compose-airflow.yml](docker-compose-airflow.yml))
* [k8s](dags/spark_job_iceberg_k8s_dag.py) - uses Airflow `SparkKubernetesOperator` to trigger creation (via Kubernetes Controller and [Kubeflow Spark Operator](https://www.kubeflow.org/docs/components/spark-operator/overview/)) Spark Cluster: driver and executors.

### DBT and Spark (dbt[spark] provider)

Same as the previous one, but using DBT. Really overengineered use case, but it's for learning purposes - pardon me.
Here is the example again, which includes too much code due to my desire to reuse the same staff for both Docker Compose and k8s.
Inside the [dbt/profiles.yml](dbt/profiles.yml), you can see all Spark configuration relevant for k8s.
If you use Docker Compose only, a remote Spark Cluster, you can delete all staff similar to this if statement:
```python
"{{ env_var('SPARK_DRIVER_HOST') if env_var('SPARK_MASTER_URL', 'local[*]').startswith('k8s://') else 'dbt' }}"
```
Otherwise, you can just remove everything after `if` as follows:
```python
"{{ env_var('SPARK_DRIVER_HOST')"
```
* [Docker Compose](dags/dbt_dag.py) - but don't use it in such a way. Very unsecured.
* [k8s](dags/dbt_k8s_dag.py)

For more information, refer to [dbt here](dbt/README.md).

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
├── Dockerfile.fake                # dbt container image
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
- **AWS Glue Catalog** - AWS managed catalog service (not tested, but should work)
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
- **Kafka**: 4.1.0

## AI-Assisted Development

This project was developed with assistance from AI tools (Cursor.ai, IntelliJ Junie, IntelliJ Gemini plug-in, ChatGPT, Gemini, Claude).
While AI provided a good starting structure, significant manual refinement was required to create a working system.
The project serves as a realistic example of AI-assisted development, including both its potential and limitations.

## Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [Apache Iceberg Documentation](https://iceberg.apache.org/)
- [dbt Documentation](https://docs.getdbt.com/)
- [MinIO Documentation](https://min.io/docs/)
- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Strimzi Kafka Operator Documentation](https://strimzi.io/docs/operators/latest/overview)
