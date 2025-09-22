# Airflow, PySpark, Iceberg and DuckDB: build local environment with AI (almost) 

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

__UPDATE__: Shortly after finishing this Apache Iceberg released libraries to support spark 4.x, so I updated all dependencies.

Finally, despite getting few wrong suggestions from Gemini and ChatGPT about right spark configurations, 
succeeded to run airflow, to show data and to write it to minIO with and without Iceberg.

As bonus, I added simple DAG using __DuckDB__ without any spark.

For more details, you can read my blog article :) [Airflow, PySpark, Iceberg: build local environment with AI (almost)](https://mikerusoft.medium.com/airflow-pyspark-iceberg-build-local-environment-with-ai-almost-6fcf608b44e2)
<a href="https://mikerusoft.medium.com/airflow-pyspark-iceberg-build-local-environment-with-ai-almost-6fcf608b44e2" target="_new">
![Airflow, PySpark, Iceberg: build local environment with AI (almost)](https://miro.medium.com/v2/resize:fit:1400/format:webp/1*yRVmIRJ5TTJu7KKaQoc_JA.jpeg "Airflow, PySpark, Iceberg: build local environment with AI (almost)")
</a>

# Detailed description for this project

The entrypoint of this project is [docker-compose-airflow](docker-compose-airflow.yml).
Start it by ``docker compose up -d --build``


**Disclaimer:** This structure is only for learning and/or development purposes.
To use in production - adjust it. Some of the components use an insecure approach. I added support for different profiles distinguished by environment variables, but honestly, I have never tested it. The only one that works currently is using the Iceberg Catalog of type Hadoop based on MinIO ObjectStorage.
Be smart, update/change it accordingly to your requirements and needs.

## Components

### Let's start with Airflow

#### Services

Currently, I am using Airflow 3.x (started from 3.0.4 and moved 3.0.6).
Airflow 3.x contains:
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


__Notes:__
1. In Airflow 3.x, they made more changes, like they extracted the module for interacting with Airflow (like logging into UI, or interacting with REST) to be external.
   so I needed to add `apache-airflow-providers-fab` into [requirements.txt](requirements.txt) and to add into airflow-init following command:
```yaml
   command: >
     bash -c "
       airflow db migrate && \
       airflow fab-db migrate && \
       airflow users create \
                 --username admin \
                 --firstname FIRST_NAME \
                 --lastname LAST_NAME \
                 --role Admin \
                 --email admin@example.org \
                 --password admin || true
```
and environment variable: `AIRFLOW__FAB__SESSION_BACKEND: "securecookie"`
2. Now we have many airflow services, so they interact with each other with **JWT** - means we need to add the following env variables:
```yaml
   AIRFLOW__API_AUTH__JWT_SECRET: stam
   # web-server responsible for issuing JWT tokens
   AIRFLOW__API_AUTH__JWT_ISSUER: http://airflow-webserver:8080
```
3. Nice to have :) `AIRFLOW__SECRETS__SENSITIVE_FIELDS: "STORAGE_SECRET_KEY,STORAGE_ACCESS_KEY_ID"`. If you have env variables, you want Airflow to mask them when logging/displaying, use this env variable.

#### Dockerfile.airflow

I am using the official docker `apache/airflow:3.0.6-python3.11`. It means that most of the staff you'll need are already included in this container.
Since I am using LocalExecutor, I need to add Spark, so I am using a multi-stage build.
I started with `FROM apache/spark:4.0.0-python3 AS build` and after that copied spark to the main part: `COPY --from=build /opt/spark/ /opt/spark/`.
I still need a JVM for running Spark, so I installed it:
```dockerfile
RUN apt-get update && apt-get install -y \
   openjdk-17-jdk \
   wget \
   curl \
   net-tools iputils-ping \
   openssh-client \
   && rm -rf /var/lib/apt/lists/*
```

The `apache/spark:4.0.0-python3` does not contain iceberg libraries, so I have added downloading them:
```dockerfile
# Download Iceberg and Glue JARs
RUN mkdir -p /opt/spark/jars/iceberg \
   && cd /opt/spark/jars/iceberg \
   && wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-4.0_2.13/1.10.0/iceberg-spark-runtime-4.0_2.13-1.10.0.jar \
   && wget -q https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar \
   && wget -q https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.33.11/bundle-2.33.11.jar \
   && wget -q https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.1/hadoop-aws-3.4.1.jar \
   && cp *.jar /opt/spark/jars/ && chown -R airflow:root /opt/spark/jars/
```

Adding the iceberg will be repeated in all other dockerfiles, except for the last line of setting the owner, since the owner is different in all other containers.

Copying DAG's folder into the container.

And the last part (Don't do it in your production !!!!) - since I am using SSHOperator to communicate with DBT, I needed to add the use of public/private key. So I added the private key to this container.

### Spark Cluster

Spark Cluster consists of a standard structure: Master and Workers.

#### Services

* Spark Master - it is responsible for managing spark resources, managing workers' health
* Spark Worker - perform distributed work, sent to Worker by Master
* Spark Driver - The place where spark-submit occurs. It generates logical and physical plans and sends tasks to workers.
  While we don't have such a resource, in my case, it's airflow-scheduler or dbt containers. I'll explain this later, while describing the DAGs.
* Spark History Service - a very important piece to debug issues happening. To make this work, I added spark-defaults.conf to point all spark
  containers to a directory to store spark events and logs. I mounted it to the local directory, so Spark workers and master can put their files, and spark-history-server can read them.
  In production, please, use S3 or some other shared storage accessible for all Spark containers.

#### Dockerfile.spark

I am using `apache/spark:4.0.0-python3` - same as in the airflow multi-stage build.
Same downloading iceberg libraries. The only difference is the last part that
```dockerfile
   ......
   ......
   && cp *.jar /opt/spark/jars/ && chown -R spark:spark /opt/spark/jars/
```
since it uses `spark` user to run Spark process.

### DBT

#### Services

It has only one service called dbt. For the local environment, I need only one.
Because it's also used as Spark-Driver, I needed the Spark installation there.

#### Dockerfile.dbt

It contains a multi-stage build with Spark and downloading Iceberg libraries, similar to Airflow.
Only the base runtime image is `FROM python:3.11-slim-bullseye AS runtime`

Copying DBT files and installing requirements modules, such as `openssh-server`, `dbt-core`, `dbt-spark`, etc.

Since the latest dbt-spark supports only Spark 3.5.x, I needed to install pyspark 4.0.0 `RUN python -m pip install --no-cache-dir pyspark==4.0.0`.

Remember, that I use SSHOperator to interact between Airflow and DBT - so it's the reason I have added `openssh-server`, copied the public key, and updated `/etc/ssh/sshd_config` to allow almost everything. We are using this only for local dev environments.

In the last part, I added a shell script ( [start_dbt.sh](scripts/start_dbt.sh) ) for starting the SSH server and executing dbt initial scripts, and made it the entrypoint.

### MinIO (our S3 storage)

When you don't want to pay for S3 during development, thanks to MinIO, we have MinIO - an ObjectStorage compatible with S3.

#### Services

* MinIO - the ObjectStorage itself.
* minio-init - inits buckets, permissions, etc. It dies immediately after it has finished.

## DAGs

Here is the main piece of shit :).

### [Simple Empty DAG](dags/empty_dag.py)

The simple DAG that just logs text to output.

### [Spark Job Without Iceberg DAG](dags/spark_job_dag.py)

The Spark Job without using Iceberg. Just read data from S3 (MinIO) and write back parquet. Nothing special.
It uses some utility [config_manager.py](dags/utils/config_manager.py) to parse env variables and populate `spark.*` configuration
It uses `SparkSubmitOperator` to trigger `spark-submit' and to send [spark simple_spark](dags/spark/simple_spark.py) to Spark.

In my previous job, we used Kubernetes and/or EMR, so we had both `KubernetesPodOperator` for some dags and `EmrContainerOperator`.
We relied on KubernetesExecutor and had Airflow Worker k8s templates, so `KubernetesPodOperator` (or `EmrContainerOperator`) triggered creation of the worker, and this one performed spark-submit.
So we could remove spark from airflow-scheduler and install it only on Worker pod.

### [Spark with Iceberg DAG](dags/spark_iceberg_dag.py)

This one ([iceberg_spark](dags/spark/iceberg_spark.py)) is simple, too.
It uses the same utility [config_manager.py](dags/utils/config_manager.py) to parse env variables, choose the catalog implementation (hadoop, glue, hive), and populate `spark.*` configuration.
The job creates some DataFrame and writes it to an iceberg table, according to the configuration.
If the Iceberg database/table doesn't exist, the code creates it.

### [Duck DB Dag](dags/duck_db_dag.py)

This one takes a csv file from S3 (MinIO) populated by minio-init container and, with the help of DuckDB, writes it to S3 as parquet file.
The next stage: it takes a parquet file, executes some grouping by SQL, and writes the result to parquet. No iceberg here.

### [DBT DAG](dags/dbt_dag.py)

Don't do it in such way in production. Please.
In my previous workplace, as I had already mentioned, we used Kubernetes, and it is much easier to deal with all staff.
Ok, but in this installation, I used SSHOperator. The problem is that every SSH session doesn't know the environment variables received during container creation.
We have a few optional solutions:
1. To allow other ssh session to see the env variable by editing `/etc/ssh/sshd_config`in in the DBT container.
2. To send env variables directly to the SSH session

I chose the second one. Read env variables I have in Airflow and then propagate them to the SSHOperator via XCOM.

```python
   @task
   def create_multiple_variables() -> dict:
       envs = {}
       for name, value in os.environ.items():
           if name.startswith("CATALOG_") or name.startswith("STORAGE_") or name.startswith("SPARK_") or name.startswith("AWS_"):
               if value is not None:
                   envs[name] = value
       envs["SCHEMA_CUSTOMERS"] = os.getenv("SCHEMA_CUSTOMERS")
       envs["RAW_DATA_PATH"] = "s3a://examples/customers-100000.csv"
       return envs
  
   dbt_ssh_task = SSHOperator(
       task_id='ssh_dbt',
       ssh_conn_id='dbt_ssh',
       command="""
               {% set dynamic_vars = task_instance.xcom_pull(task_ids='create_multiple_variables') %}
               {% for key, value in dynamic_vars.items() %}
               export {{ key }}="{{ value }}"
               {% endfor %}
               dbt run --project-dir /dbt --profiles-dir /dbt --profile $CATALOG_TYPE --target dev
           """,
       cmd_timeout=600,
       do_xcom_push=False,
       get_pty=False
   )
```

The advantage is clear - I can send different parameters, according to what catalog type/implementation I want to use.
The disadvantage is clear, too - security.

Now, about DBT itself. I have two stages:
1. Read data from a CSV file and store it in an iceberg. During this stage, DBT stores data in a temp table, then purges it into an iceberg table.
2. Read data from the iceberg table, make the grouping by aggregation, and store it in another iceberg table.


That's it.
