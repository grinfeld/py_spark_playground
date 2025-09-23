import os
from datetime import datetime

import logging

from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.sdk import dag, task
import uuid

logger = logging.getLogger(__name__)

spark_image_versions = os.getenv("SPARK_IMAGE_VERSION", "latest")
job_id = str(uuid.uuid4())[:8]
pod_name=f"dbt_airflow_k8s_pod_{job_id}"
@dag(
    dag_id='dbt_k8s',
    description='DBT',
    #    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt[spark]', 's3', 'iceberg', 'k8s'],
    default_args={}
)
def dbt_dag():

    @task
    def create_multiple_variables() -> dict:
        envs = {}
        for name, value in os.environ.items():
            if name.startswith("CATALOG_") or name.startswith("STORAGE_") or name.startswith("SPARK_") or name.startswith("AWS_") or name.startswith("DBT_"):
                if value is not None:
                    envs[name] = value
        envs["SCHEMA_CUSTOMERS"] = os.getenv("SCHEMA_CUSTOMERS")
        envs["RAW_DATA_PATH"] = "s3a://examples/customers-100000.csv"
        envs["DBT_STANDALONE"] = "false"
        envs["SPARK_JOB_ID"] = job_id
        envs["SPARK_APP_NAME"] = f"dbt-spark-{job_id}"
        envs["SPARK_DRIVER_LABEL_JOB_ID"] = job_id
        envs["SPARK_EXECUTOR_LABEL_JOB_ID"] = job_id
        return envs

    dbt_env = create_multiple_variables()

    dbt_task = KubernetesPodOperator(
        kubernetes_conn_id="kubernetes_default",
        task_id='dbt_airflow_k8s_pod',
        name=pod_name,
        namespace='py-spark',
        image=f"py-spark-dbt:{spark_image_versions}",
        image_pull_policy='IfNotPresent',
        service_account_name="airflow-worker",
        get_logs=True,
        container_logs=True,
        is_delete_operator_pod=True,
        on_finish_action="delete_succeeded_pod",
        log_events_on_failure=True,
        env_vars=dbt_env,
        pod_template_file="/dbt/templates/dbt_template.yaml",
        labels={
            "job-id": dbt_env["SPARK_JOB_ID"],
            "app": "dbt-spark"
        }
    )

    dbt_task

# Create the DAG
dbt_dag()