import logging
import os
from datetime import datetime

from airflow.models import DagRun
from airflow.sdk import dag, task, Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from utils.spark_utils import load_template
from utils.config_manager import config_manager

logger = logging.getLogger(__name__)

# Define default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    #    'retries': 1,
    #    'retry_delay': timedelta(minutes=15),
    'email_on_failure': False,
    'email_on_retry': False,
}

@dag(
    dag_id='custom_spark_sql_k8s_dag',
    description='Job to execute manually SQL over Spark with Iceberg',
    #    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 's3', 'iceberg', 'k8s', 'sql'],
    default_args=default_args,
    params={
        "spark_sql": Param("", type="string"),
        "warehouse_name": Param(config_manager.catalog_config.catalog_name, type="string")
    }
)
def spark_job_dag():
    @task
    def create_template(**kwargs) -> dict:
        dag_run: DagRun = kwargs["dag_run"]
        return (
           load_template(config_manager)
           .override_with("spark_sql.py", {
               "SPARK_JOB_NAME": "Iceberg Spark SQL Job",
               "SPARK_SQL": dag_run.conf["spark_sql"],
               "CATALOG_WAREHOUSE_NAME": dag_run.conf["warehouse_name"],
           })
        )

    spark_template = create_template()

    spark_submit = SparkKubernetesOperator(
        kubernetes_conn_id="kubernetes_default",
        task_id='spark_iceberg_job_k8s_submit',
        template_spec=spark_template,
        namespace="py-spark",
        log_pod_spec_on_failure=True
    )

    spark_submit

# Create the DAG
spark_job_dag()