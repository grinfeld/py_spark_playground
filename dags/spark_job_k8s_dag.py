"""
Airflow 3.0 for Spark job using modern decorators
"""
import logging
from datetime import datetime

from airflow.sdk import dag, task
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from utils.spark_utils import load_template, ConfigTypes
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
    dag_id='spark_job_k8s',
    description='Spark job using Airflow 3 with k8s',
#    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 's3', 'k8s'],
    default_args=default_args
)
def spark_job_dag():
    @task
    def create_template() -> dict:
        return (
            load_template(config_manager, {ConfigTypes.storage(), ConfigTypes.basic()})
            .override_with("simple_spark.py",
               {
                    "SPARK_JOB_NAME": "Simple Spark Job",
                    "OUTPUT_PATH": f"s3a://{config_manager.storage_config.bucket}/output"
                }
            )
        )

    spark_template = create_template()

    spark_submit = SparkKubernetesOperator(
        kubernetes_conn_id="kubernetes_default",
        task_id='spark_job_k8s_submit',
        template_spec=spark_template,
        namespace="py-spark",
        log_pod_spec_on_failure=True
    )

    spark_submit

# Create the DAG
spark_job_dag()
