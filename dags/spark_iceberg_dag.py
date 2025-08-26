"""
Airflow 3.0 for Spark job using modern decorators
"""
import logging
import os
from datetime import datetime
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from utils.config_manager import config_manager

logger = logging.getLogger(__name__)

spark_master=os.getenv("SPARK_MASTER_URL")

# Define default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
#    'retries': 1,
#    'retry_delay': timedelta(minutes=15),
    'email_on_failure': False,
    'email_on_retry': False,
}

configs = config_manager.get_storage_config()
configs.update(config_manager.get_spark_configs())
configs.update(config_manager.get_catalog_configs())

# configs.update(config_manager.get_catalog_configs()) # if we need to use catalog

# Airflow 3.0 DAG using decorators
@dag(
    dag_id='spark_iceberg_job',
    description='Spark job using Airflow 3',
#    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 's3', 'iceberg'],
    default_args=default_args
)
def spark_job_dag():
    """Airflow 3.0 DAG using task decorators."""

    logging.info(os.getcwd())
    main_file = f"{os.getcwd()}/dags/spark/iceberg_spark.py"
    logging.info(f"Got catalog config: {config_manager.get_catalog_configs()}")
    logging.info(f"Got catalog name: {config_manager.catalog_config.catalog_name}")
    spark_submit = SparkSubmitOperator(
        task_id='spark_job_submit',
        application=main_file,
        conn_id='spark_default',
        name='spark_job',
        deploy_mode='client',
        packages="org.apache.hadoop:hadoop-aws:3.3.4"
                ",software.amazon.awssdk:bundle:2.32.29"
                ",com.amazonaws:aws-java-sdk-bundle:1.12.262"
                ",org.apache.iceberg:iceberg-aws-bundle:1.4.2"
                ",org.apache.iceberg:iceberg-hive-runtime:1.4.2"
                ",org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2"
        ,
        conf=configs,
        env_vars={
            "AWS_ACCESS_KEY_ID": config_manager.storage_config.access_key,
            "AWS_SECRET_ACCESS_KEY": config_manager.storage_config.secret_key
        },
        application_args=[
            "--job-name", "Simple Show Job",
            "--master", spark_master,
            "--db-name", f"{config_manager.catalog_config.catalog_name}",
            "--table-name", "mytable"
        ]
    )

    spark_submit

# Create the DAG
spark_job_dag()
