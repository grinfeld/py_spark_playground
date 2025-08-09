"""
Airflow 3.0 DAG for Spark 4.0 job using modern decorators
"""

import os
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task


# Import our shared Spark job module
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Airflow 3.0 DAG using decorators
@dag(
    dag_id='spark_job',
    description='Spark 4.0 job using Airflow 3.0 decorators',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'configurable', 'iceberg'],
    owner='data-engineering',
    retries=1,
    retry_delay=timedelta(minutes=5),
    email_on_failure=False,
    email_on_retry=False,
)
def spark_job_dag():
    """Airflow 3.0 DAG using task decorators."""
    
    spark_submit = SparkSubmitOperator(
        task_id='spark_job_submit',
        application='/opt/airflow/dags/standalone/main.py',
        name='spark_job',
        master='spark://spark-master:7077',
        deploy_mode='client',
        conf={
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        },
        env_vars={
            'STORAGE_BUCKET': 'spark-data',
            'CATALOG_TYPE': 'hadoop',
            'CATALOG_WAREHOUSE_NAME': 'iceberg-warehouse',
        },
    )

    spark_submit

# Create the DAG
spark_job_dag()
