"""
Airflow 3.0 for Spark 4.0 job using modern decorators
"""

from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Airflow 3.0 DAG using decorators
@dag(
    dag_id='spark_job',
    description='Spark 4.0 job using Airflow 3.0 decorators',
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'configurable', 'iceberg'],
    default_args=default_args,
)
def spark_job_dag():
    """Airflow 3.0 DAG using task decorators."""
    
    spark_submit = SparkSubmitOperator(
        task_id='spark_job_submit',
        application='/opt/airflow/dags/main.py',
        name='spark_job',
        deploy_mode='client',
        conf={
            'master': 'spark://spark-master:7077',
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
