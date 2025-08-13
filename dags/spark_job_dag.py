"""
Airflow 3.0 for Spark 4.0 job using modern decorators
"""
import logging
import os
from datetime import datetime, timedelta
from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

logger = logging.getLogger(__name__)

endpoint=os.getenv('STORAGE_ENDPOINT')
access_key=os.getenv('STORAGE_ACCESS_KEY_ID')
secret_key=os.getenv('STORAGE_SECRET_KEY')
bucket=os.getenv('STORAGE_BUCKET')
region=os.getenv('AWS_REGION', 'us-east-1')
path_style_access=os.getenv('STORAGE_PATH_STYLE_ACCESS')
credentials_provider=os.getenv('STORAGE_CREDENTIALS_PROVIDER')
catalog_name=os.getenv("CATALOG_NAME")
catalog_io_impl=os.getenv("CATALOG_IO_IMPL")
catalog_type=os.getenv("CATALOG_TYPE")
warehouse_name=os.getenv("CATALOG_WAREHOUSE_NAME")
spark_master=os.getenv("SPARK_MASTER_URL")

# 'CATALOG_WAREHOUSE_NAME': 'iceberg-warehouse',
# 'STORAGE_BUCKET': 'spark-data',
# 'CATALOG_TYPE': 'hadoop',
# 'CATALOG_NAME': 'spark_catalog',
# 'STORAGE_PATH_STYLE_ACCESS': 'true',

# Define default arguments for the DAG
default_args = {
    'owner': 'data-engineering',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

def find_py_files_in_dir(directory):
    all_entries = os.listdir(directory)
    py_files = [entry for entry in all_entries if os.path.isfile(os.path.join(directory, entry)) and entry.endswith(".py")]
    return py_files

# Airflow 3.0 DAG using decorators
@dag(
    dag_id='spark_job',
    description='Spark 4.0 job using Airflow 3.0 decorators',
    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'configurable', 'iceberg'],
    default_args=default_args
)
def spark_job_dag():
    """Airflow 3.0 DAG using task decorators."""
    app_base_path = f"{os.getcwd()}/dags/spark"
    main_app_file = f"{app_base_path}/main.py"
    spark_py_files = find_py_files_in_dir(app_base_path) + find_py_files_in_dir(f"{app_base_path}/utils")
    for fname in spark_py_files:
        logger.info(f"{fname}")

    spark_submit = SparkSubmitOperator(
        task_id='spark_job_submit',
        application=main_app_file,
        py_files=spark_py_files,
        name='spark_job',
        deploy_mode='client',
        conf={
            'master': spark_master,
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.extensions': 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions',
        },
        env_vars={
            'STORAGE_ENDPOINT': f"{endpoint}",
            'STORAGE_ACCESS_KEY_ID':f"{access_key}",
            'STORAGE_SECRET_KEY': f"{secret_key}",
            'STORAGE_BUCKET': f"{bucket}",
            'AWS_REGION': f"{region}",
            'STORAGE_PATH_STYLE_ACCESS': f"{path_style_access}",
            'STORAGE_CREDENTIALS_PROVIDER': f"{credentials_provider}",
            'CATALOG_NAME': f"{catalog_name}",
            'CATALOG_IO_IMPL': f"{catalog_io_impl}",
            'CATALOG_TYPE': f"{catalog_type}",
            'CATALOG_WAREHOUSE_NAME': f"{warehouse_name}"
        },
    )

    spark_submit

# Create the DAG
spark_job_dag()
