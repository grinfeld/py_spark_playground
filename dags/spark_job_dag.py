"""
Airflow 3.0 DAG for Spark 4.0 job using modern decorators
"""

import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

# Add the project root to Python path (works in Docker and local)
from lib.utils import add_project_root_to_path
add_project_root_to_path()

# Import our shared Spark job module
from jobs.spark_job import run_spark_job

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
    
    @task
    def run_spark_job_task():
        """Run the Spark job using the shared module."""
        try:
            from loguru import logger
            
            # Set up logging for Airflow
            logger.add("/opt/airflow/logs/spark_job.log", rotation="1 day")
            
            # Run the shared Spark job
            result = run_spark_job()
            
            logger.info(f"✅ Airflow DAG completed with result: {result}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Airflow DAG failed: {e}")
            raise e
    
    # Execute the task
    run_spark_job_task()

# Create the DAG
spark_job_dag()
