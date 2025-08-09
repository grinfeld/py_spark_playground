"""
Airflow 3.0 DAG for Spark 4.0 job in Kubernetes using modern decorators
"""

import os
import sys
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.providers.cncf.kubernetes.backcompat.pod import Port


# Airflow 3.0 DAG using decorators for Kubernetes
@dag(
    dag_id='spark_job_kubernetes',
    description='Spark 4.0 job in Kubernetes using Airflow 3.0 decorators',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['spark', 'configurable', 'iceberg', 'kubernetes'],
    owner='data-engineering',
    retries=1,
    retry_delay=timedelta(minutes=5),
    email_on_failure=False,
    email_on_retry=False,
)
def spark_job_kubernetes_dag():
    """Airflow 3.0 DAG using task decorators for Kubernetes Spark jobs."""
    
    # Kubernetes Pod Operator for Spark job
    spark_kubernetes_task = KubernetesPodOperator(
        task_id='spark_job_kubernetes',
        name='spark-job-kubernetes',
        namespace='default',
        image='your-registry/py-spark:latest',  # Your Spark Docker image
        cmds=['python'],
         arguments=['-c', '''
import sys
from jobs.spark_job import run_spark_job
result = run_spark_job()
print(f"Kubernetes Spark job completed: {result}")
'''],
        env_vars={
            'STORAGE_BUCKET': '{{ var.value.STORAGE_BUCKET }}',
            'CATALOG_TYPE': '{{ var.value.CATALOG_TYPE }}',
            'CATALOG_NAME': '{{ var.value.CATALOG_NAME }}',
            'CATALOG_WAREHOUSE_NAME': '{{ var.value.CATALOG_WAREHOUSE_NAME }}',
            'STORAGE_ENDPOINT': '{{ var.value.STORAGE_ENDPOINT }}',
            'STORAGE_ACCESS_KEY_ID': '{{ var.value.STORAGE_ACCESS_KEY_ID }}',
            'STORAGE_SECRET_KEY': '{{ var.value.STORAGE_SECRET_KEY }}',
            'STORAGE_PATH_STYLE_ACCESS': '{{ var.value.STORAGE_PATH_STYLE_ACCESS }}',
            'CATALOG_IO_IMPL': '{{ var.value.CATALOG_IO_IMPL }}',
        },
        resources={
            'request_memory': '2Gi',
            'request_cpu': '1000m',
            'limit_memory': '4Gi',
            'limit_cpu': '2000m',
        },
        get_logs=True,
        is_delete_operator_pod=True,
        in_cluster=True,
        config_file='/opt/airflow/.kube/config',
    )
    
    # Execute the Kubernetes task
    spark_kubernetes_task

# Create the DAG
spark_job_kubernetes_dag()
