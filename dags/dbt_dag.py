import os
from datetime import datetime

import logging
from airflow.sdk import dag, task
from airflow.providers.ssh.operators.ssh import SSHOperator

logger = logging.getLogger(__name__)

@dag(
    dag_id='dbt',
    description='DBT',
    #    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dbt[spark]', 's3', 'iceberg', 'docker-compose'],
    default_args={}
)
def dbt_dag():

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

    create_multiple_variables() >> dbt_ssh_task

# Create the DAG
dbt_dag()