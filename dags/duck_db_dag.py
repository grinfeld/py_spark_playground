from datetime import datetime

import duckdb
import logging
import os
import uuid
from airflow.sdk import dag,task

logger = logging.getLogger(__name__)

endpoint=os.getenv('STORAGE_ENDPOINT')
access_key=os.getenv('STORAGE_ACCESS_KEY_ID')
secret_key=os.getenv('STORAGE_SECRET_KEY')
region=os.getenv('AWS_REGION', 'us-east-1')

uuid = str(uuid.uuid4())
@dag(
    dag_id='duck_db',
    description='DuckDB',
    #    schedule=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['duckdb', 's3', 'iceberg', 'docker-compose', 'k8s'],
    default_args={}
)
def duck_db_dag():

    def init_conn(conn: duckdb.DuckDBPyConnection):
        conn.sql("INSTALL httpfs")
        conn.sql("LOAD httpfs")
        conn.sql(f"SET s3_access_key_id='{access_key}'")
        conn.sql(f"SET s3_secret_access_key='{secret_key}'")
        conn.sql(f"SET s3_region='{region}'")
        if endpoint is not None:
            conn.sql(f"SET s3_endpoint='{endpoint[7:] if endpoint.startswith('http://') else endpoint}'")
            conn.sql("SET s3_use_ssl=false")
        path_style_access_str = os.getenv('STORAGE_PATH_STYLE_ACCESS')
        if path_style_access_str is not None and path_style_access_str.lower() in ('true', '1', 'yes', 'on'):
            conn.sql(f"SET s3_url_style='path'")
        return conn

    @task
    def csv_to_parquet() -> tuple[str, str]:

        #You have to do this or else it will try to load the entire file into
        #memory and your process might crash
        local_db = f'/tmp/{uuid}_csv.db'
        conn = init_conn(duckdb.connect(database = local_db, read_only = False))

        path = f"s3a://data/{uuid}.parquet"
        conn.read_csv("s3a://examples/customers-100000.csv").create("duck_temp")
        # conn.sql(f"CREATE OR REPLACE TABLE duck_temp AS SELECT * FROM 's3a://examples/customers-100000.csv'")
        conn.sql(f"COPY duck_temp TO '{path}' (FORMAT PARQUET);")
        conn.close()
        os.remove(local_db)
        logger.info(f"Done storing data at {path} for {local_db}")
        return local_db, path

    @task
    def make_job(routes: tuple[str,str]):
        path_to_result = f"s3a://data/in_company_{uuid}.parquet"
        local_db = f'{routes[0]}'
        conn = init_conn(duckdb.connect(database = local_db, read_only = False))
        conn.read_parquet(routes[1]).create("customers")
        conn.sql("""
                 SELECT
                     company,
                     COUNT(Index) AS number_of_employees
                 FROM
                     customers
                 GROUP BY
                     company
                 ORDER BY
                     number_of_employees DESC;
        """).to_parquet(path_to_result)
        conn.close()
        os.remove(local_db)
        logger.info(f"Done to {path_to_result}")

    paths = csv_to_parquet()
    make_job(paths)

# Create the DAG
duck_db_dag()