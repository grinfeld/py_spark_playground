import datetime
import logging

from airflow.sdk import dag, task

logger = logging.getLogger(__name__)

logger.info("Starting")

@dag(start_date=datetime.datetime(2021, 1, 1), schedule=None)
def generate_dag():
    @task()
    def extract():
        logger.info("generate dag")

    extract()

generate_dag()