"""
Simple job pySpark
"""
import logging

from dataclasses import dataclass

from pyspark.sql import SparkSession
import argparse

logger = logging.getLogger(__name__)

@dataclass
class IcebergConfig:
    """Configuration for storage backend."""
    db_name: str
    table_name: str

def create_spark_session(app_name: str, master: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).master(master).getOrCreate()

def main(app_name: str, master: str, config: IcebergConfig):
    spark_session = create_spark_session(app_name, master)

    current_catalog = spark_session.catalog.currentCatalog()
    logging.info(f"Current catalog: {current_catalog}")

    data = [(1, f"Something"), (2, current_catalog)]
    df = spark_session.createDataFrame(data, ["id", "name"])
    df.show()

    cmd = df.writeTo(config.table_name).using("iceberg")

    if not spark_session.catalog.tableExists(f"{config.table_name}"):
        cmd.create()
    else:
        cmd.append()

    iceberg_df = spark_session.table(config.table_name)
    iceberg_df.show(5)

    spark_session.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-name", help="Name of the job", required=True)
    parser.add_argument("--master", help="Master", required=True)
    parser.add_argument("--db-name", help="DB name", required=True)
    parser.add_argument("--table-name", help="Table name", required=True)
    args = parser.parse_args()
    config = IcebergConfig(args.db_name, args.table_name)
    main(args.job_name, args.master, config)


