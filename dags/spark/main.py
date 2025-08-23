"""
Simple job pySpark
"""
import logging

from pyspark.sql import SparkSession
import argparse

logger = logging.getLogger(__name__)

def create_spark_session(app_name: str, master: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).master(master).getOrCreate()

def main(app_name: str, master: str, output: str):
    spark_session = create_spark_session(app_name, master)

    data = [(1, "Something"), (2, "Another")]
    df = spark_session.createDataFrame(data, ["id", "name"])
    df.show()

    df.write.mode("overwrite").parquet(output)

    spark_session.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-name", help="Name of the job", required=True)
    parser.add_argument("--master", help="Master", required=True)
    parser.add_argument("--output-path", help="Output path", required=True)
    args = parser.parse_args()
    main(args.job_name, args.master, args.output_path)


