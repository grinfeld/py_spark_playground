"""
Simple job pySpark
"""
import logging

from dataclasses import dataclass

from pyspark.sql import SparkSession
import argparse

from job_file import PythonSparkJob, ParamExtractor, ArgParserParamExtractor, EnvVarParamExtractor, \
    BothEnvAndArgsExtractor

logger = logging.getLogger(__name__)

@dataclass
class IcebergConfig:
    """Configuration for storage backend."""
    db_name: str
    table_name: str

class IcebergJob(PythonSparkJob):

    def __init__(self, ps: ParamExtractor, spark_conf: dict = None):
        super().__init__(ps, spark_conf)
        db_name = ps.get_param("db-name", required=True)
        table_name = ps.get_param("table-name", required=True)
        logger.info(f"IcebergJob db_name={db_name}, table_name={table_name}")
        self.iceberg_conf = IcebergConfig(db_name, table_name)

    def run(self):
        spark_session = self.create_spark_session()
        self._run_inner(spark_session, self.iceberg_conf)

    def _run_inner(self, spark_session: SparkSession, config: IcebergConfig):

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
    parser.add_argument("--spark-job-name", help="Name of the job", required=False)
    parser.add_argument("--spark-master-url", help="Master", required=False)
    parser.add_argument("--db-name", help="DB name", required=False)
    parser.add_argument("--table-name", help="Table name", required=False)
    IcebergJob(BothEnvAndArgsExtractor(ArgParserParamExtractor(parser), EnvVarParamExtractor())).run()


