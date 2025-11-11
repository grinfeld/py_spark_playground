import logging

from pyspark.sql import SparkSession
import argparse

from job_file import PythonSparkJob, ParamExtractor, ArgParserParamExtractor, EnvVarParamExtractor, BothEnvAndArgsExtractor

logger = logging.getLogger(__name__)

class IcebergSqlJob(PythonSparkJob):

    def __init__(self, ps: ParamExtractor, spark_conf: dict = None):
        super().__init__(ps, spark_conf)
        self.sql = ps.get_param("spark-sql", required=True)

    def run(self):
        spark_session = self.create_spark_session()
        self._run_inner(spark_session)

    def _run_inner(self, spark_session: SparkSession):
        super()
        current_catalog = spark_session.catalog.currentCatalog()
        logging.info(f"Current catalog: {current_catalog}")
        df = spark_session.sql(self.sql)
        df.explain(mode="formatted")
        df.show(10)
        df.limit(1).collect()
        spark_session.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark-job-name", help="Name of the job", required=False)
    parser.add_argument("--spark-master-url", help="Master", required=False)
    parser.add_argument("--spark-sql", help="sql", required=False)
    IcebergSqlJob(BothEnvAndArgsExtractor(ArgParserParamExtractor(parser), EnvVarParamExtractor())).run()


