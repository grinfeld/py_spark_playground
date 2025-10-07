"""
Simple job pySpark
"""
import logging

from pyspark.sql import SparkSession
import argparse

from job_file import PythonSparkJob, ParamExtractor, ArgParserParamExtractor, BothEnvAndArgsExtractor, \
    EnvVarParamExtractor

logger = logging.getLogger(__name__)

class SimpleSparkJob(PythonSparkJob):

    def __init__(self, ps: ParamExtractor, spark_conf: dict = None):
        super().__init__(ps, spark_conf)
        self.output = ps.get_param("output-path", required=True)

    def run(self):
        spark_session = self.create_spark_session()
        self._run_inner(spark_session, self.output)

    def _run_inner(self, spark_session: SparkSession, out: str):
        data = [(1, "Something"), (2, "Another")]
        df = spark_session.createDataFrame(data, ["id", "name"])
        df.show()
        df.write.mode("overwrite").parquet(out)
        spark_session.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spark-job-name", help="Name of the job", required=False)
    parser.add_argument("--spark-master-url", help="Master", required=False)
    parser.add_argument("--output-path", help="Output path", required=False)
    SimpleSparkJob(BothEnvAndArgsExtractor(ArgParserParamExtractor(parser), EnvVarParamExtractor())).run()



