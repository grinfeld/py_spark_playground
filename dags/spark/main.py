"""
Standalone entry point for running the Spark job outside Airflow.
"""

from sparks.spark_job import run_spark_job


def main():
    result = run_spark_job()
    print(f"Job completed with result: {result}")


if __name__ == "__main__":
    main()


