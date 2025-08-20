"""
Shared Spark job logic for both standalone and Airflow
"""

import os
from typing import Optional
from loguru import logger

from .utils.config_manager import config_manager


def create_spark_session(app_name: str = "SparkJob", catalog_name: Optional[str] = None):
    """
    Create and configure a Spark 4.0 session with configurable storage and catalog.
    """
    from pyspark.sql import SparkSession

    if catalog_name is None:
        catalog_name = os.getenv('CATALOG_NAME', 'spark_catalog')

    logger.info("Creating Spark session with configurable storage...")

    warehouse_paths = config_manager.get_warehouse_paths()

    spark_builder = (
        SparkSession.builder.appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL"))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
        .config("spark.sql.adaptive.optimizeSkewedJoin.enabled", "true")
        .config("spark.sql.adaptive.forceApply", "true")
        .config("spark.sql.warehouse.dir", warehouse_paths["warehouse"])
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    )

    # If SPARK_MASTER_URL is provided, connect to the standalone cluster
    master_url = os.getenv("SPARK_MASTER_URL")
    if master_url:
        spark_builder = spark_builder.master(master_url)

    for key, value in config_manager.get_spark_configs().items():
        spark_builder = spark_builder.config(key, value)

    for key, value in config_manager.get_catalog_configs(catalog_name).items():
        spark_builder = spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_spark_job(catalog_name: Optional[str] = None) -> str:
    """Run the Spark job with configurable storage and catalog."""
    try:
        from pyspark.sql.functions import col, when, count, avg
        import pandas as pd
        import numpy as np

        if catalog_name is None:
            catalog_name = os.getenv('CATALOG_NAME', 'spark_catalog')

        logger.info("Starting sample Spark 4.0 job with configurable storage, Iceberg, and Glue integration...")

        np.random.seed(42)
        n_employees = 1000
        data = {
            'employee_id': range(1, n_employees + 1),
            'name': [f'Employee_{i}' for i in range(1, n_employees + 1)],
            'department': np.random.choice(['Engineering', 'Sales', 'Marketing', 'HR', 'Finance'], n_employees),
            'salary': np.random.normal(75000, 15000, n_employees).astype(int),
            'years_experience': np.random.randint(1, 20, n_employees),
            'location': np.random.choice(['NYC', 'SF', 'LA', 'Chicago', 'Austin'], n_employees),
            'performance_rating': np.random.uniform(3.0, 5.0, n_employees).round(1)
        }

        df = pd.DataFrame(data)
        spark = create_spark_session("SparkJob", catalog_name)
        spark_df = spark.createDataFrame(df)
        spark_df.show(5)

        dept_stats = (
            spark_df.groupBy("department")
            .agg(count("*").alias("employee_count"), avg("salary").alias("avg_salary"), avg("years_experience").alias("avg_experience"), avg("performance_rating").alias("avg_rating"))
            .orderBy("avg_salary", ascending=False)
        )
        dept_stats.show()

        # Write to storage
        data_paths = config_manager.get_data_paths("employees")
        spark_df.write.mode("overwrite").parquet(data_paths["parquet"])

        try:
            table_path = f"{catalog_name}.default.employees"
            spark_df.writeTo(table_path).using("iceberg").createOrReplace()
            iceberg_df = spark.table(table_path)
            iceberg_df.show(5)
        except Exception:
            logger.warning("Iceberg operations failed, continuing...")

        spark.stop()
        return "SUCCESS"
    except Exception as e:
        logger.error(f"Spark job failed: {e}")
        raise e


