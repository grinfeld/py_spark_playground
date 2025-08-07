"""
Shared Spark job logic for both main.py and Airflow DAG
"""

import os
import sys
from typing import Optional
from loguru import logger


def create_spark_session(app_name: str = "SparkJob", catalog_name: Optional[str] = None):
    """
    Create and configure a Spark 4.0 session with configurable storage and catalog.
    
    Args:
        app_name: Name for the Spark application
        catalog_name: Optional catalog name override. If None, uses CATALOG_NAME env var.
        
    Returns:
        SparkSession: Configured Spark session
    """
    # Import PySpark modules
    from pyspark.sql import SparkSession
    
    # Import our config manager
    from config_manager import config_manager
    
    # Get catalog name from parameter or environment
    if catalog_name is None:
        catalog_name = os.getenv('CATALOG_NAME', 'spark_catalog')
    
    logger.info("Creating Spark session with configurable storage...")
    
    # Get storage configuration
    logger.info("Configuring Spark with configurable storage")
    
    # Get warehouse paths
    warehouse_paths = config_manager.get_warehouse_paths()
    
    # Build Spark configuration
    spark_builder = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.adaptive.optimizeSkewedJoin.enabled", "true") \
        .config("spark.sql.adaptive.forceApply", "true") \
        .config("spark.sql.warehouse.dir", warehouse_paths["warehouse"]) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

    # Add storage-specific configurations
    spark_configs = config_manager.get_spark_configs()
    for key, value in spark_configs.items():
        spark_builder = spark_builder.config(key, value)

    # Add catalog configurations
    catalog_configs = config_manager.get_catalog_configs(catalog_name)
    for key, value in catalog_configs.items():
        spark_builder = spark_builder.config(key, value)

    spark = spark_builder.getOrCreate()
    
    # Set log level to reduce verbose output
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def run_spark_job(catalog_name: Optional[str] = None) -> str:
    """
    Run the Spark job with configurable storage and catalog.
    
    Args:
        catalog_name: Optional catalog name override. If None, uses CATALOG_NAME env var.
        
    Returns:
        str: "SUCCESS" if job completes successfully
    """
    try:
        # Import PySpark modules
        from pyspark.sql.functions import col, when, count, avg, sum as spark_sum
        import pandas as pd
        import numpy as np
        
        # Get catalog name from parameter or environment
        if catalog_name is None:
            catalog_name = os.getenv('CATALOG_NAME', 'spark_catalog')
        
        logger.info("Starting sample Spark 4.0 job with configurable storage, Iceberg, and Glue integration...")
        
        # Create sample data
        logger.info("Creating sample employee data...")
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
        
        # Create Spark session using shared function
        spark = create_spark_session("SparkJob", catalog_name)
        
        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(df)
        
        logger.info("Sample data created:")
        spark_df.show(5)
        
        # Perform analytics
        logger.info("Performing analytics...")
        
        # Department statistics
        dept_stats = spark_df.groupBy("department") \
            .agg(
                count("*").alias("employee_count"),
                avg("salary").alias("avg_salary"),
                avg("years_experience").alias("avg_experience"),
                avg("performance_rating").alias("avg_rating")
            ) \
            .orderBy("avg_salary", ascending=False)
        
        logger.info("Department statistics:")
        dept_stats.show()
        
        # Location analysis
        location_stats = spark_df.groupBy("location") \
            .agg(
                count("*").alias("employee_count"),
                avg("salary").alias("avg_salary")
            ) \
            .orderBy("avg_salary", ascending=False)
        
        logger.info("Location statistics:")
        location_stats.show()
        
        # High performers analysis
        high_performers = spark_df.filter(col("performance_rating") >= 4.5) \
            .groupBy("department") \
            .agg(count("*").alias("high_performer_count")) \
            .orderBy("high_performer_count", ascending=False)
        
        logger.info("High performers by department:")
        high_performers.show()
        
        # Write to different formats in storage
        logger.info("Writing data to configurable storage...")
        
        # Get data paths
        data_paths = config_manager.get_data_paths("employees")
        
        # Write as Parquet
        spark_df.write.mode("overwrite").parquet(data_paths["parquet"])
        dept_stats.write.mode("overwrite").parquet(data_paths["parquet"].replace("employees", "department_stats"))
        location_stats.write.mode("overwrite").parquet(data_paths["parquet"].replace("employees", "location_stats"))
        high_performers.write.mode("overwrite").parquet(data_paths["parquet"].replace("employees", "high_performers"))
        
        # Catalog operations (Hive, Glue, or S3)
        logger.info("Demonstrating catalog operations...")
        
        try:
            # Create Iceberg table
            table_path = f"{catalog_name}.default.employees"
            spark_df.writeTo(table_path).using("iceberg").createOrReplace()
            
            logger.info("✅ Iceberg table created successfully")
            
            # Read back from Iceberg
            iceberg_df = spark.table(table_path)
            logger.info("✅ Iceberg table read successfully")
            logger.info(f"Total employees: {iceberg_df.count()}")
            
            # Show sample data
            logger.info("Sample data from Iceberg table:")
            iceberg_df.show(5)
            
        except Exception as e:
            logger.error(f"Error writing to configurable storage/Iceberg: {e}")
            # Fallback to local storage
            logger.info("Falling back to local storage...")
            
            # Write to local filesystem as fallback
            spark_df.write.mode("overwrite").parquet("/tmp/employees_fallback.parquet")
            logger.info("✅ Data written to local fallback location")
        
        # Read back from storage to verify
        logger.info("Reading data back from configurable storage...")
        read_df = spark.read.parquet(data_paths["parquet"])
        logger.info("Data read from configurable storage:")
        read_df.show()
        
        # Clean up
        spark.stop()
        logger.info("✅ Spark session closed")
        
        return "SUCCESS"
        
    except Exception as e:
        logger.error(f"❌ Spark job failed: {e}")
        raise e


if __name__ == "__main__":
    # Set up logging for standalone execution
    logger.add("logs/spark_job.log", rotation="1 day")
    
    # Run the job
    result = run_spark_job()
    print(f"Job completed with result: {result}")
