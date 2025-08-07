"""
Main entry point for the py_spark application.

This module demonstrates PySpark 4.0 functionality with configurable storage
(MinIO or AWS S3) and Apache Iceberg with AWS Glue support for data lake capabilities.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, current_timestamp, avg, min, max
import logging
import os
from datetime import datetime
from config_manager import config_manager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str = "py_spark_app") -> SparkSession:
    """
    Create and configure a Spark 4.0 session with configurable storage (MinIO or AWS S3),
    Iceberg, and Glue support.
    
    Args:
        app_name: Name for the Spark application
        
    Returns:
        SparkSession: Configured Spark session
    """
    # Import the shared SparkSession creation function
    from spark_job import create_spark_session as create_shared_spark_session
    
    return create_shared_spark_session(app_name)


def sample_spark_job(spark: SparkSession):
    """
    Sample Spark 4.0 job demonstrating enhanced DataFrame operations with configurable storage,
    Iceberg, and Glue integration.
    
    Args:
        spark: SparkSession instance
    """
    # Get storage configuration
    catalog_name = os.getenv('CATALOG_NAME', 'spark_catalog')
    
    logger.info("Starting sample Spark 4.0 job with configurable storage, Iceberg, and Glue integration...")
    
    # Create sample data
    data = [
        ("Alice", 25, "Engineer", "2024-01-01"),
        ("Bob", 30, "Manager", "2024-01-02"),
        ("Charlie", 35, "Engineer", "2024-01-03"),
        ("Diana", 28, "Analyst", "2024-01-04"),
        ("Eve", 32, "Manager", "2024-01-05"),
        ("Frank", 29, "Engineer", "2024-01-06"),
        ("Grace", 31, "Analyst", "2024-01-07"),
        ("Henry", 33, "Manager", "2024-01-08")
    ]
    
    columns = ["name", "age", "role", "hire_date"]
    
    # Create DataFrame
    df = spark.createDataFrame(data, columns)
    
    logger.info("Sample DataFrame created:")
    df.show()
    
    # Add timestamp column
    df_with_timestamp = df.withColumn("processed_at", current_timestamp())
    
    # Demonstrate Spark 4.0 features
    logger.info("Spark 4.0 - Enhanced DataFrame operations:")
    
    # Filtering engineers
    logger.info("Filtering engineers:")
    engineers = df_with_timestamp.filter(col("role") == "Engineer")
    engineers.show()
    
    # Enhanced aggregations with Spark 4.0
    logger.info("Enhanced aggregations by role:")
    role_stats = df_with_timestamp.groupBy("role").agg(
        count("*").alias("count"),
        avg("age").alias("avg_age"),
        min("age").alias("min_age"),
        max("age").alias("max_age")
    )
    role_stats.show()
    
    # Spark 4.0 - Improved window functions
    logger.info("Spark 4.0 - Window functions with ranking:")
    from pyspark.sql.window import Window
    from pyspark.sql.functions import rank, dense_rank
    
    window_spec = Window.partitionBy("role").orderBy(col("age").desc())
    df_with_rank = df_with_timestamp.withColumn("rank", rank().over(window_spec)) \
                                   .withColumn("dense_rank", dense_rank().over(window_spec))
    
    logger.info("Top employees by age within each role:")
    df_with_rank.filter(col("rank") <= 2).show()
    
    # Write data to storage
    try:
        # Get data paths
        data_paths = config_manager.get_data_paths("employees")
        
        # Write to different formats in storage
        logger.info("Writing data to configurable storage...")
        
        # Write as Parquet
        df_with_timestamp.write.mode("overwrite").parquet(data_paths["parquet"])
        logger.info(f"Data written to {data_paths['parquet']}")
        
        # Write as CSV
        df_with_timestamp.write.mode("overwrite").option("header", "true").csv(data_paths["csv"])
        logger.info(f"Data written to {data_paths['csv']}")
        
        # Write aggregated data with Spark 4.0 optimizations
        role_stats_paths = config_manager.get_data_paths("role_stats")
        role_stats.write.mode("overwrite").parquet(role_stats_paths["parquet"])
        logger.info(f"Enhanced aggregated data written to {role_stats_paths['parquet']}")
        
        # Write ranked data
        ranked_paths = config_manager.get_data_paths("ranked_employees")
        df_with_rank.write.mode("overwrite").parquet(ranked_paths["parquet"])
        logger.info(f"Ranked data written to {ranked_paths['parquet']}")
        
        # Iceberg operations
        logger.info("Demonstrating Apache Iceberg operations...")
        
        # Create Iceberg table
        df_with_timestamp.writeTo("spark_catalog.default.employees_iceberg").using("iceberg").createOrReplace()
        logger.info("Iceberg table created: spark_catalog.default.employees_iceberg")
        
        # Insert data into Iceberg table
        df_with_timestamp.writeTo("spark_catalog.default.employees_iceberg").append()
        logger.info("Data inserted into Iceberg table")
        
        # Read from Iceberg table
        iceberg_df = spark.table("spark_catalog.default.employees_iceberg")
        logger.info("Data read from Iceberg table:")
        iceberg_df.show()
        
        # Demonstrate Iceberg schema evolution
        logger.info("Demonstrating Iceberg schema evolution...")
        
        # Add new column to existing data
        df_with_salary = df_with_timestamp.withColumn("salary", col("age") * 1000)
        
        # Write to Iceberg table (schema evolution)
        df_with_salary.writeTo("spark_catalog.default.employees_iceberg").overwrite()
        logger.info("Schema evolution completed - salary column added")
        
        # Read evolved schema
        evolved_df = spark.table("spark_catalog.default.employees_iceberg")
        logger.info("Data with evolved schema:")
        evolved_df.show()
        
        # Catalog operations (Hive, Glue, or S3)
        logger.info("Demonstrating catalog operations...")
        
        try:
            # Create table in catalog
            catalog_table_path = f"{catalog_name}.default.employees_catalog"
            df_with_timestamp.writeTo(catalog_table_path).using("iceberg").createOrReplace()
            logger.info(f"Catalog table created: {catalog_table_path}")
            
            # Read from catalog
            catalog_df = spark.table(catalog_table_path)
            logger.info("Data read from catalog:")
            catalog_df.show()
            
        except Exception as e:
            logger.warning(f"Catalog operations not available: {e}")
            logger.info("Continuing with local Iceberg operations...")
        
        # Read back from storage to verify
        logger.info("Reading data back from configurable storage...")
        read_df = spark.read.parquet(data_paths["parquet"])
        logger.info("Data read from configurable storage:")
        read_df.show()
        
    except Exception as e:
        logger.error(f"Error writing to configurable storage/Iceberg: {e}")
        # Fallback to local storage
        logger.info("Falling back to local storage...")
        df_with_timestamp.write.mode("overwrite").parquet("/app/output/employees.parquet")
        role_stats.write.mode("overwrite").parquet("/app/output/role_stats.parquet")
        df_with_rank.write.mode("overwrite").parquet("/app/output/ranked_employees.parquet")
    
    logger.info("Sample Spark job completed successfully!")





def main():
    """Main function to run the Spark application."""
    try:
        # Import our shared Spark job module
        from spark_job import run_spark_job
        
        # Set up logging
        import logging
        logging.basicConfig(level=logging.INFO)
        
        # Run the shared Spark job
        result = run_spark_job()
        
        logger.info(f"✅ Main application completed with result: {result}")
        return result
        
    except Exception as e:
        logger.error(f"❌ Main application failed: {e}")
        raise e


if __name__ == "__main__":
    main()
