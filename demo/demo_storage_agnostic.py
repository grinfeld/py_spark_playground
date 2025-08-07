"""
Demonstration of storage-agnostic code without if statements.

This script shows how clean and simple the code becomes when using
the storage abstraction layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
import logging
from config_manager import config_manager
from storage_utils import (
    write_dataframe_to_storage,
    read_dataframe_from_storage,
    get_storage_stats,
    create_iceberg_table,
    read_iceberg_table
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def demonstrate_storage_agnostic_code():
    """
    Demonstrate how clean the code is without storage-specific if statements.
    """
    logger.info("=== Storage-Agnostic Code Demonstration ===")
    
    # 1. Get storage information (no if statements!)
    stats = get_storage_stats()
    logger.info(f"Storage Type: {stats['type']}")
    logger.info(f"Bucket: {stats['bucket']}")
    logger.info(f"Accessible: {stats['accessible']}")
    
    # 2. Create Spark session (storage configuration handled automatically)
    spark = create_spark_session("storage_agnostic_demo")
    
    # 3. Create sample data
    data = [
        ("Alice", 25, "Engineer"),
        ("Bob", 30, "Manager"),
        ("Charlie", 35, "Engineer"),
        ("Diana", 28, "Analyst")
    ]
    df = spark.createDataFrame(data, ["name", "age", "role"])
    
    # 4. Write data (no if statements!)
    logger.info("Writing data to storage...")
    parquet_path = write_dataframe_to_storage(df, "demo_employees", "parquet")
    csv_path = write_dataframe_to_storage(df, "demo_employees", "csv")
    
    logger.info(f"Data written to: {parquet_path}")
    logger.info(f"Data written to: {csv_path}")
    
    # 5. Read data back (no if statements!)
    logger.info("Reading data from storage...")
    read_df = read_dataframe_from_storage(spark, "demo_employees", "parquet")
    read_df.show()
    
    # 6. Create Iceberg table (no if statements!)
    logger.info("Creating Iceberg table...")
    table_path = create_iceberg_table(spark, df, "demo_employees_iceberg")
    logger.info(f"Iceberg table created: {table_path}")
    
    # 7. Read from Iceberg table (no if statements!)
    logger.info("Reading from Iceberg table...")
    iceberg_df = read_iceberg_table(spark, "demo_employees_iceberg")
    iceberg_df.show()
    
    # 8. Perform aggregations
    logger.info("Performing aggregations...")
    agg_df = df.groupBy("role").agg(
        count("*").alias("count"),
        avg("age").alias("avg_age")
    )
    
    # 9. Write aggregated data (no if statements!)
    write_dataframe_to_storage(agg_df, "demo_aggregations", "parquet")
    logger.info("Aggregated data written to storage")
    
    # 10. Clean up
    spark.stop()
    logger.info("=== Demonstration Complete ===")


def create_spark_session(app_name: str = "storage_agnostic_demo") -> SparkSession:
    """
    Create Spark session using the storage abstraction layer.
    """
    from main import create_spark_session as create_spark
    return create_spark(app_name)


if __name__ == "__main__":
    demonstrate_storage_agnostic_code()
