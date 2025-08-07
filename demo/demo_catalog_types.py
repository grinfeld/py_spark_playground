"""
Demonstration of different catalog types (Hive, Glue, S3).

This script shows how to configure and use different catalog types
with the storage-agnostic approach.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
import logging
from config_manager import config_manager
from storage_utils import create_iceberg_table, read_iceberg_table

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def demonstrate_catalog_types():
    """
    Demonstrate different catalog types with the same code.
    """
    catalog_types = ['hive', 'glue', 's3']
    
    for catalog_type in catalog_types:
        logger.info(f"\n=== Demonstrating {catalog_type.upper()} Catalog ===")
        
        # Set catalog type
        os.environ['CATALOG_TYPE'] = catalog_type
        
        # Create Spark session with catalog configuration
        spark = create_spark_session(f"catalog_{catalog_type}_demo")
        
        # Create sample data
        data = [
            ("Alice", 25, "Engineer"),
            ("Bob", 30, "Manager"),
            ("Charlie", 35, "Engineer"),
            ("Diana", 28, "Analyst")
        ]
        df = spark.createDataFrame(data, ["name", "age", "role"])
        
        try:
            # Create Iceberg table (works with any catalog type)
            table_name = f"employees_{catalog_type}"
            table_path = create_iceberg_table(spark, df, table_name)
            logger.info(f"✅ Created table: {table_path}")
            
            # Read from Iceberg table (works with any catalog type)
            read_df = read_iceberg_table(spark, table_name)
            logger.info(f"✅ Read from table: {table_name}")
            read_df.show()
            
            # Perform aggregations
            agg_df = df.groupBy("role").agg(
                count("*").alias("count"),
                avg("age").alias("avg_age")
            )
            
            # Create aggregated table
            agg_table_name = f"aggregations_{catalog_type}"
            agg_table_path = create_iceberg_table(spark, agg_df, agg_table_name)
            logger.info(f"✅ Created aggregated table: {agg_table_path}")
            
        except Exception as e:
            logger.error(f"❌ Error with {catalog_type} catalog: {e}")
        
        finally:
            spark.stop()
            logger.info(f"Stopped Spark session for {catalog_type} catalog")


def create_spark_session(app_name: str = "catalog_demo") -> SparkSession:
    """
    Create Spark session using the storage abstraction layer.
    """
    from main import create_spark_session as create_spark
    return create_spark(app_name)


def show_catalog_configurations():
    """
    Show the different catalog configurations.
    """
    logger.info("=== Catalog Configurations ===")
    
    catalog_configs = {
        'hive': {
            'description': 'Traditional Hive metastore',
            'requirements': 'Hive metastore service running',
            'best_for': 'Local development and testing'
        },
        'glue': {
            'description': 'AWS Glue Data Catalog',
            'requirements': 'AWS Glue service access',
            'best_for': 'Production AWS environments'
        },
        's3': {
            'description': 'Direct S3-based catalog',
            'requirements': 'S3 access only',
            'best_for': 'Simple Iceberg operations'
        }
    }
    
    for catalog_type, config in catalog_configs.items():
        logger.info(f"\n📚 {catalog_type.upper()} Catalog:")
        logger.info(f"   Description: {config['description']}")
        logger.info(f"   Requirements: {config['requirements']}")
        logger.info(f"   Best for: {config['best_for']}")


if __name__ == "__main__":
    show_catalog_configurations()
    demonstrate_catalog_types()
