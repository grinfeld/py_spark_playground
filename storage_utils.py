"""
Storage utility functions demonstrating the clean storage-agnostic approach.

This module shows how to work with storage without any if statements.
"""

import os
from typing import Dict, List
from pyspark.sql import SparkSession, DataFrame
from config_manager import config_manager


def write_dataframe_to_storage(df: DataFrame, filename: str, format: str = "parquet") -> str:
    """
    Write DataFrame to storage in a storage-agnostic way.
    
    Args:
        df: DataFrame to write
        filename: Base filename (without extension)
        format: Output format (parquet, csv, etc.)
        
    Returns:
        str: Path where data was written
    """
    data_paths = config_manager.get_data_paths(filename)
    output_path = data_paths.get(format, data_paths["parquet"])
    
    if format == "csv":
        df.write.mode("overwrite").option("header", "true").csv(output_path)
    else:
        df.write.mode("overwrite").parquet(output_path)
    
    return output_path


def read_dataframe_from_storage(spark: SparkSession, filename: str, format: str = "parquet") -> DataFrame:
    """
    Read DataFrame from storage in a storage-agnostic way.
    
    Args:
        spark: SparkSession instance
        filename: Base filename (without extension)
        format: Input format (parquet, csv, etc.)
        
    Returns:
        DataFrame: Loaded DataFrame
    """
    data_paths = config_manager.get_data_paths(filename)
    input_path = data_paths.get(format, data_paths["parquet"])
    
    if format == "csv":
        return spark.read.option("header", "true").csv(input_path)
    else:
        return spark.read.parquet(input_path)


def list_storage_files(spark: SparkSession, prefix: str = "") -> List[str]:
    """
    List files in storage in a storage-agnostic way.
    
    Args:
        spark: SparkSession instance
        prefix: File prefix to filter by
        
    Returns:
        List[str]: List of file paths
    """
    # Get bucket from config directly
    bucket = config_manager.config.bucket
    base_path = f"s3a://{bucket}"
    
    if prefix:
        base_path = f"{base_path}/{prefix}"
    
    try:
        # This is a simplified example - in practice you'd use Spark's file listing
        return [f"{base_path}/employees.parquet", f"{base_path}/role_stats.parquet"]
    except Exception:
        return []


def get_storage_stats() -> Dict[str, any]:
    """
    Get storage statistics in a storage-agnostic way.
    
    Returns:
        Dict: Storage statistics
    """
    config = config_manager.config
    
    return {
        "bucket": config.bucket,
        "region": config.region,
        "endpoint": config.endpoint,
        "catalog_type": config.catalog_type,
        "has_credentials": bool(config.access_key and config.secret_key)
    }


def create_iceberg_table(spark: SparkSession, df: DataFrame, table_name: str, catalog: str = None) -> str:
    """
    Create Iceberg table in a storage-agnostic way.
    
    Args:
        spark: SparkSession instance
        df: DataFrame to write
        table_name: Name of the table
        catalog: Catalog to use (if None, uses configured catalog)
        
    Returns:
        str: Full table path
    """
    if catalog is None:
        catalog = os.getenv('CATALOG_NAME', 'spark_catalog')
    
    table_path = f"{catalog}.default.{table_name}"
    df.writeTo(table_path).using("iceberg").createOrReplace()
    return table_path


def read_iceberg_table(spark: SparkSession, table_name: str, catalog: str = None) -> DataFrame:
    """
    Read from Iceberg table in a storage-agnostic way.
    
    Args:
        spark: SparkSession instance
        table_name: Name of the table
        catalog: Catalog to use (if None, uses configured catalog)
        
    Returns:
        DataFrame: Loaded DataFrame
    """
    if catalog is None:
        catalog = os.getenv('CATALOG_NAME', 'spark_catalog')
    
    table_path = f"{catalog}.default.{table_name}"
    return spark.table(table_path)
