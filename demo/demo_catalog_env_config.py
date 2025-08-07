#!/usr/bin/env python3
"""
Demo: Environment-Driven Catalog Configuration

This demonstrates how warehouse paths and IO implementations can now be
configured via environment variables instead of hardcoded in classes.
"""

import os
import logging
from config_manager import ConfigManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def demonstrate_environment_driven_config():
    """
    Demonstrate how catalog configuration is now environment-driven.
    """
    logger.info("=== Environment-Driven Catalog Configuration ===")
    
    # Test 1: Default configuration (no environment variables)
    logger.info("\n🔧 Test 1: Default Configuration")
    os.environ.update({
        'STORAGE_TYPE': 'minio',
        'STORAGE_ENDPOINT': 'http://localhost:9000',
        'STORAGE_ACCESS_KEY_ID': 'minioadmin',
        'STORAGE_SECRET_KEY': 'minioadmin',
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue'
    })
    
    manager = StorageManager()
    catalog_configs = manager.get_catalog_configs("spark_catalog")
    
    logger.info("📋 Default Glue Catalog Configs:")
    for key, value in catalog_configs.items():
        if 'warehouse' in key or 'io-impl' in key:
            logger.info(f"   ✅ {key}: {value}")
    
    # Test 2: Custom warehouse path
    logger.info("\n🔧 Test 2: Custom Warehouse Path")
    os.environ.update({
        'GLUE_WAREHOUSE_PATH': 's3a://spark-data/custom-glue-warehouse'
    })
    
    manager = StorageManager()
    catalog_configs = manager.get_catalog_configs("spark_catalog")
    
    logger.info("📋 Custom Warehouse Path Configs:")
    for key, value in catalog_configs.items():
        if 'warehouse' in key or 'io-impl' in key:
            logger.info(f"   ✅ {key}: {value}")
    
    # Test 3: Custom IO implementation
    logger.info("\n🔧 Test 3: Custom IO Implementation")
    os.environ.update({
        'GLUE_IO_IMPL': 'org.apache.iceberg.aws.s3.CustomS3FileIO'
    })
    
    manager = StorageManager()
    catalog_configs = manager.get_catalog_configs("spark_catalog")
    
    logger.info("📋 Custom IO Implementation Configs:")
    for key, value in catalog_configs.items():
        if 'warehouse' in key or 'io-impl' in key:
            logger.info(f"   ✅ {key}: {value}")
    
    # Test 4: Global overrides
    logger.info("\n🔧 Test 4: Global Overrides")
    os.environ.update({
        'CATALOG_WAREHOUSE_PATH': 's3a://spark-data/global-warehouse',
        'CATALOG_IO_IMPL': 'org.apache.iceberg.aws.s3.GlobalS3FileIO'
    })
    
    manager = StorageManager()
    catalog_configs = manager.get_catalog_configs("spark_catalog")
    
    logger.info("📋 Global Override Configs:")
    for key, value in catalog_configs.items():
        if 'warehouse' in key or 'io-impl' in key:
            logger.info(f"   ✅ {key}: {value}")


def demonstrate_catalog_types():
    """
    Demonstrate environment-driven configuration for different catalog types.
    """
    logger.info("\n=== Catalog Type Demonstrations ===")
    
    catalog_types = ['hive', 'glue', 's3']
    
    for catalog_type in catalog_types:
        logger.info(f"\n🔧 Testing {catalog_type.upper()} Catalog:")
        
        # Set catalog type
        os.environ['CATALOG_TYPE'] = catalog_type
        
        # Set custom warehouse path for this catalog type
        warehouse_env = f'{catalog_type.upper()}_WAREHOUSE_PATH'
        os.environ[warehouse_env] = f's3a://spark-data/custom-{catalog_type}-warehouse'
        
        # Set custom IO implementation for this catalog type
        io_impl_env = f'{catalog_type.upper()}_IO_IMPL'
        os.environ[io_impl_env] = f'org.apache.iceberg.aws.s3.Custom{catalog_type.title()}S3FileIO'
        
        manager = ConfigManager()
        catalog_configs = manager.get_catalog_configs("spark_catalog")
        
        logger.info(f"📋 {catalog_type.upper()} Catalog Configs:")
        for key, value in catalog_configs.items():
            if 'warehouse' in key or 'io-impl' in key:
                logger.info(f"   ✅ {key}: {value}")


def demonstrate_benefits():
    """
    Show the benefits of environment-driven catalog configuration.
    """
    logger.info("\n=== Benefits of Environment-Driven Configuration ===")
    
    logger.info("✅ **Flexibility**")
    logger.info("   - Warehouse paths configurable without code changes")
    logger.info("   - IO implementations can be customized per environment")
    logger.info("   - No need to modify classes for different deployments")
    
    logger.info("\n✅ **Environment-Specific Configuration**")
    logger.info("   - Development: Use local paths")
    logger.info("   - Staging: Use staging bucket paths")
    logger.info("   - Production: Use production bucket paths")
    
    logger.info("\n✅ **Reduced Code Complexity**")
    logger.info("   - No hardcoded paths in classes")
    logger.info("   - Configuration separated from implementation")
    logger.info("   - Easier to test with different configurations")
    
    logger.info("\n✅ **Deployment Flexibility**")
    logger.info("   - Same code works in different environments")
    logger.info("   - Configuration via environment variables")
    logger.info("   - No code changes needed for new paths")


def show_environment_variables():
    """
    Show all available environment variables for catalog configuration.
    """
    logger.info("\n=== Available Environment Variables ===")
    
    logger.info("📋 Global Catalog Variables:")
    logger.info("   - CATALOG_WAREHOUSE_PATH: Global warehouse path override")
    logger.info("   - CATALOG_IO_IMPL: Global IO implementation override")
    
    logger.info("\n📋 Catalog-Specific Variables:")
    logger.info("   - HIVE_WAREHOUSE_PATH: Hive catalog warehouse path")
    logger.info("   - GLUE_WAREHOUSE_PATH: Glue catalog warehouse path")
    logger.info("   - S3_CATALOG_IO_IMPL: S3 catalog IO implementation")
    logger.info("   - GLUE_IO_IMPL: Glue catalog IO implementation")
    
    logger.info("\n📋 Precedence Order:")
    logger.info("   1. Catalog-specific variables (e.g., GLUE_WAREHOUSE_PATH)")
    logger.info("   2. Global variables (e.g., CATALOG_WAREHOUSE_PATH)")
    logger.info("   3. Default values in classes")


if __name__ == "__main__":
    demonstrate_environment_driven_config()
    demonstrate_catalog_types()
    demonstrate_benefits()
    show_environment_variables()
