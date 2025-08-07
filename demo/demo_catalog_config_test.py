"""
Test script to verify the correct catalog configuration patterns.

This script tests different catalog configurations to ensure
we're using the correct patterns for each catalog type.
"""

import os
import logging
from config_manager import ConfigManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def test_catalog_configurations():
    """
    Test different catalog configurations to verify correctness.
    """
    logger.info("=== Catalog Configuration Test ===")
    
    # Test scenarios
    scenarios = [
        {
            'name': 'Hive Catalog',
            'env': {
                'STORAGE_TYPE': 'minio',
                'STORAGE_ENDPOINT': 'http://localhost:9000',
                'STORAGE_ACCESS_KEY_ID': 'minioadmin',
                'STORAGE_SECRET_KEY': 'minioadmin',
                'MINIO_BUCKET': 'spark-data',
                'CATALOG_TYPE': 'hive'
            }
        },
        {
            'name': 'Glue Catalog',
            'env': {
                'STORAGE_TYPE': 'minio',
                'STORAGE_ENDPOINT': 'http://localhost:9000',
                'STORAGE_ACCESS_KEY_ID': 'minioadmin',
                'STORAGE_SECRET_KEY': 'minioadmin',
                'MINIO_BUCKET': 'spark-data',
                'CATALOG_TYPE': 'glue'
            }
        },
        {
            'name': 'S3 Catalog',
            'env': {
                'STORAGE_TYPE': 'minio',
                'STORAGE_ENDPOINT': 'http://localhost:9000',
                'STORAGE_ACCESS_KEY_ID': 'minioadmin',
                'STORAGE_SECRET_KEY': 'minioadmin',
                'MINIO_BUCKET': 'spark-data',
                'CATALOG_TYPE': 's3'
            }
        }
    ]
    
    for scenario in scenarios:
        logger.info(f"\n🔄 Testing: {scenario['name']}")
        
        # Set environment variables
        for key, value in scenario['env'].items():
            os.environ[key] = value
        
        # Create storage manager
        manager = ConfigManager()
        
        # Get catalog configurations
        catalog_configs = manager.get_catalog_configs("test_catalog")
        
        # Show configurations
        show_catalog_configs(catalog_configs, scenario['name'])
        
        # Clean up environment
        for key in scenario['env'].keys():
            if key in os.environ:
                del os.environ[key]


def show_catalog_configs(catalog_configs, scenario_name):
    """
    Show the catalog configurations and analyze them.
    """
    logger.info(f"📋 Catalog Configurations for {scenario_name}:")
    
    # Check for catalog class configuration
    catalog_class_keys = [k for k in catalog_configs.keys() if k.endswith('.catalog') and not k.endswith('.catalog-impl')]
    catalog_type_keys = [k for k in catalog_configs.keys() if k.endswith('.type')]
    
    logger.info(f"🔧 Catalog Class Keys: {catalog_class_keys}")
    logger.info(f"🔧 Catalog Type Keys: {catalog_type_keys}")
    
    # Show all configurations
    for key, value in catalog_configs.items():
        logger.info(f"   {key}: {value}")
    
    # Analyze the pattern
    logger.info(f"\n📊 Analysis for {scenario_name}:")
    
    if catalog_class_keys and catalog_type_keys:
        logger.info("⚠️  WARNING: Both catalog class and type are set!")
        logger.info("   This might cause conflicts. Check Apache Iceberg documentation.")
    elif catalog_class_keys:
        logger.info("✅ Direct catalog class configuration (e.g., Glue)")
    elif catalog_type_keys:
        logger.info("✅ SparkCatalog with type configuration (e.g., Hive)")
    else:
        logger.info("❌ No catalog configuration found!")


def demonstrate_correct_patterns():
    """
    Demonstrate the correct configuration patterns based on Apache Iceberg documentation.
    """
    logger.info("\n=== Correct Configuration Patterns ===")
    
    logger.info("📚 **Apache Iceberg Catalog Patterns**")
    
    logger.info("\n🔧 **Hive Catalog Pattern**")
    logger.info("   spark.sql.catalog.{name} = org.apache.iceberg.spark.SparkCatalog")
    logger.info("   spark.sql.catalog.{name}.type = hive")
    logger.info("   spark.sql.catalog.{name}.uri = thrift://localhost:9083")
    logger.info("   spark.sql.catalog.{name}.warehouse = s3a://bucket/warehouse")
    
    logger.info("\n🔧 **Glue Catalog Pattern**")
    logger.info("   spark.sql.catalog.{name} = org.apache.iceberg.aws.glue.GlueCatalog")
    logger.info("   spark.sql.catalog.{name}.warehouse = s3a://bucket/warehouse")
    logger.info("   spark.sql.catalog.{name}.io-impl = org.apache.iceberg.aws.s3.S3FileIO")
    
    logger.info("\n🔧 **S3 Catalog Pattern**")
    logger.info("   spark.sql.catalog.{name} = org.apache.iceberg.spark.SparkCatalog")
    logger.info("   spark.sql.catalog.{name}.type = hadoop")
    logger.info("   spark.sql.catalog.{name}.warehouse = s3a://bucket/warehouse")
    logger.info("   spark.sql.catalog.{name}.io-impl = org.apache.iceberg.aws.s3.S3FileIO")
    
    logger.info("\n✅ **Key Insight**")
    logger.info("   - Hive and S3 catalogs use SparkCatalog + type")
    logger.info("   - Glue catalog uses direct GlueCatalog class")
    logger.info("   - These are mutually exclusive patterns")


def test_current_implementation():
    """
    Test our current implementation against the correct patterns.
    """
    logger.info("\n=== Current Implementation Test ===")
    
    # Test our current configurations
    test_configs = [
        {
            'name': 'HiveCatalog (Current)',
            'configs': {
                'spark.sql.catalog.test': 'org.apache.iceberg.spark.SparkCatalog',
                'spark.sql.catalog.test.type': 'hive',
                'spark.sql.catalog.test.uri': 'thrift://localhost:9083',
                'spark.sql.catalog.test.warehouse': 's3a://bucket/warehouse'
            }
        },
        {
            'name': 'GlueCatalog (Current)',
            'configs': {
                'spark.sql.catalog.test': 'org.apache.iceberg.aws.glue.GlueCatalog',
                'spark.sql.catalog.test.warehouse': 's3a://bucket/warehouse',
                'spark.sql.catalog.test.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO'
            }
        },
        {
            'name': 'S3Catalog (Current)',
            'configs': {
                'spark.sql.catalog.test': 'org.apache.iceberg.spark.SparkCatalog',
                'spark.sql.catalog.test.type': 'hadoop',
                'spark.sql.catalog.test.warehouse': 's3a://bucket/warehouse',
                'spark.sql.catalog.test.io-impl': 'org.apache.iceberg.aws.s3.S3FileIO'
            }
        }
    ]
    
    for test in test_configs:
        logger.info(f"\n🔄 {test['name']}")
        
        configs = test['configs']
        has_catalog_class = any(k.endswith('.catalog') and not k.endswith('.catalog-impl') for k in configs.keys())
        has_type = any(k.endswith('.type') for k in configs.keys())
        
        logger.info(f"   Has catalog class: {'✅ Yes' if has_catalog_class else '❌ No'}")
        logger.info(f"   Has type: {'✅ Yes' if has_type else '❌ No'}")
        
        if has_catalog_class and has_type:
            logger.info("   ⚠️  WARNING: Both catalog class and type set!")
        elif has_catalog_class:
            logger.info("   ✅ Direct catalog class (Glue pattern)")
        elif has_type:
            logger.info("   ✅ SparkCatalog with type (Hive/S3 pattern)")
        else:
            logger.info("   ❌ No catalog configuration!")


if __name__ == "__main__":
    test_catalog_configurations()
    demonstrate_correct_patterns()
    test_current_implementation()
