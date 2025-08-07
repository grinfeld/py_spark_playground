"""
Demonstration of unified configuration approach.

This script shows how all storage configurations are now handled
in the parent class based on environment variable existence.
"""

import os
import logging
from config_manager import ConfigManager, StorageBackend

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def demonstrate_unified_configuration():
    """
    Demonstrate the unified configuration approach.
    """
    logger.info("=== Unified Configuration Demo ===")
    
    # Test scenarios
    scenarios = [
        {
            'name': 'MinIO with All Variables',
            'env': {
                'STORAGE_TYPE': 'minio',
                'STORAGE_ENDPOINT': 'http://localhost:9000',
                'STORAGE_ACCESS_KEY_ID': 'minioadmin',
                'STORAGE_SECRET_KEY': 'minioadmin',
                'STORAGE_PATH_STYLE_ACCESS': 'true',
                'STORAGE_CREDENTIALS_PROVIDER': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
                'MINIO_BUCKET': 'spark-data'
            }
        },
        {
            'name': 'MinIO without Credentials Provider',
            'env': {
                'STORAGE_TYPE': 'minio',
                'STORAGE_ENDPOINT': 'http://localhost:9000',
                'STORAGE_ACCESS_KEY_ID': 'minioadmin',
                'STORAGE_SECRET_KEY': 'minioadmin',
                'STORAGE_PATH_STYLE_ACCESS': 'true',
                'MINIO_BUCKET': 'spark-data'
            }
        },
        {
            'name': 'AWS S3 with Credentials',
            'env': {
                'STORAGE_TYPE': 's3',
                'STORAGE_ACCESS_KEY_ID': 'your-aws-key',
                'STORAGE_SECRET_KEY': 'your-aws-secret',
                'STORAGE_PATH_STYLE_ACCESS': 'false',
                'S3_BUCKET': 'my-spark-bucket'
            }
        },
        {
            'name': 'AWS S3 without Credentials (IAM)',
            'env': {
                'STORAGE_TYPE': 's3',
                'S3_BUCKET': 'my-spark-bucket'
                # No credentials - will use IAM roles
            }
        },
        {
            'name': 'S3-Compatible Service',
            'env': {
                'STORAGE_TYPE': 's3',
                'STORAGE_ENDPOINT': 'https://custom-s3-endpoint.com',
                'STORAGE_ACCESS_KEY_ID': 'custom-key',
                'STORAGE_SECRET_KEY': 'custom-secret',
                'STORAGE_PATH_STYLE_ACCESS': 'true',
                'S3_BUCKET': 'my-bucket'
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
        
        # Show configuration details
        show_unified_config_details(manager, scenario['name'])
        
        # Clean up environment
        for key in scenario['env'].keys():
            if key in os.environ:
                del os.environ[key]


def show_unified_config_details(manager, scenario_name):
    """
    Show the unified configuration details.
    """
    config = manager.config
    storage_backend = manager.storage_backend
    
    logger.info(f"📦 Storage Type: {config.storage_type}")
    logger.info(f"🔗 Endpoint: {config.endpoint or 'Not Set'}")
    logger.info(f"🔑 Access Key: {config.access_key or 'Not Set'}")
    logger.info(f"🔐 Secret Key: {'***' if config.secret_key else 'Not Set'}")
    logger.info(f"🏪 Bucket: {config.bucket}")
    logger.info(f"🌍 Region: {config.region}")
    logger.info(f"🔧 Credentials Provider: {config.credentials_provider or 'Not Set'}")
    logger.info(f"🛣️  Path Style Access: {config.path_style_access}")
    logger.info(f"📚 Catalog Type: {config.catalog_type}")
    
    # Show backend type
    logger.info(f"🔧 Backend Class: {type(storage_backend).__name__}")
    
    # Show all Spark configurations
    spark_configs = storage_backend.get_spark_configs()
    logger.info(f"⚙️  Total Spark Configs: {len(spark_configs)} items")
    for key, value in spark_configs.items():
        logger.info(f"   {key}: {value}")


def demonstrate_code_simplification():
    """
    Demonstrate how much simpler the code is now.
    """
    logger.info("\n=== Code Simplification Demo ===")
    
    logger.info("✅ **Before (Separate Configurations)**")
    logger.info("   MinIOBackend.get_spark_configs():")
    logger.info("     - Get common configs")
    logger.info("     - Add MinIO-specific configs (endpoint, credentials)")
    logger.info("   S3Backend.get_spark_configs():")
    logger.info("     - Get common configs")
    logger.info("     - Add S3-specific configs (endpoint, credentials)")
    
    logger.info("\n✅ **After (Unified Configuration)**")
    logger.info("   StorageBackend.get_common_spark_configs():")
    logger.info("     - Handle ALL configurations based on environment variables")
    logger.info("   MinIOBackend.get_spark_configs():")
    logger.info("     - return self.get_common_spark_configs()")
    logger.info("   S3Backend.get_spark_configs():")
    logger.info("     - return self.get_common_spark_configs()")
    
    logger.info("\n🎯 **Benefits**")
    logger.info("   - Zero code duplication")
    logger.info("   - Single source of truth for all configurations")
    logger.info("   - Environment-driven configuration")
    logger.info("   - Easy to add new storage backends")


def demonstrate_environment_driven_config():
    """
    Demonstrate how configuration is driven by environment variables.
    """
    logger.info("\n=== Environment-Driven Configuration Demo ===")
    
    # Test different environment combinations
    env_combinations = [
        {
            'name': 'All Variables Set',
            'env': {
                'STORAGE_TYPE': 'minio',
                'STORAGE_ENDPOINT': 'http://localhost:9000',
                'STORAGE_ACCESS_KEY_ID': 'test-key',
                'STORAGE_SECRET_KEY': 'test-secret',
                'STORAGE_PATH_STYLE_ACCESS': 'true',
                'STORAGE_CREDENTIALS_PROVIDER': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
                'MINIO_BUCKET': 'test-bucket'
            },
            'expected_configs': 6
        },
        {
            'name': 'No Credentials Provider',
            'env': {
                'STORAGE_TYPE': 'minio',
                'STORAGE_ENDPOINT': 'http://localhost:9000',
                'STORAGE_ACCESS_KEY_ID': 'test-key',
                'STORAGE_SECRET_KEY': 'test-secret',
                'STORAGE_PATH_STYLE_ACCESS': 'true',
                'MINIO_BUCKET': 'test-bucket'
            },
            'expected_configs': 5
        },
        {
            'name': 'No Path Style Access',
            'env': {
                'STORAGE_TYPE': 'minio',
                'STORAGE_ENDPOINT': 'http://localhost:9000',
                'STORAGE_ACCESS_KEY_ID': 'test-key',
                'STORAGE_SECRET_KEY': 'test-secret',
                'MINIO_BUCKET': 'test-bucket'
            },
            'expected_configs': 4
        },
        {
            'name': 'No Credentials (IAM)',
            'env': {
                'STORAGE_TYPE': 's3',
                'S3_BUCKET': 'test-bucket'
            },
            'expected_configs': 2
        }
    ]
    
    for combo in env_combinations:
        logger.info(f"\n🔄 Testing: {combo['name']}")
        
        # Set environment
        for key, value in combo['env'].items():
            os.environ[key] = value
        
        manager = StorageManager()
        spark_configs = manager.get_spark_configs()
        
        logger.info(f"📊 Expected Configs: {combo['expected_configs']}")
        logger.info(f"📊 Actual Configs: {len(spark_configs)}")
        logger.info(f"📊 Match: {'✅ Yes' if len(spark_configs) == combo['expected_configs'] else '❌ No'}")
        
        # Show which configs were included
        logger.info("📋 Included Configurations:")
        for key, value in spark_configs.items():
            logger.info(f"   ✅ {key}: {value}")
        
        # Clean up
        for key in combo['env'].keys():
            if key in os.environ:
                del os.environ[key]


def show_unified_benefits():
    """
    Show the benefits of the unified configuration approach.
    """
    logger.info("\n=== Unified Configuration Benefits ===")
    
    logger.info("✅ **Single Source of Truth**")
    logger.info("   - All Spark configurations in one place")
    logger.info("   - No duplication between storage backends")
    logger.info("   - Consistent behavior across all storage types")
    
    logger.info("\n✅ **Environment-Driven**")
    logger.info("   - Configuration based on environment variable existence")
    logger.info("   - Optional configurations only added when needed")
    logger.info("   - Flexible and adaptable")
    
    logger.info("\n✅ **Zero Code Duplication**")
    logger.info("   - StorageBackend.get_spark_configs(): 1 line")
    logger.info("   - Single implementation for all storage types")
    logger.info("   - All logic in one place")
    
    logger.info("\n✅ **Easy to Extend**")
    logger.info("   - New storage backend: extend StorageBackend if needed")
    logger.info("   - Inherit all configuration logic automatically")
    logger.info("   - Override get_spark_configs() for custom logic")
    
    logger.info("\n✅ **Maintainable**")
    logger.info("   - Changes to configuration logic in one place")
    logger.info("   - No risk of inconsistent configurations")
    logger.info("   - Clear separation of concerns")


if __name__ == "__main__":
    demonstrate_unified_configuration()
    demonstrate_code_simplification()
    demonstrate_environment_driven_config()
    show_unified_benefits()
