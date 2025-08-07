"""
Demonstration of unified environment variables.

This script shows how the environment variables are now unified
across different storage types with consistent naming.
"""

import os
import logging
from config_manager import ConfigManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def demonstrate_unified_environment_variables():
    """
    Demonstrate the unified environment variable approach.
    """
    logger.info("=== Unified Environment Variables Demo ===")
    
    # Test scenarios
    scenarios = [
        {
            'name': 'MinIO with Unified Variables',
            'env': {
                'STORAGE_TYPE': 'minio',
                'STORAGE_ENDPOINT': 'http://localhost:9000',
                'STORAGE_ACCESS_KEY_ID': 'minioadmin',
                'STORAGE_SECRET_KEY': 'minioadmin',
                'STORAGE_BUCKET': 'spark-data',
                'STORAGE_PATH_STYLE_ACCESS': 'true',
                'STORAGE_CREDENTIALS_PROVIDER': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
            }
        },
        {
            'name': 'MinIO with Legacy Variables (Backward Compatibility)',
            'env': {
                'STORAGE_TYPE': 'minio',
                'MINIO_ENDPOINT': 'http://localhost:9000',
                'MINIO_ACCESS_KEY': 'minioadmin',
                'MINIO_SECRET_KEY': 'minioadmin',
                'MINIO_BUCKET': 'spark-data'
            }
        },
        {
            'name': 'AWS S3 with Unified Variables',
            'env': {
                'STORAGE_TYPE': 's3',
                'STORAGE_ACCESS_KEY_ID': 'your-aws-key',
                'STORAGE_SECRET_KEY': 'your-aws-secret',
                'STORAGE_BUCKET': 'my-spark-bucket',
                'STORAGE_PATH_STYLE_ACCESS': 'false'
            }
        },
        {
            'name': 'AWS S3 with Legacy Variables (Backward Compatibility)',
            'env': {
                'STORAGE_TYPE': 's3',
                'AWS_ACCESS_KEY_ID': 'your-aws-key',
                'AWS_SECRET_ACCESS_KEY': 'your-aws-secret',
                'S3_BUCKET': 'my-spark-bucket'
            }
        },
        {
            'name': 'Mixed Unified and Legacy Variables',
            'env': {
                'STORAGE_TYPE': 'minio',
                'STORAGE_ENDPOINT': 'http://localhost:9000',
                'MINIO_ACCESS_KEY': 'minioadmin',  # Legacy
                'STORAGE_SECRET_KEY': 'minioadmin',  # Unified
                'STORAGE_BUCKET': 'spark-data'  # Unified
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
    info = manager.get_storage_info()
    
    logger.info(f"📦 Storage Type: {info['type']}")
    logger.info(f"🔗 Endpoint: {config.endpoint or 'Not Set'}")
    logger.info(f"🔑 Access Key: {config.access_key or 'Not Set'}")
    logger.info(f"🔐 Secret Key: {'***' if config.secret_key else 'Not Set'}")
    logger.info(f"🏪 Bucket: {config.bucket}")
    logger.info(f"🌍 Region: {config.region}")
    logger.info(f"🔧 Credentials Provider: {config.credentials_provider or 'Not Set'}")
    logger.info(f"🛣️  Path Style Access: {config.path_style_access}")
    logger.info(f"📚 Catalog Type: {config.catalog_type}")
    
    # Show backend type
    storage_backend = manager.storage_backend
    logger.info(f"🔧 Storage Backend: {type(storage_backend).__name__}")
    
    # Show all Spark configurations
    spark_configs = manager.get_spark_configs()
    logger.info(f"⚙️  Spark Configs: {len(spark_configs)} items")
    
    # Show all catalog configurations
    catalog_configs = manager.get_catalog_configs()
    logger.info(f"📚 Catalog Configs: {len(catalog_configs)} items")


def demonstrate_variable_precedence():
    """
    Demonstrate the precedence order of environment variables.
    """
    logger.info("\n=== Variable Precedence Demo ===")
    
    # Test precedence scenarios
    precedence_scenarios = [
        {
            'name': 'Unified Variables Take Precedence',
            'env': {
                'STORAGE_TYPE': 'minio',
                'STORAGE_ENDPOINT': 'http://unified-endpoint:9000',
                'MINIO_ENDPOINT': 'http://legacy-endpoint:9000',
                'STORAGE_ACCESS_KEY_ID': 'unified-key',
                'MINIO_ACCESS_KEY': 'legacy-key',
                'STORAGE_BUCKET': 'unified-bucket',
                'MINIO_BUCKET': 'legacy-bucket'
            },
            'expected': {
                'endpoint': 'http://unified-endpoint:9000',
                'access_key': 'unified-key',
                'bucket': 'unified-bucket'
            }
        },
        {
            'name': 'Legacy Variables as Fallback',
            'env': {
                'STORAGE_TYPE': 'minio',
                'MINIO_ENDPOINT': 'http://legacy-endpoint:9000',
                'MINIO_ACCESS_KEY': 'legacy-key',
                'MINIO_BUCKET': 'legacy-bucket'
            },
            'expected': {
                'endpoint': 'http://legacy-endpoint:9000',
                'access_key': 'legacy-key',
                'bucket': 'legacy-bucket'
            }
        }
    ]
    
    for scenario in precedence_scenarios:
        logger.info(f"\n🔄 Testing: {scenario['name']}")
        
        # Set environment variables
        for key, value in scenario['env'].items():
            os.environ[key] = value
        
        # Create storage manager
        manager = ConfigManager()
        config = manager.config
        
        # Show actual vs expected
        logger.info(f"📊 Expected Endpoint: {scenario['expected']['endpoint']}")
        logger.info(f"📊 Actual Endpoint: {config.endpoint}")
        logger.info(f"📊 Match: {'✅ Yes' if config.endpoint == scenario['expected']['endpoint'] else '❌ No'}")
        
        logger.info(f"📊 Expected Access Key: {scenario['expected']['access_key']}")
        logger.info(f"📊 Actual Access Key: {config.access_key}")
        logger.info(f"📊 Match: {'✅ Yes' if config.access_key == scenario['expected']['access_key'] else '❌ No'}")
        
        logger.info(f"📊 Expected Bucket: {scenario['expected']['bucket']}")
        logger.info(f"📊 Actual Bucket: {config.bucket}")
        logger.info(f"📊 Match: {'✅ Yes' if config.bucket == scenario['expected']['bucket'] else '❌ No'}")
        
        # Clean up
        for key in scenario['env'].keys():
            if key in os.environ:
                del os.environ[key]


def show_unified_benefits():
    """
    Show the benefits of unified environment variables.
    """
    logger.info("\n=== Unified Environment Variables Benefits ===")
    
    logger.info("✅ **Consistent Naming**")
    logger.info("   - STORAGE_ENDPOINT (not MINIO_ENDPOINT vs AWS_ENDPOINT)")
    logger.info("   - STORAGE_ACCESS_KEY_ID (not MINIO_ACCESS_KEY vs AWS_ACCESS_KEY_ID)")
    logger.info("   - STORAGE_SECRET_KEY (not MINIO_SECRET_KEY vs AWS_SECRET_ACCESS_KEY)")
    logger.info("   - STORAGE_BUCKET (not MINIO_BUCKET vs S3_BUCKET)")
    
    logger.info("\n✅ **Backward Compatibility**")
    logger.info("   - Legacy variables still work as fallbacks")
    logger.info("   - Gradual migration possible")
    logger.info("   - No breaking changes")
    
    logger.info("\n✅ **Simplified Configuration**")
    logger.info("   - Same variable names for all storage types")
    logger.info("   - Easier to remember and use")
    logger.info("   - Consistent across environments")
    
    logger.info("\n✅ **Flexible Precedence**")
    logger.info("   - Unified variables take precedence")
    logger.info("   - Legacy variables as fallbacks")
    logger.info("   - Mixed usage supported")


def demonstrate_migration_path():
    """
    Demonstrate the migration path from legacy to unified variables.
    """
    logger.info("\n=== Migration Path Demo ===")
    
    logger.info("🔄 **Phase 1: Legacy Only**")
    logger.info("   MINIO_ENDPOINT=http://localhost:9000")
    logger.info("   MINIO_ACCESS_KEY=minioadmin")
    logger.info("   MINIO_SECRET_KEY=minioadmin")
    logger.info("   MINIO_BUCKET=spark-data")
    
    logger.info("\n🔄 **Phase 2: Mixed (Current)**")
    logger.info("   STORAGE_ENDPOINT=http://localhost:9000  # New")
    logger.info("   MINIO_ACCESS_KEY=minioadmin            # Legacy")
    logger.info("   STORAGE_SECRET_KEY=minioadmin          # New")
    logger.info("   MINIO_BUCKET=spark-data                # Legacy")
    
    logger.info("\n🔄 **Phase 3: Unified Only**")
    logger.info("   STORAGE_ENDPOINT=http://localhost:9000")
    logger.info("   STORAGE_ACCESS_KEY_ID=minioadmin")
    logger.info("   STORAGE_SECRET_KEY=minioadmin")
    logger.info("   STORAGE_BUCKET=spark-data")


if __name__ == "__main__":
    demonstrate_unified_environment_variables()
    demonstrate_variable_precedence()
    show_unified_benefits()
    demonstrate_migration_path()
