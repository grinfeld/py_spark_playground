#!/usr/bin/env python3
"""
Demo: Explicit Configuration Requirements

This demonstrates how the storage configuration now requires explicit
environment variables and will fail with clear error messages if they're missing.
"""

import os
import logging
from config_manager import ConfigManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger(__name__)


def demonstrate_explicit_requirements():
    """
    Demonstrate that all required environment variables must be explicitly set.
    """
    logger.info("=== Explicit Configuration Requirements ===")
    
    # Test 1: Missing STORAGE_BUCKET (should fail)
    logger.info("\n🔧 Test 1: Missing STORAGE_BUCKET")
    os.environ.update({
        'STORAGE_ENDPOINT': 'http://localhost:9000',
        'STORAGE_ACCESS_KEY_ID': 'minioadmin',
        'STORAGE_SECRET_KEY': 'minioadmin',
        'CATALOG_TYPE': 'glue'
        # STORAGE_BUCKET intentionally missing
    })
    
    try:
        manager = ConfigManager()
        logger.info("❌ Should have failed - STORAGE_BUCKET missing")
    except ValueError as e:
        logger.info(f"✅ Correctly failed: {e}")
    
    # Test 2: Missing CATALOG_TYPE (should fail)
    logger.info("\n🔧 Test 2: Missing CATALOG_TYPE")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data'
        # CATALOG_TYPE intentionally missing
    })
    
    try:
        manager = ConfigManager()
        logger.info("❌ Should have failed - CATALOG_TYPE missing")
    except ValueError as e:
        logger.info(f"✅ Correctly failed: {e}")
    
    # Test 3: Only required variables set (should work)
    logger.info("\n🔧 Test 3: Only required variables set")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue'
        # All other variables intentionally missing
    })
    
    try:
        manager = ConfigManager()
        logger.info("✅ Successfully created ConfigManager with only required variables")
        storage_info = manager.get_storage_info()
        logger.info(f"📦 Storage Type: {storage_info['type']}")
        logger.info(f"📦 Endpoint: {manager.config.endpoint}")
        logger.info(f"📦 Access Key: {manager.config.access_key}")
        logger.info(f"📦 Secret Key: {manager.config.secret_key}")
        logger.info(f"📦 Bucket: {manager.config.bucket}")
        logger.info(f"📦 Catalog Type: {manager.config.catalog_type}")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
    
    # Test 4: Missing STORAGE_BUCKET for S3 (should fail)
    logger.info("\n🔧 Test 4: Missing STORAGE_BUCKET for S3")
    os.environ.update({
        'STORAGE_ACCESS_KEY_ID': 'your-key',
        'STORAGE_SECRET_KEY': 'your-secret',
        'CATALOG_TYPE': 'glue'
        # STORAGE_BUCKET intentionally missing
    })
    
    try:
        manager = StorageManager()
        logger.info("❌ Should have failed - STORAGE_BUCKET missing")
    except ValueError as e:
        logger.info(f"✅ Correctly failed: {e}")
    
    # Test 5: Complete MinIO configuration (should work)
    logger.info("\n🔧 Test 4: Complete MinIO Configuration")
    os.environ.update({
        'STORAGE_ENDPOINT': 'http://localhost:9000',
        'STORAGE_ACCESS_KEY_ID': 'minioadmin',
        'STORAGE_SECRET_KEY': 'minioadmin',
        'STORAGE_BUCKET': 'spark-data'
    })
    
    try:
        manager = ConfigManager()
        logger.info("✅ Successfully created ConfigManager")
        storage_info = manager.get_storage_info()
        logger.info(f"📦 Storage Type: {storage_info['type']}")
        logger.info(f"📦 Endpoint: {manager.config.endpoint}")
        logger.info(f"📦 Bucket: {manager.config.bucket}")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
    
    # Test 6: Invalid catalog type (should fail)
    logger.info("\n🔧 Test 6: Invalid catalog type")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'invalid_catalog'  # Invalid catalog type
    })
    
    try:
        manager = ConfigManager()
        logger.info("❌ Should have failed - invalid catalog type")
    except ValueError as e:
        logger.info(f"✅ Correctly failed: {e}")
    
    # Test 7: Complete S3 configuration (should work)
    logger.info("\n🔧 Test 7: Complete S3 Configuration")
    os.environ.update({
        'STORAGE_ACCESS_KEY_ID': 'your-key',
        'STORAGE_SECRET_KEY': 'your-secret',
        'STORAGE_BUCKET': 'my-bucket',
        'CATALOG_TYPE': 'glue',
        'AWS_REGION': 'us-east-1'
    })
    
    try:
        manager = ConfigManager()
        logger.info("✅ Successfully created ConfigManager")
        storage_info = manager.get_storage_info()
        logger.info(f"📦 Storage Type: {storage_info['type']}")
        logger.info(f"📦 Endpoint: {manager.config.endpoint}")
        logger.info(f"📦 Bucket: {manager.config.bucket}")
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")


def demonstrate_benefits():
    """
    Show the benefits of explicit configuration requirements.
    """
    logger.info("\n=== Benefits of Explicit Configuration ===")
    
    logger.info("✅ **No Hidden Defaults**")
    logger.info("   - All configuration is explicit and visible")
    logger.info("   - No surprise behavior from hidden defaults")
    logger.info("   - Clear error messages when variables are missing")
    
    logger.info("\n✅ **Environment-Specific Configuration**")
    logger.info("   - Each environment must explicitly set its values")
    logger.info("   - No accidental use of development defaults in production")
    logger.info("   - Forces proper configuration management")
    
    logger.info("\n✅ **Better Error Handling**")
    logger.info("   - Clear error messages for missing variables")
    logger.info("   - Fail fast with descriptive errors")
    logger.info("   - Easy to identify configuration issues")
    
    logger.info("\n✅ **Improved Security**")
    logger.info("   - No hardcoded credentials in defaults")
    logger.info("   - Forces explicit credential management")
    logger.info("   - Reduces risk of credential exposure")


def show_required_variables():
    """
    Show all required environment variables.
    """
    logger.info("\n=== Required Environment Variables ===")
    
    logger.info("📋 Required Variables:")
    logger.info("   ✅ STORAGE_BUCKET=spark-data")
    logger.info("   ✅ CATALOG_TYPE=glue")
    logger.info("   ✅ CATALOG_WAREHOUSE_NAME=iceberg-warehouse")
    
    logger.info("\n📋 Optional Variables (all others):")
    logger.info("   🔸 STORAGE_ENDPOINT=http://localhost:9000 (auto-detects MinIO if set)")
    logger.info("   🔸 STORAGE_ACCESS_KEY_ID=minioadmin")
    logger.info("   🔸 STORAGE_SECRET_KEY=minioadmin")
    logger.info("   🔸 AWS_REGION=us-east-1")
    logger.info("   🔸 STORAGE_CREDENTIALS_PROVIDER (uses Spark defaults)")
    logger.info("   🔸 STORAGE_PATH_STYLE_ACCESS (uses Spark defaults)")
    logger.info("   🔸 CATALOG_WAREHOUSE_PATH (uses class defaults)")
    logger.info("   🔸 GLUE_IO_IMPL (uses class defaults)")


def demonstrate_docker_usage():
    """
    Show how to use with Docker Compose.
    """
    logger.info("\n=== Docker Compose Usage ===")
    
    logger.info("📋 Create .env file with required variables:")
    logger.info("""
# .env file
STORAGE_BUCKET=spark-data
CATALOG_TYPE=glue
CATALOG_WAREHOUSE_NAME=iceberg-warehouse
STORAGE_ENDPOINT=http://minio:9000
STORAGE_ACCESS_KEY_ID=minioadmin
STORAGE_SECRET_KEY=minioadmin
AWS_REGION=us-east-1
    """)
    
    logger.info("📋 Run with Docker Compose:")
    logger.info("""
docker-compose up
    """)
    
    logger.info("📋 For production with S3:")
    logger.info("""
# .env file
STORAGE_BUCKET=your-production-bucket
CATALOG_TYPE=glue
CATALOG_WAREHOUSE_NAME=iceberg-warehouse
STORAGE_ACCESS_KEY_ID=your-production-key
STORAGE_SECRET_KEY=your-production-secret
AWS_REGION=us-east-1
    """)


if __name__ == "__main__":
    demonstrate_explicit_requirements()
    demonstrate_benefits()
    show_required_variables()
    demonstrate_docker_usage()
