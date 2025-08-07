#!/usr/bin/env python3
"""
Demo: Catalog Type Validation

This demo shows how the system validates catalog types and throws
errors for invalid or missing catalog types.
"""

import os
import sys
from loguru import logger

# Add the parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_manager import ConfigManager


def test_catalog_type_validation():
    """Test catalog type validation and error handling."""
    
    logger.info("🔧 Testing Catalog Type Validation")
    logger.info("=" * 60)
    
    # Test 1: Missing CATALOG_TYPE (should fail)
    logger.info("\n🔧 Test 1: Missing CATALOG_TYPE")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data'
        # CATALOG_TYPE intentionally missing
    })
    
    try:
        manager = ConfigManager()
        logger.info("❌ Should have failed - CATALOG_TYPE missing")
    except ValueError as e:
        logger.info(f"✅ Correctly failed: {e}")
    
    # Test 2: Empty CATALOG_TYPE (should fail)
    logger.info("\n🔧 Test 2: Empty CATALOG_TYPE")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': ''  # Empty string
    })
    
    try:
        manager = ConfigManager()
        logger.info("❌ Should have failed - CATALOG_TYPE empty")
    except ValueError as e:
        logger.info(f"✅ Correctly failed: {e}")
    
    # Test 3: Invalid catalog type (should fail)
    logger.info("\n🔧 Test 3: Invalid catalog type")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'invalid_catalog'
    })
    
    try:
        manager = ConfigManager()
        logger.info("❌ Should have failed - invalid catalog type")
    except ValueError as e:
        logger.info(f"✅ Correctly failed: {e}")
    
    # Test 4: Valid catalog types (should work)
    logger.info("\n🔧 Test 4: Valid catalog types")
    
    valid_catalog_types = ['hive', 'glue', 's3']
    
    for catalog_type in valid_catalog_types:
        logger.info(f"\n   Testing catalog type: {catalog_type.upper()}")
        os.environ.update({
            'STORAGE_BUCKET': 'spark-data',
            'CATALOG_TYPE': catalog_type
        })
        
        try:
            manager = ConfigManager()
            logger.info(f"   ✅ Successfully created ConfigManager with {catalog_type}")
            logger.info(f"   ✅ Catalog Backend: {type(manager.catalog_backend).__name__}")
        except Exception as e:
            logger.error(f"   ❌ Error with {catalog_type}: {e}")
    
    # Test 5: Case sensitivity (should work)
    logger.info("\n🔧 Test 5: Case sensitivity")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'GLUE'  # Uppercase
    })
    
    try:
        manager = ConfigManager()
        logger.info("✅ Successfully created ConfigManager with uppercase GLUE")
        logger.info(f"✅ Catalog Type: {manager.config.catalog_type}")
        logger.info(f"✅ Catalog Backend: {type(manager.catalog_backend).__name__}")
    except Exception as e:
        logger.error(f"❌ Error with uppercase: {e}")
    
    # Test 6: Whitespace handling (should work)
    logger.info("\n🔧 Test 6: Whitespace handling")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': '  glue  '  # With whitespace
    })
    
    try:
        manager = ConfigManager()
        logger.info("✅ Successfully created ConfigManager with whitespace")
        logger.info(f"✅ Catalog Type: {manager.config.catalog_type}")
        logger.info(f"✅ Catalog Backend: {type(manager.catalog_backend).__name__}")
    except Exception as e:
        logger.error(f"❌ Error with whitespace: {e}")
    
    logger.info("\n" + "=" * 60)
    logger.info("📋 Summary:")
    logger.info("✅ CATALOG_TYPE is required (no defaults)")
    logger.info("✅ Invalid catalog types throw ValueError")
    logger.info("✅ Empty or missing CATALOG_TYPE throws ValueError")
    logger.info("✅ Valid catalog types: 'hive', 'glue', 's3'")
    logger.info("✅ Case-insensitive (converted to lowercase)")
    logger.info("✅ Whitespace is trimmed")


def show_error_messages():
    """Show the different error messages for catalog type issues."""
    
    logger.info("\n=== Catalog Type Error Messages ===")
    
    logger.info("📋 Missing CATALOG_TYPE:")
    logger.info("   ValueError: CATALOG_TYPE must be set for storage configuration")
    
    logger.info("\n📋 Empty CATALOG_TYPE:")
    logger.info("   ValueError: CATALOG_TYPE must be set for storage configuration")
    
    logger.info("\n📋 Invalid catalog type:")
    logger.info("   ValueError: Unknown catalog type: invalid_catalog. Supported types: 'hive', 'glue', 's3'")
    
    logger.info("\n📋 Valid catalog types:")
    logger.info("   ✅ 'hive' → HiveCatalog")
    logger.info("   ✅ 'glue' → GlueCatalog")
    logger.info("   ✅ 's3' → S3Catalog")


def show_usage_examples():
    """Show usage examples for catalog types."""
    
    logger.info("\n=== Catalog Type Usage Examples ===")
    
    logger.info("📋 Hive Catalog:")
    logger.info("""
export STORAGE_BUCKET=spark-data
export CATALOG_TYPE=hive
python main.py
    """)
    
    logger.info("📋 Glue Catalog:")
    logger.info("""
export STORAGE_BUCKET=spark-data
export CATALOG_TYPE=glue
export AWS_REGION=us-east-1
python main.py
    """)
    
    logger.info("📋 S3 Catalog:")
    logger.info("""
export STORAGE_BUCKET=spark-data
export CATALOG_TYPE=s3
export AWS_REGION=us-east-1
python main.py
    """)
    
    logger.info("📋 Invalid (will fail):")
    logger.info("""
export STORAGE_BUCKET=spark-data
export CATALOG_TYPE=invalid  # ❌ Will throw ValueError
python main.py
    """)


if __name__ == "__main__":
    test_catalog_type_validation()
    show_error_messages()
    show_usage_examples()
