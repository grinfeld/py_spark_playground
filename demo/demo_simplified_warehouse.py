#!/usr/bin/env python3
"""
Demo: Simplified CATALOG_WAREHOUSE_NAME Configuration

This demo shows how the warehouse path configuration has been simplified
to use only CATALOG_WAREHOUSE_NAME for all catalog types.
"""

import os
import sys
from loguru import logger

# Add the parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_manager import ConfigManager


def test_simplified_warehouse():
    """Test the simplified warehouse path configuration."""
    
    logger.info("🔧 Testing Simplified CATALOG_WAREHOUSE_NAME Configuration")
    logger.info("=" * 60)
    
    # Test 1: Missing CATALOG_WAREHOUSE_NAME (should fail)
    logger.info("\n🔧 Test 1: Missing CATALOG_WAREHOUSE_NAME")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue'
        # CATALOG_WAREHOUSE_NAME intentionally missing
    })
    
    try:
        manager = ConfigManager()
        catalog_backend = manager.catalog_backend
        warehouse_path = catalog_backend._get_warehouse_path()
        logger.info("❌ Should have failed - CATALOG_WAREHOUSE_NAME missing")
    except ValueError as e:
        logger.info(f"✅ Correctly failed: {e}")
    
    # Test 2: Required warehouse name set
    logger.info("\n🔧 Test 2: Required warehouse name set")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue',
        'CATALOG_WAREHOUSE_NAME': 'iceberg-warehouse'
    })
    
    try:
        manager = ConfigManager()
        catalog_backend = manager.catalog_backend
        warehouse_path = catalog_backend._get_warehouse_path()
        logger.info(f"✅ Catalog Type: {manager.config.catalog_type}")
        logger.info(f"✅ Warehouse Path: {warehouse_path}")
        logger.info(f"✅ Uses Required: {warehouse_path == 's3a://spark-data/iceberg-warehouse'}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    
    # Test 2: Custom warehouse name
    logger.info("\n🔧 Test 2: Custom warehouse name")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue',
        'CATALOG_WAREHOUSE_NAME': 'my-custom-warehouse'
    })
    
    try:
        manager = ConfigManager()
        catalog_backend = manager.catalog_backend
        warehouse_path = catalog_backend._get_warehouse_path()
        logger.info(f"✅ Catalog Type: {manager.config.catalog_type}")
        logger.info(f"✅ Warehouse Path: {warehouse_path}")
        logger.info(f"✅ Uses Custom: {warehouse_path == 's3a://spark-data/my-custom-warehouse'}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    
    # Test 3: Different warehouse names for different catalogs
    logger.info("\n🔧 Test 3: Different warehouse names for different catalogs")
    
    catalog_types = ['glue', 's3', 'hive']
    warehouse_names = ['glue-warehouse', 's3-warehouse', 'hive-warehouse']
    
    for catalog_type, warehouse_name in zip(catalog_types, warehouse_names):
        logger.info(f"\n   Testing {catalog_type.upper()} with {warehouse_name}")
        os.environ.update({
            'STORAGE_BUCKET': 'spark-data',
            'CATALOG_TYPE': catalog_type,
            'CATALOG_WAREHOUSE_NAME': warehouse_name
        })
        
        try:
            manager = ConfigManager()
            catalog_backend = manager.catalog_backend
            warehouse_path = catalog_backend._get_warehouse_path()
            expected_path = f"s3a://spark-data/{warehouse_name}"
            logger.info(f"   ✅ Warehouse Path: {warehouse_path}")
            logger.info(f"   ✅ Matches Expected: {warehouse_path == expected_path}")
        except Exception as e:
            logger.error(f"   ❌ Error: {e}")
    
    # Test 4: Different buckets with same warehouse name
    logger.info("\n🔧 Test 4: Different buckets with same warehouse name")
    
    buckets = ['dev-data', 'prod-data', 'test-data']
    warehouse_name = 'iceberg-warehouse'
    
    for bucket in buckets:
        logger.info(f"\n   Testing bucket '{bucket}' with warehouse '{warehouse_name}'")
        os.environ.update({
            'STORAGE_BUCKET': bucket,
            'CATALOG_TYPE': 'glue',
            'CATALOG_WAREHOUSE_NAME': warehouse_name
        })
        
        try:
            manager = ConfigManager()
            catalog_backend = manager.catalog_backend
            warehouse_path = catalog_backend._get_warehouse_path()
            expected_path = f"s3a://{bucket}/{warehouse_name}"
            logger.info(f"   ✅ Warehouse Path: {warehouse_path}")
            logger.info(f"   ✅ Matches Expected: {warehouse_path == expected_path}")
        except Exception as e:
            logger.error(f"   ❌ Error: {e}")
    
    logger.info("\n" + "=" * 60)
    logger.info("📋 Summary:")
    logger.info("✅ Single CATALOG_WAREHOUSE_NAME variable for all catalog types")
    logger.info("✅ Warehouse path = s3a://{bucket}/{warehouse_name}")
    logger.info("✅ Required warehouse name (no defaults)")
    logger.info("✅ No catalog-specific warehouse path variables")
    logger.info("✅ Cleaner and more consistent configuration")


def show_environment_variables():
    """Show the simplified environment variable configuration."""
    
    logger.info("\n=== Simplified Warehouse Path Configuration ===")
    
    logger.info("📋 Before (Complex):")
    logger.info("   🔸 CATALOG_WAREHOUSE_PATH (global override)")
    logger.info("   🔸 HIVE_WAREHOUSE_PATH (Hive-specific)")
    logger.info("   🔸 GLUE_WAREHOUSE_PATH (Glue-specific)")
    
    logger.info("\n📋 After (Simplified):")
    logger.info("   ✅ CATALOG_WAREHOUSE_NAME (single variable for all catalogs)")
    
    logger.info("\n📋 Usage Examples:")
    logger.info("""
# Required warehouse name
export CATALOG_WAREHOUSE_NAME=iceberg-warehouse
# Result: s3a://spark-data/iceberg-warehouse

# Custom warehouse name
export CATALOG_WAREHOUSE_NAME=my-custom-warehouse
# Result: s3a://spark-data/my-custom-warehouse

# Different warehouse for different environments
export CATALOG_WAREHOUSE_NAME=dev-warehouse    # Development
export CATALOG_WAREHOUSE_NAME=prod-warehouse   # Production
export CATALOG_WAREHOUSE_NAME=test-warehouse   # Testing

# Must be set - no defaults
# export CATALOG_WAREHOUSE_NAME=  # Will raise ValueError
    """)


def show_path_construction():
    """Show how warehouse paths are constructed."""
    
    logger.info("\n=== Warehouse Path Construction ===")
    
    logger.info("📋 Formula:")
    logger.info("   warehouse_path = s3a://{STORAGE_BUCKET}/{CATALOG_WAREHOUSE_NAME}")
    
    logger.info("\n📋 Examples:")
    
    examples = [
        ("spark-data", "iceberg-warehouse", "s3a://spark-data/iceberg-warehouse"),
        ("my-bucket", "custom-warehouse", "s3a://my-bucket/custom-warehouse"),
        ("prod-data", "glue-warehouse", "s3a://prod-data/glue-warehouse"),
        ("test-bucket", "hive-warehouse", "s3a://test-bucket/hive-warehouse")
    ]
    
    for bucket, warehouse_name, expected_path in examples:
        logger.info(f"   STORAGE_BUCKET={bucket}")
        logger.info(f"   CATALOG_WAREHOUSE_NAME={warehouse_name}")
        logger.info(f"   → {expected_path}")
        logger.info("")


if __name__ == "__main__":
    test_simplified_warehouse()
    show_environment_variables()
    show_path_construction()
