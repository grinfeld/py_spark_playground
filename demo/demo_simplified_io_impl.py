#!/usr/bin/env python3
"""
Demo: Simplified CATALOG_IO_IMPL Configuration

This demo shows how the IO implementation configuration has been simplified
to use only CATALOG_IO_IMPL for all catalog types.
"""

import os
import sys
from loguru import logger

# Add the parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_manager import ConfigManager


def test_simplified_io_impl():
    """Test the simplified IO implementation configuration."""
    
    logger.info("🔧 Testing Simplified CATALOG_IO_IMPL Configuration")
    logger.info("=" * 60)
    
    # Test 1: No IO implementation set (should use defaults)
    logger.info("\n🔧 Test 1: No IO implementation set (defaults)")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue'
    })
    
    try:
        manager = ConfigManager()
        catalog_backend = manager.catalog_backend
        io_impl = catalog_backend._get_io_impl()
        logger.info(f"✅ Catalog Type: {manager.config.catalog_type}")
        logger.info(f"✅ IO Implementation: {io_impl}")
        logger.info(f"✅ Uses Default: {io_impl is None}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    
    # Test 2: CATALOG_IO_IMPL set for Glue
    logger.info("\n🔧 Test 2: CATALOG_IO_IMPL set for Glue")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue',
        'CATALOG_IO_IMPL': 'org.apache.iceberg.aws.s3.S3FileIO'
    })
    
    try:
        manager = ConfigManager()
        catalog_backend = manager.catalog_backend
        io_impl = catalog_backend._get_io_impl()
        logger.info(f"✅ Catalog Type: {manager.config.catalog_type}")
        logger.info(f"✅ IO Implementation: {io_impl}")
        logger.info(f"✅ Uses Environment: {io_impl == 'org.apache.iceberg.aws.s3.S3FileIO'}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    
    # Test 3: CATALOG_IO_IMPL set for S3
    logger.info("\n🔧 Test 3: CATALOG_IO_IMPL set for S3")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 's3',
        'CATALOG_IO_IMPL': 'org.apache.iceberg.aws.s3.S3FileIO'
    })
    
    try:
        manager = ConfigManager()
        catalog_backend = manager.catalog_backend
        io_impl = catalog_backend._get_io_impl()
        logger.info(f"✅ Catalog Type: {manager.config.catalog_type}")
        logger.info(f"✅ IO Implementation: {io_impl}")
        logger.info(f"✅ Uses Environment: {io_impl == 'org.apache.iceberg.aws.s3.S3FileIO'}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    
    # Test 4: CATALOG_IO_IMPL set for Hive
    logger.info("\n🔧 Test 4: CATALOG_IO_IMPL set for Hive")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'hive',
        'CATALOG_IO_IMPL': 'org.apache.iceberg.hadoop.HadoopFileIO'
    })
    
    try:
        manager = ConfigManager()
        catalog_backend = manager.catalog_backend
        io_impl = catalog_backend._get_io_impl()
        logger.info(f"✅ Catalog Type: {manager.config.catalog_type}")
        logger.info(f"✅ IO Implementation: {io_impl}")
        logger.info(f"✅ Uses Environment: {io_impl == 'org.apache.iceberg.hadoop.HadoopFileIO'}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    
    # Test 5: Different IO implementation for each catalog
    logger.info("\n🔧 Test 5: Different IO implementations")
    
    catalog_types = ['glue', 's3', 'hive']
    io_implementations = [
        'org.apache.iceberg.aws.s3.S3FileIO',
        'org.apache.iceberg.aws.s3.S3FileIO', 
        'org.apache.iceberg.hadoop.HadoopFileIO'
    ]
    
    for catalog_type, io_impl in zip(catalog_types, io_implementations):
        logger.info(f"\n   Testing {catalog_type.upper()} with {io_impl}")
        os.environ.update({
            'STORAGE_BUCKET': 'spark-data',
            'CATALOG_TYPE': catalog_type,
            'CATALOG_IO_IMPL': io_impl
        })
        
        try:
            manager = ConfigManager()
            catalog_backend = manager.catalog_backend
            actual_io_impl = catalog_backend._get_io_impl()
            logger.info(f"   ✅ Actual IO Implementation: {actual_io_impl}")
            logger.info(f"   ✅ Matches Expected: {actual_io_impl == io_impl}")
        except Exception as e:
            logger.error(f"   ❌ Error: {e}")
    
    logger.info("\n" + "=" * 60)
    logger.info("📋 Summary:")
    logger.info("✅ Single CATALOG_IO_IMPL variable for all catalog types")
    logger.info("✅ Only set if environment variable exists")
    logger.info("✅ No catalog-specific IO implementation variables")
    logger.info("✅ Cleaner and more consistent configuration")
    logger.info("✅ Easier to manage and understand")


def show_environment_variables():
    """Show the simplified environment variable configuration."""
    
    logger.info("\n=== Simplified IO Implementation Configuration ===")
    
    logger.info("📋 Before (Complex):")
    logger.info("   🔸 CATALOG_IO_IMPL (global override)")
    logger.info("   🔸 GLUE_IO_IMPL (Glue-specific)")
    logger.info("   🔸 S3_CATALOG_IO_IMPL (S3-specific)")
    
    logger.info("\n📋 After (Simplified):")
    logger.info("   ✅ CATALOG_IO_IMPL (single variable for all catalogs)")
    
    logger.info("\n📋 Usage Examples:")
    logger.info("""
# For Glue Catalog
export CATALOG_IO_IMPL=org.apache.iceberg.aws.s3.S3FileIO

# For S3 Catalog  
export CATALOG_IO_IMPL=org.apache.iceberg.aws.s3.S3FileIO

# For Hive Catalog
export CATALOG_IO_IMPL=org.apache.iceberg.hadoop.HadoopFileIO

# Or leave unset to use defaults
# export CATALOG_IO_IMPL=
    """)


if __name__ == "__main__":
    test_simplified_io_impl()
    show_environment_variables()
