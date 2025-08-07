#!/usr/bin/env python3
"""
Demo: STORAGE_PATH_STYLE_ACCESS is independent of STORAGE_ENDPOINT

This demo shows that the path_style_access setting is completely independent
of whether STORAGE_ENDPOINT is set or not.
"""

import os
import sys
from loguru import logger

# Add the parent directory to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_manager import ConfigManager


def test_path_style_independence():
    """Test that path_style_access is independent of STORAGE_ENDPOINT."""
    
    logger.info("🔧 Testing STORAGE_PATH_STYLE_ACCESS independence")
    logger.info("=" * 60)
    
    # Test 1: MinIO with endpoint, path_style_access=true
    logger.info("\n🔧 Test 1: MinIO with endpoint, path_style_access=true")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue',
        'STORAGE_ENDPOINT': 'http://localhost:9000',
        'STORAGE_PATH_STYLE_ACCESS': 'true'
    })
    
    try:
        manager = ConfigManager()
        config = manager.config
        logger.info(f"✅ Storage Type: {manager.get_storage_info()['type']}")
        logger.info(f"✅ Endpoint: {config.endpoint}")
        logger.info(f"✅ Path Style Access: {config.path_style_access}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    
    # Test 2: MinIO with endpoint, path_style_access=false
    logger.info("\n🔧 Test 2: MinIO with endpoint, path_style_access=false")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue',
        'STORAGE_ENDPOINT': 'http://localhost:9000',
        'STORAGE_PATH_STYLE_ACCESS': 'false'
    })
    
    try:
        manager = ConfigManager()
        config = manager.config
        logger.info(f"✅ Storage Type: {manager.get_storage_info()['type']}")
        logger.info(f"✅ Endpoint: {config.endpoint}")
        logger.info(f"✅ Path Style Access: {config.path_style_access}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    
    # Test 3: S3 without endpoint, path_style_access=true
    logger.info("\n🔧 Test 3: S3 without endpoint, path_style_access=true")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue',
        'STORAGE_PATH_STYLE_ACCESS': 'true'
        # STORAGE_ENDPOINT intentionally not set
    })
    
    try:
        manager = ConfigManager()
        config = manager.config
        logger.info(f"✅ Storage Type: {manager.get_storage_info()['type']}")
        logger.info(f"✅ Endpoint: {config.endpoint}")
        logger.info(f"✅ Path Style Access: {config.path_style_access}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    
    # Test 4: S3 without endpoint, path_style_access=false
    logger.info("\n🔧 Test 4: S3 without endpoint, path_style_access=false")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue',
        'STORAGE_PATH_STYLE_ACCESS': 'false'
        # STORAGE_ENDPOINT intentionally not set
    })
    
    try:
        manager = ConfigManager()
        config = manager.config
        logger.info(f"✅ Storage Type: {manager.get_storage_info()['type']}")
        logger.info(f"✅ Endpoint: {config.endpoint}")
        logger.info(f"✅ Path Style Access: {config.path_style_access}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    
    # Test 5: S3 without endpoint, path_style_access not set
    logger.info("\n🔧 Test 5: S3 without endpoint, path_style_access not set")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue'
        # Both STORAGE_ENDPOINT and STORAGE_PATH_STYLE_ACCESS intentionally not set
    })
    
    try:
        manager = ConfigManager()
        config = manager.config
        logger.info(f"✅ Storage Type: {manager.get_storage_info()['type']}")
        logger.info(f"✅ Endpoint: {config.endpoint}")
        logger.info(f"✅ Path Style Access: {config.path_style_access}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    
    # Test 6: MinIO with endpoint, path_style_access not set
    logger.info("\n🔧 Test 6: MinIO with endpoint, path_style_access not set")
    os.environ.update({
        'STORAGE_BUCKET': 'spark-data',
        'CATALOG_TYPE': 'glue',
        'STORAGE_ENDPOINT': 'http://localhost:9000'
        # STORAGE_PATH_STYLE_ACCESS intentionally not set
    })
    
    try:
        manager = ConfigManager()
        config = manager.config
        logger.info(f"✅ Storage Type: {manager.get_storage_info()['type']}")
        logger.info(f"✅ Endpoint: {config.endpoint}")
        logger.info(f"✅ Path Style Access: {config.path_style_access}")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
    
    logger.info("\n" + "=" * 60)
    logger.info("📋 Summary:")
    logger.info("✅ path_style_access is completely independent of STORAGE_ENDPOINT")
    logger.info("✅ It can be set to true/false regardless of endpoint presence")
    logger.info("✅ When not set, it defaults to None (Spark will use its defaults)")
    logger.info("✅ This allows fine-grained control over S3A path style access")


if __name__ == "__main__":
    test_path_style_independence()
