"""
Demonstration of improved inheritance structure with common configurations.

This script shows how the common Spark configurations are now shared
between storage backends using inheritance.
"""

import os
import logging
from config_manager import ConfigManager, StorageBackend

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def demonstrate_inheritance_structure():
    """
    Demonstrate the improved inheritance structure.
    """
    logger.info("=== Improved Inheritance Structure Demo ===")
    
    # Test scenarios
    scenarios = [
        {
            'name': 'MinIO with Path Style Access',
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
            'name': 'MinIO without Path Style Access (Custom)',
            'env': {
                'STORAGE_TYPE': 'minio',
                'STORAGE_ENDPOINT': 'http://localhost:9000',
                'STORAGE_ACCESS_KEY_ID': 'minioadmin',
                'STORAGE_SECRET_KEY': 'minioadmin',
                'STORAGE_PATH_STYLE_ACCESS': 'false',
                'STORAGE_CREDENTIALS_PROVIDER': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
                'MINIO_BUCKET': 'spark-data'
            }
        },
        {
            'name': 'AWS S3 with Virtual Hosted Style',
            'env': {
                'STORAGE_TYPE': 's3',
                'STORAGE_ACCESS_KEY_ID': 'your-aws-key',
                'STORAGE_SECRET_KEY': 'your-aws-secret',
                'STORAGE_PATH_STYLE_ACCESS': 'false',
                'S3_BUCKET': 'my-spark-bucket'
            }
        },
        {
            'name': 'AWS S3 with Path Style Access',
            'env': {
                'STORAGE_TYPE': 's3',
                'STORAGE_ACCESS_KEY_ID': 'your-aws-key',
                'STORAGE_SECRET_KEY': 'your-aws-secret',
                'STORAGE_PATH_STYLE_ACCESS': 'true',
                'S3_BUCKET': 'my-spark-bucket'
            }
        },
        {
            'name': 'S3-Compatible Service with Custom Endpoint',
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
        show_inheritance_details(manager, scenario['name'])
        
        # Clean up environment
        for key in scenario['env'].keys():
            if key in os.environ:
                del os.environ[key]


def show_inheritance_details(manager, scenario_name):
    """
    Show the inheritance structure and configuration details.
    """
    config = manager.config
    storage_backend = manager.storage_backend
    
    logger.info(f"📦 Storage Type: {config.storage_type}")
    logger.info(f"🔗 Endpoint: {config.endpoint or 'Default'}")
    logger.info(f"🔑 Access Key: {config.access_key or 'Not Set'}")
    logger.info(f"🔐 Secret Key: {'***' if config.secret_key else 'Not Set'}")
    logger.info(f"🏪 Bucket: {config.bucket}")
    logger.info(f"🌍 Region: {config.region}")
    logger.info(f"🔧 Credentials Provider: {config.credentials_provider or 'Spark Default'}")
    logger.info(f"🛣️  Path Style Access: {config.path_style_access}")
    logger.info(f"📚 Catalog Type: {config.catalog_type}")
    
    # Show backend type
    logger.info(f"🔧 Backend Class: {type(storage_backend).__name__}")
    
    # Show common configurations
    common_configs = storage_backend.get_common_spark_configs()
    logger.info(f"⚙️  Common Configs: {len(common_configs)} items")
    for key, value in common_configs.items():
        logger.info(f"   {key}: {value}")
    
    # Show specific configurations
    specific_configs = storage_backend.get_spark_configs()
    specific_only = {k: v for k, v in specific_configs.items() if k not in common_configs}
    logger.info(f"🔧 Specific Configs: {len(specific_only)} items")
    for key, value in specific_only.items():
        logger.info(f"   {key}: {value}")


def demonstrate_common_configurations():
    """
    Demonstrate the common configurations shared by all backends.
    """
    logger.info("\n=== Common Configurations Demo ===")
    
    # Test different backends
    backends = [
        ('MinIO', 'minio'),
        ('S3', 's3')
    ]
    
    for backend_name, storage_type in backends:
        logger.info(f"\n🔄 Testing {backend_name} Backend")
        
        # Set environment
        os.environ.update({
            'STORAGE_TYPE': storage_type,
            'STORAGE_ENDPOINT': 'http://localhost:9000' if storage_type == 'minio' else '',
            'STORAGE_ACCESS_KEY_ID': 'test-key',
            'STORAGE_SECRET_KEY': 'test-secret',
            'STORAGE_PATH_STYLE_ACCESS': 'true',
            'STORAGE_CREDENTIALS_PROVIDER': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
            'MINIO_BUCKET': 'test-bucket' if storage_type == 'minio' else '',
            'S3_BUCKET': 'test-bucket' if storage_type == 's3' else ''
        })
        
        manager = StorageManager()
        backend = manager.storage_backend
        
        # Show common configurations
        common_configs = backend.get_common_spark_configs()
        logger.info(f"📋 Common Configurations for {backend_name}:")
        for key, value in common_configs.items():
            logger.info(f"   ✅ {key}: {value}")
        
        # Clean up
        for key in ['STORAGE_TYPE', 'STORAGE_ENDPOINT', 'STORAGE_ACCESS_KEY_ID', 
                   'STORAGE_SECRET_KEY', 'STORAGE_PATH_STYLE_ACCESS', 'STORAGE_CREDENTIALS_PROVIDER',
                   'MINIO_BUCKET', 'S3_BUCKET']:
            if key in os.environ:
                del os.environ[key]


def demonstrate_path_style_access():
    """
    Demonstrate different path style access configurations.
    """
    logger.info("\n=== Path Style Access Demo ===")
    
    path_style_options = [
        {'name': 'Path Style (true)', 'value': 'true'},
        {'name': 'Virtual Hosted Style (false)', 'value': 'false'},
        {'name': 'Not Specified (default)', 'value': None}
    ]
    
    for option in path_style_options:
        logger.info(f"\n🔄 Testing: {option['name']}")
        
        # Set environment
        os.environ.update({
            'STORAGE_TYPE': 'minio',
            'STORAGE_ENDPOINT': 'http://localhost:9000',
            'STORAGE_ACCESS_KEY_ID': 'test-key',
            'STORAGE_SECRET_KEY': 'test-secret',
            'MINIO_BUCKET': 'test-bucket'
        })
        
        if option['value']:
            os.environ['STORAGE_PATH_STYLE_ACCESS'] = option['value']
        
        manager = StorageManager()
        config = manager.config
        spark_configs = manager.get_spark_configs()
        
        logger.info(f"🔧 Config Path Style Access: {config.path_style_access}")
        
        has_path_style = 'spark.hadoop.fs.s3a.path.style.access' in spark_configs
        logger.info(f"🔧 Spark Config Path Style: {'✅ Set' if has_path_style else '❌ Not Set'}")
        
        if has_path_style:
            logger.info(f"🔧 Path Style Value: {spark_configs['spark.hadoop.fs.s3a.path.style.access']}")
        
        # Clean up
        for key in ['STORAGE_TYPE', 'STORAGE_ENDPOINT', 'STORAGE_ACCESS_KEY_ID', 
                   'STORAGE_SECRET_KEY', 'MINIO_BUCKET', 'STORAGE_PATH_STYLE_ACCESS']:
            if key in os.environ:
                del os.environ[key]


def show_inheritance_benefits():
    """
    Show the benefits of the improved inheritance structure.
    """
    logger.info("\n=== Inheritance Benefits ===")
    
    logger.info("✅ **Common Configurations Shared**")
    logger.info("   - spark.hadoop.fs.s3a.impl")
    logger.info("   - spark.hadoop.aws.region")
    logger.info("   - spark.hadoop.fs.s3a.path.style.access (when specified)")
    logger.info("   - spark.hadoop.fs.s3a.aws.credentials.provider (when specified)")
    
    logger.info("\n✅ **DRY Principle Applied**")
    logger.info("   - No code duplication between MinIO and S3 backends")
    logger.info("   - Common logic in parent class")
    logger.info("   - Specific logic in child classes")
    
    logger.info("\n✅ **Easy to Extend**")
    logger.info("   - Add new storage backend by extending StorageBackend")
    logger.info("   - Inherit common configurations automatically")
    logger.info("   - Only implement storage-specific configurations")
    
    logger.info("\n✅ **Configurable Path Style Access**")
    logger.info("   - Can be set via STORAGE_PATH_STYLE_ACCESS environment variable")
    logger.info("   - Defaults to true for MinIO, false for S3")
    logger.info("   - Can be overridden for any storage type")


if __name__ == "__main__":
    demonstrate_inheritance_structure()
    demonstrate_common_configurations()
    demonstrate_path_style_access()
    show_inheritance_benefits()
