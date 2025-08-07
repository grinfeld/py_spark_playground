"""
Demonstration of unified environment variables for storage configuration.

This script shows how the new unified environment variables work
across different storage types and configurations.
"""

import os
import logging
from config_manager import ConfigManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def demonstrate_unified_environment():
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
                'STORAGE_CREDENTIALS_PROVIDER': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
                'MINIO_BUCKET': 'spark-data'
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
                'S3_BUCKET': 'my-spark-bucket'
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
            'name': 'S3-Compatible Service with Custom Endpoint',
            'env': {
                'STORAGE_TYPE': 's3',
                'STORAGE_ENDPOINT': 'https://custom-s3-endpoint.com',
                'STORAGE_ACCESS_KEY_ID': 'custom-key',
                'STORAGE_SECRET_KEY': 'custom-secret',
                'S3_BUCKET': 'my-bucket'
            }
        },
        {
            'name': 'AWS S3 with Default Credentials (No Provider)',
            'env': {
                'STORAGE_TYPE': 's3',
                'S3_BUCKET': 'my-spark-bucket'
                # No credentials provider - will use Spark defaults
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
        
        # Show configuration
        show_configuration(manager, scenario['name'])
        
        # Clean up environment
        for key in scenario['env'].keys():
            if key in os.environ:
                del os.environ[key]


def show_configuration(manager, scenario_name):
    """
    Show the configuration details for a scenario.
    """
    config = manager.config
    info = manager.get_storage_info()
    
    logger.info(f"📦 Storage Type: {info['type']}")
    logger.info(f"🔗 Endpoint: {config.endpoint or 'Default'}")
    logger.info(f"🔑 Access Key: {config.access_key or 'Not Set'}")
    logger.info(f"🔐 Secret Key: {'***' if config.secret_key else 'Not Set'}")
    logger.info(f"🏪 Bucket: {config.bucket}")
    logger.info(f"🌍 Region: {config.region}")
    logger.info(f"🔧 Credentials Provider: {config.credentials_provider or 'Spark Default'}")
    logger.info(f"📚 Catalog Type: {config.catalog_type}")
    
    # Show Spark configurations
    spark_configs = manager.get_spark_configs()
    logger.info(f"⚙️  Spark Configs: {len(spark_configs)} items")
    
    # Show catalog configurations
    catalog_configs = manager.get_catalog_configs()
    logger.info(f"📚 Catalog Configs: {len(catalog_configs)} items")


def demonstrate_backward_compatibility():
    """
    Demonstrate backward compatibility with legacy environment variables.
    """
    logger.info("\n=== Backward Compatibility Demo ===")
    
    # Test legacy MinIO variables
    logger.info("🔄 Testing legacy MinIO variables...")
    os.environ.update({
        'STORAGE_TYPE': 'minio',
        'MINIO_ENDPOINT': 'http://legacy-minio:9000',
        'MINIO_ACCESS_KEY': 'legacy-key',
        'MINIO_SECRET_KEY': 'legacy-secret',
        'MINIO_BUCKET': 'legacy-bucket'
    })
    
    manager = StorageManager()
    logger.info(f"✅ Legacy MinIO config loaded: {manager.config.endpoint}")
    
    # Test legacy AWS variables
    logger.info("🔄 Testing legacy AWS variables...")
    os.environ.update({
        'STORAGE_TYPE': 's3',
        'AWS_ACCESS_KEY_ID': 'legacy-aws-key',
        'AWS_SECRET_ACCESS_KEY': 'legacy-aws-secret',
        'S3_BUCKET': 'legacy-s3-bucket'
    })
    
    manager = StorageManager()
    logger.info(f"✅ Legacy AWS config loaded: {manager.config.access_key}")
    
    # Clean up
    for key in ['STORAGE_TYPE', 'MINIO_ENDPOINT', 'MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY', 
                'MINIO_BUCKET', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET']:
        if key in os.environ:
            del os.environ[key]


def demonstrate_credentials_provider():
    """
    Demonstrate different credentials provider configurations.
    """
    logger.info("\n=== Credentials Provider Demo ===")
    
    providers = [
        {
            'name': 'SimpleAWSCredentialsProvider',
            'provider': 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'
        },
        {
            'name': 'DefaultAWSCredentialsProviderChain',
            'provider': 'org.apache.hadoop.fs.s3a.DefaultAWSCredentialsProviderChain'
        },
        {
            'name': 'EnvironmentVariableCredentialsProvider',
            'provider': 'org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider'
        },
        {
            'name': 'No Provider (Spark Default)',
            'provider': None
        }
    ]
    
    for provider_info in providers:
        logger.info(f"\n🔄 Testing: {provider_info['name']}")
        
        # Set environment
        os.environ.update({
            'STORAGE_TYPE': 'minio',
            'STORAGE_ENDPOINT': 'http://localhost:9000',
            'STORAGE_ACCESS_KEY_ID': 'test-key',
            'STORAGE_SECRET_KEY': 'test-secret',
            'MINIO_BUCKET': 'test-bucket'
        })
        
        if provider_info['provider']:
            os.environ['STORAGE_CREDENTIALS_PROVIDER'] = provider_info['provider']
        
        manager = StorageManager()
        spark_configs = manager.get_spark_configs()
        
        has_provider = 'spark.hadoop.fs.s3a.aws.credentials.provider' in spark_configs
        logger.info(f"🔧 Credentials Provider Set: {'✅ Yes' if has_provider else '❌ No'}")
        
        if has_provider:
            logger.info(f"🔧 Provider: {spark_configs['spark.hadoop.fs.s3a.aws.credentials.provider']}")
        
        # Clean up
        for key in ['STORAGE_TYPE', 'STORAGE_ENDPOINT', 'STORAGE_ACCESS_KEY_ID', 
                   'STORAGE_SECRET_KEY', 'MINIO_BUCKET', 'STORAGE_CREDENTIALS_PROVIDER']:
            if key in os.environ:
                del os.environ[key]


if __name__ == "__main__":
    demonstrate_unified_environment()
    demonstrate_backward_compatibility()
    demonstrate_credentials_provider()
