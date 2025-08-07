"""
Demonstration of polymorphic storage and catalog management.

This script shows how clean and extensible the code becomes when using
inheritance and polymorphism instead of if statements.
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg
import logging
from config_manager import config_manager, StorageBackend, CatalogBackend

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def demonstrate_polymorphism():
    """
    Demonstrate the polymorphic approach with different storage and catalog combinations.
    """
    logger.info("=== Polymorphic Storage and Catalog Management ===")
    
    # Test different combinations
    combinations = [
        {'storage': 'minio', 'catalog': 'hive'},
        {'storage': 'minio', 'catalog': 'glue'},
        {'storage': 'minio', 'catalog': 's3'},
        {'storage': 's3', 'catalog': 'glue'},
        {'storage': 's3', 'catalog': 's3'},
    ]
    
    for combo in combinations:
        logger.info(f"\n🔄 Testing {combo['storage'].upper()} + {combo['catalog'].upper()} Catalog")
        
        # Set environment variables
        os.environ['STORAGE_TYPE'] = combo['storage']
        os.environ['CATALOG_TYPE'] = combo['catalog']
        
        # Create new storage manager (will use polymorphism)
        from config_manager import ConfigManager
        manager = ConfigManager()
        
        # Show polymorphic behavior
        show_polymorphic_behavior(manager, combo)


def show_polymorphic_behavior(manager, combo):
    """
    Show how polymorphism works with different backends.
    """
    # 1. Show storage backend type (polymorphism in action)
    storage_backend = manager.storage_backend
    catalog_backend = manager.catalog_backend
    
    logger.info(f"📦 Storage Backend: {type(storage_backend).__name__}")
    logger.info(f"📚 Catalog Backend: {type(catalog_backend).__name__}")
    
    # 2. Get configurations (same interface, different implementations)
    spark_configs = manager.get_spark_configs()
    catalog_configs = manager.get_catalog_configs("spark_catalog")
    
    logger.info(f"⚙️  Spark Configs: {len(spark_configs)} items")
    logger.info(f"⚙️  Catalog Configs: {len(catalog_configs)} items")
    
    # 3. Show storage info
    info = manager.get_storage_info()
    logger.info(f"ℹ️  Storage Type: {info['type']}")
    logger.info(f"ℹ️  Catalog Type: {info['catalog_type']}")
    logger.info(f"ℹ️  Bucket: {info['bucket']}")


def demonstrate_extensibility():
    """
    Demonstrate how easy it is to add new storage or catalog backends.
    """
    logger.info("\n=== Extensibility Demonstration ===")
    
    # Show how to add a new storage backend
    logger.info("📝 To add a new storage backend:")
    logger.info("   1. Create class extending StorageBackend (if needed)")
    logger.info("   2. Override get_spark_configs() for custom logic")
    logger.info("   3. Update _create_storage_backend() in StorageManager")
    logger.info("   Note: Currently using single StorageBackend for all types")
    
    # Show how to add a new catalog backend
    logger.info("\n📝 To add a new catalog backend:")
    logger.info("   1. Create class extending CatalogBackend")
    logger.info("   2. Implement get_catalog_configs()")
    logger.info("   3. Add to _create_catalog_backend() in StorageManager")
    
    # Example of what a new backend would look like
    logger.info("\n📝 Example new backend structure:")
    logger.info("""
class NewStorageBackend(StorageBackend):
    def get_spark_configs(self) -> Dict[str, str]:
        return {"custom.config": "value"}
    """)


def demonstrate_clean_code():
    """
    Demonstrate how clean the code is without if statements.
    """
    logger.info("\n=== Clean Code Demonstration ===")
    
    # The StorageManager interface is completely clean
    logger.info("✅ StorageManager.get_spark_configs() - No if statements!")
    logger.info("✅ StorageManager.get_catalog_configs() - No if statements!")
    
    # All logic is encapsulated in the appropriate backend classes
    logger.info("\n🔧 All storage-specific logic is in:")
    logger.info("   - StorageBackend.get_spark_configs() (unified)")
    logger.info("   - HiveCatalog.get_catalog_configs()")
    logger.info("   - GlueCatalog.get_catalog_configs()")
    logger.info("   - S3Catalog.get_catalog_configs()")


def show_backend_hierarchy():
    """
    Show the class hierarchy and polymorphism.
    """
    logger.info("\n=== Class Hierarchy ===")
    
    logger.info("📦 StorageBackend (Concrete Implementation)")
    logger.info("   └── Unified for MinIO and S3")
    
    logger.info("\n📚 CatalogBackend (Abstract Base Class)")
    logger.info("   ├── HiveCatalog")
    logger.info("   ├── GlueCatalog")
    logger.info("   └── S3Catalog")
    
    logger.info("\n🎯 StorageManager (Composition)")
    logger.info("   ├── storage_backend: StorageBackend")
    logger.info("   └── catalog_backend: CatalogBackend")


if __name__ == "__main__":
    show_backend_hierarchy()
    demonstrate_clean_code()
    demonstrate_extensibility()
    demonstrate_polymorphism()
