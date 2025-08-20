"""
Storage configuration abstraction layer using inheritance and polymorphism.

This module provides a unified interface for different storage backends
(MinIO and AWS S3) and catalog types (Hive, Glue, S3) using proper OOP.
"""

import os
import logging
from typing import Dict, Optional
from dataclasses import dataclass
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

@dataclass
class StorageConfig:
    """Configuration for storage backend."""
    endpoint: Optional[str]
    access_key: Optional[str]
    secret_key: Optional[str]
    bucket: str
    region: str
    path_style_access: Optional[bool]
    credentials_provider: Optional[str]
    catalog_type: str  # 'hive', 'glue', or 'hadoop'


class StorageBackend:
    """Storage backend implementation."""
    
    def __init__(self, config: StorageConfig):
        self.config = config
    
    def get_common_spark_configs(self) -> Dict[str, str]:
        """Get common Spark configuration shared by all storage backends."""
        configs = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.aws.region": self.config.region,
        }
        
        # Add endpoint if specified
        if self.config.endpoint:
            configs["spark.hadoop.fs.s3a.endpoint"] = self.config.endpoint
        
        # Add credentials if specified
        if self.config.access_key and self.config.secret_key:
            configs.update({
                "spark.hadoop.fs.s3a.access.key": self.config.access_key,
                "spark.hadoop.fs.s3a.secret.key": self.config.secret_key,
            })
        
        # Add path style access if specified
        if self.config.path_style_access is not None:
            configs["spark.hadoop.fs.s3a.path.style.access"] = str(self.config.path_style_access).lower()
        
        # Add credentials provider if specified
        if self.config.credentials_provider:
            configs["spark.hadoop.fs.s3a.aws.credentials.provider"] = self.config.credentials_provider
        
        return configs
    
    def get_spark_configs(self) -> Dict[str, str]:
        """Get Spark configuration for the storage backend."""
        return self.get_common_spark_configs()


class CatalogBackend(ABC):
    """Abstract base class for catalog backends."""
    
    def __init__(self, config: StorageConfig):
        self.config = config
    
    def get_common_catalog_configs(self, catalog_name: str) -> Dict[str, str]:
        """Get common catalog configuration shared by all catalog backends."""
        configs = {
            f"spark.sql.catalog.{catalog_name}.warehouse": self._get_warehouse_path(),
        }
        
        # Add io-impl if specified (common for S3-based catalogs)
        io_impl = self._get_io_impl()
        if io_impl:
            configs[f"spark.sql.catalog.{catalog_name}.io-impl"] = io_impl
        
        return configs
    
    def _get_warehouse_path(self) -> str:
        """Get warehouse path using CATALOG_WAREHOUSE_NAME."""
        warehouse_name = os.getenv('CATALOG_WAREHOUSE_NAME')
        if not warehouse_name:
            raise ValueError("CATALOG_WAREHOUSE_NAME must be set for warehouse configuration")
        return f"s3a://{self.config.bucket}/{warehouse_name}"
    
    def _get_io_impl(self) -> Optional[str]:
        """Get IO implementation class. Read from environment or use default."""
        io_impl = os.getenv('CATALOG_IO_IMPL')
        if io_impl:
            return io_impl
        return None
    
    @abstractmethod
    def get_catalog_configs(self, catalog_name: str) -> Dict[str, str]:
        """Get catalog configuration."""
        pass


class HiveCatalog(CatalogBackend):
    """Hive catalog implementation."""
    
    def get_catalog_configs(self, catalog_name: str) -> Dict[str, str]:
        """Get Hive catalog configuration."""
        # Get common configurations
        configs = self.get_common_catalog_configs(catalog_name)
        
        # Add Hive-specific configurations
        configs.update({
            f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{catalog_name}.type": "hive",
            f"spark.sql.catalog.{catalog_name}.uri": "thrift://localhost:9083",
        })
        
        return configs


class GlueCatalog(CatalogBackend):
    """AWS Glue catalog implementation."""
    
    def get_catalog_configs(self, catalog_name: str) -> Dict[str, str]:
        """Get Glue catalog configuration."""
        # Get common configurations
        configs = self.get_common_catalog_configs(catalog_name)
        
        # Add Glue-specific configurations
        configs.update({
            f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.aws.glue.GlueCatalog",
        })
        
        # Add storage-specific configurations for MinIO
        if self.config.endpoint:  # If endpoint is set, it's MinIO
            configs.update({
                f"spark.sql.catalog.{catalog_name}.s3.endpoint": self.config.endpoint,
                f"spark.sql.catalog.{catalog_name}.s3.access-key": self.config.access_key,
                f"spark.sql.catalog.{catalog_name}.s3.secret-key": self.config.secret_key,
            })
        
        return configs


class S3Catalog(CatalogBackend):
    """S3/Hadoop catalog implementation."""
    
    def get_catalog_configs(self, catalog_name: str) -> Dict[str, str]:
        """Get S3/Hadoop catalog configuration."""
        # Get common configurations
        configs = self.get_common_catalog_configs(catalog_name)
        
        # Add S3/Hadoop-specific configurations
        configs.update({
            f"spark.sql.catalog.{catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{catalog_name}.type": "hadoop",
        })
        
        # Add storage-specific configurations
        if self.config.endpoint:  # If endpoint is set, it's MinIO
            configs.update({
                f"spark.sql.catalog.{catalog_name}.s3.endpoint": self.config.endpoint,
                f"spark.sql.catalog.{catalog_name}.s3.access-key": self.config.access_key,
                f"spark.sql.catalog.{catalog_name}.s3.secret-key": self.config.secret_key,
            })
        else:  # If no endpoint, it's AWS S3
            configs.update({
                f"spark.sql.catalog.{catalog_name}.s3.region": self.config.region,
            })
        
        return configs


class ConfigManager:
    """Manages storage and catalog configuration using polymorphism."""
    
    def __init__(self):
        self.config = self._load_config()
        self.storage_backend = self._create_storage_backend()
        self.catalog_backend = self._create_catalog_backend()
    
    def _load_config(self) -> StorageConfig:
        """Load storage configuration from environment variables."""
        # Common validation - bucket and catalog type are required
        bucket = os.getenv('STORAGE_BUCKET')
        if not bucket:
            raise ValueError("STORAGE_BUCKET must be set for storage configuration")
        
        catalog_type = os.getenv('CATALOG_TYPE')
        if not catalog_type:
            raise ValueError("CATALOG_TYPE must be set for storage configuration")
        
        # Parse path style access
        path_style_access_str = os.getenv('STORAGE_PATH_STYLE_ACCESS')
        path_style_access = None
        if path_style_access_str:
            path_style_access = path_style_access_str.lower() in ('true', '1', 'yes', 'on')
        
        return StorageConfig(
            endpoint=os.getenv('STORAGE_ENDPOINT'),
            access_key=os.getenv('STORAGE_ACCESS_KEY_ID'),
            secret_key=os.getenv('STORAGE_SECRET_KEY'),
            bucket=bucket,
            region=os.getenv('AWS_REGION', 'us-east-1'),
            path_style_access=path_style_access,
            credentials_provider=os.getenv('STORAGE_CREDENTIALS_PROVIDER'),
            catalog_type=catalog_type.lower()
        )
    
    def _create_storage_backend(self) -> StorageBackend:
        """Create storage backend based on configuration."""
        return StorageBackend(self.config)
    
    def _create_catalog_backend(self) -> CatalogBackend:
        """Create appropriate catalog backend based on configuration."""
        if self.config.catalog_type == 'hive':
            return HiveCatalog(self.config)
        elif self.config.catalog_type == 'glue':
            return GlueCatalog(self.config)
        elif self.config.catalog_type == 's3':
            return S3Catalog(self.config)
        elif self.config.catalog_type == 'hadoop':
            return S3Catalog(self.config)  # Use S3Catalog for Hadoop type
        else:
            raise ValueError(f"Unknown catalog type: {self.config.catalog_type}. Supported types: 'hive', 'glue', 's3', 'hadoop'")
    
    def get_spark_configs(self) -> Dict[str, str]:
        """Get Spark configuration using polymorphic storage backend."""
        return self.storage_backend.get_spark_configs()
    
    def get_catalog_configs(self, catalog_name: str = "spark_catalog") -> Dict[str, str]:
        """Get catalog configuration using polymorphic catalog backend."""
        return self.catalog_backend.get_catalog_configs(catalog_name)
    
    def get_warehouse_paths(self) -> Dict[str, str]:
        """Get warehouse paths for the current storage backend."""
        return {
            "warehouse": f"s3a://{self.config.bucket}/warehouse",
            "iceberg_warehouse": f"s3a://{self.config.bucket}/iceberg-warehouse",
            "glue_warehouse": f"s3a://{self.config.bucket}/glue-warehouse"
        }
    
    def get_data_paths(self, filename: str) -> Dict[str, str]:
        """Get data file paths for the current storage backend."""
        base_path = f"s3a://{self.config.bucket}"
        return {
            "parquet": f"{base_path}/{filename}.parquet",
            "csv": f"{base_path}/{filename}.csv",
            "iceberg_table": f"spark_catalog.default.{filename}_iceberg",
            "glue_table": f"glue_catalog.default.{filename}_glue"
        }


# Global config manager instance
config_manager = ConfigManager()


