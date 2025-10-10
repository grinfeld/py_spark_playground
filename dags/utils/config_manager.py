"""
Storage configuration abstraction layer using inheritance and polymorphism.

This module provides a unified interface for different storage backends
(MinIO and AWS S3) and catalog types (Hive, Glue, S3) using proper OOP.
"""

import os
import logging
from typing import Dict, Optional, Set
from dataclasses import dataclass
from abc import ABC

logger = logging.getLogger(__name__)

@dataclass
class StorageConfig:
    """Configuration for storage backend."""
    endpoint: Optional[str]
    ssl_enabled: Optional[bool]
    access_key: Optional[str]
    secret_key: Optional[str]
    bucket: Optional[str]
    region: Optional[str]
    path_style_access: Optional[bool]
    credentials_provider: Optional[str]

@dataclass
class CatalogConfig:
    catalog_type: str
    catalog_name: str
    warehouse_path: str
    io_impl: Optional[str]
    warehouse_dir: Optional[str]
    empty: bool = False

class StorageBackend:
    """Storage backend implementation."""
    
    def __init__(self, config: StorageConfig):
        self.config = config
    
    def get_spark_storage_config(self) -> Dict[str, str]:
        """Get common Spark configuration shared by all storage backends."""
        configs = {
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        }

        if self.config.region:
            configs["spark.hadoop.aws.region"] = self.config.region
        
        # Add endpoint if specified
        if self.config.endpoint:
            configs["spark.hadoop.fs.s3a.endpoint"] = self.config.endpoint
        
        # Add credentials if specified
        if self.config.access_key and self.config.secret_key:
            configs.update({
                "spark.hadoop.fs.s3a.access.key": self.config.access_key,
                "spark.hadoop.fs.s3a.secret.key": self.config.secret_key
            })
        
        # Add ssl enabled if specified
        if self.config.path_style_access is not None:
            configs["spark.hadoop.fs.s3a.path.style.access"] = str(self.config.path_style_access).lower()

        # Add path style access if specified
        if self.config.ssl_enabled is not None:
            configs["spark.hadoop.fs.s3a.connection.ssl.enabled"] = str(self.config.ssl_enabled).lower()
        
        # Add credentials provider if specified
        if self.config.credentials_provider:
            configs["spark.hadoop.fs.s3a.aws.credentials.provider"] = self.config.credentials_provider

        # "spark.executorEnv.AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
        # "spark.executorEnv.AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,

        return configs

class IcebergCatalogBackend(ABC):
    """Abstract base class for catalog backends."""
    
    def __init__(self, storage_config: StorageConfig, catalog_config: CatalogConfig):
        self.storage_config = storage_config
        self.catalog_config = catalog_config

    def _format_with_name(self, name: str = "") -> str:
        if name == "":
            return f"spark.sql.catalog.{self.catalog_config.catalog_name}"
        else:
            return f"spark.sql.catalog.{self.catalog_config.catalog_name}.{name}"

    def get_common_catalog_configs(self) -> Dict[str, str]:
        """Get common catalog configuration shared by all catalog backends."""
        configs = {
            self._format_with_name("warehouse"): self.catalog_config.warehouse_path,
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.sql.defaultCatalog": self.catalog_config.catalog_name
        }

        if self.catalog_config.warehouse_dir is not None:
            # hive shouldn't have it
            configs[f"spark.sql.warehouse.dir"] = self.catalog_config.warehouse_dir

        # Add io-impl if specified (common for S3-based catalogs)
        if self.catalog_config.io_impl is not None:
            configs[self._format_with_name("io-impl")] =  self.catalog_config.io_impl
        
        return configs
    
    def _get_warehouse_dir(self) -> Optional[str]:
        return self.catalog_config.warehouse_dir

    def get_catalog_configs(self) -> Dict[str, str]:
        """Get catalog configuration."""
        return self.get_common_catalog_configs()

class EmptyCatalog(IcebergCatalogBackend):

    def __init__(self):
        super().__init__(StorageConfig(None, None, None, None, None, None, None), CatalogConfig("", "", "", None, None, True))

    def get_common_catalog_configs(self):
        return {}

    def get_catalog_configs(self):
        return {}


class HiveCatalog(IcebergCatalogBackend):
    """Hive catalog implementation."""

    def __init__(self, storage_config: StorageConfig, catalog_config: CatalogConfig):
        super().__init__(storage_config, catalog_config)
        self.uri = os.getenv("HIVE_CATALOG_URI")

    def get_catalog_configs(self) -> Dict[str, str]:
        """Get Hive catalog configuration."""
        # Get common configurations
        configs = self.get_common_catalog_configs()
        
        # Add Hive-specific configurations
        configs.update({
            self._format_with_name(): "org.apache.iceberg.spark.SparkCatalog",
            self._format_with_name("type"): "hive"
        })

        if self.catalog_config.warehouse_path is not None:
            configs[self._format_with_name("warehouse")] = self.catalog_config.warehouse_path
        
        return configs

    def _get_warehouse_dir(self) -> Optional[str]:
        return None

class WithS3Catalog(IcebergCatalogBackend):
    def _s3_config(self):
        configs = {}
        # Add storage-specific configurations for MinIO
        if self.storage_config.endpoint:  # If endpoint is set, it's MinIO
            configs[self._format_with_name("s3.endpoint")] = self.storage_config.endpoint

        if self.storage_config.access_key:
            configs["spark.executorEnv.AWS_ACCESS_KEY_ID"] = self.storage_config.access_key

        if self.storage_config.secret_key:
            configs["spark.executorEnv.AWS_SECRET_ACCESS_KEY"] = self.storage_config.secret_key

        # Add ssl enabled if specified
        if self.storage_config.path_style_access is not None:
            configs[self._format_with_name("s3.path-style-access")] = str(self.storage_config.path_style_access).lower()

        return configs

class GlueCatalog(WithS3Catalog):
    """AWS Glue catalog implementation."""
    
    def get_catalog_configs(self) -> Dict[str, str]:
        """Get Glue catalog configuration."""
        # Get common configurations
        configs = self.get_common_catalog_configs()
        configs.update(super()._s3_config())
        # Add Glue-specific configurations
        configs.update({
            self._format_with_name(): "org.apache.iceberg.spark.SparkCatalog",
            self._format_with_name("type"): "glue"
        })

        if self.storage_config.region is not None:
            configs[self._format_with_name("glue.region")] = self.storage_config.region

        # glue works with s3 and not with s3a protocol
        if self.catalog_config.warehouse_path is not None:
            sub = str(self.catalog_config.warehouse_path)
            if sub.startswith("s3a://"):
                configs[self._format_with_name("warehouse")] = f"s3://{sub[6:]}"

        return configs

# catalog implementation for hdfs or s3 compatible storage (e.g., s3 or minIO)
class HadoopCatalog(WithS3Catalog):
    def get_catalog_configs(self) -> Dict[str, str]:
        """Get S3/Hadoop catalog configuration."""
        # Get common configurations
        configs = self.get_common_catalog_configs()
        configs.update(super()._s3_config())

        # Add S3/Hadoop-specific configurations
        configs.update({
            self._format_with_name(): "org.apache.iceberg.spark.SparkCatalog",
            self._format_with_name("type"): "hadoop",
        })

        return configs

class ConfigManager:
    """Manages storage and catalog configuration using polymorphism."""
    
    def __init__(self):
        self.storage_config = self._load_storage_config()
        self.catalog_config = self._load_catalog_config()
        self.storage_backend = self._create_storage_backend(self.storage_config)
        self.catalog_backend = self._create_catalog_backend(self.catalog_config, self.storage_config)

    def _load_catalog_config(self) -> CatalogConfig:
        catalog_type = os.getenv("CATALOG_TYPE")
        if catalog_type is None:
            logger.info("Catalog type should not be empty. No catalog should be used in script")
            return CatalogConfig(empty=True, catalog_type="", catalog_name="", warehouse_path="", io_impl=None, warehouse_dir=None)
        return CatalogConfig(
            catalog_type=catalog_type,
            catalog_name=os.getenv("CATALOG_WAREHOUSE_NAME"),
            warehouse_path=os.getenv("CATALOG_WAREHOUSE_PATH"),
            io_impl=os.getenv("CATALOG_IO_IMPL"),
            warehouse_dir=os.getenv("CATALOG_WAREHOUSE_DIR"),
            empty=False
        )

    def _load_storage_config(self) -> StorageConfig:
        """Load storage configuration from environment variables."""
        # Common validation - bucket and catalog type are required
        bucket_env = os.getenv('STORAGE_BUCKET')
        if not bucket_env:
            raise ValueError("STORAGE_BUCKET must be set for storage configuration")

        # Parse storage ssl enabled
        storage_ssl_enabled_str = os.getenv('STORAGE_ENDPOINT_SSL_ENABLE')
        storage_ssl_enabled = None
        if storage_ssl_enabled_str:
            storage_ssl_enabled = storage_ssl_enabled_str.lower() in ('true', '1', 'yes', 'on')

        # Parse path style access
        path_style_access_str = os.getenv('STORAGE_PATH_STYLE_ACCESS')
        path_style_access = None
        if path_style_access_str:
            path_style_access = path_style_access_str.lower() in ('true', '1', 'yes', 'on')

        return StorageConfig(
            endpoint=os.getenv('STORAGE_ENDPOINT'),
            access_key=os.getenv('STORAGE_ACCESS_KEY_ID'),
            secret_key=os.getenv('STORAGE_SECRET_KEY'),
            bucket=bucket_env,
            region=os.getenv('AWS_REGION', 'us-east-1'),
            path_style_access=path_style_access,
            credentials_provider=os.getenv('STORAGE_CREDENTIALS_PROVIDER'),
            ssl_enabled=storage_ssl_enabled
        )

    def _create_storage_backend(self, config: StorageConfig) -> StorageBackend:
        """Create storage backend based on configuration."""
        return StorageBackend(config)
    
    def _create_catalog_backend(self, catalog_config: CatalogConfig, storage_config: StorageConfig) -> IcebergCatalogBackend:
        """Create appropriate catalog backend based on configuration."""
        if catalog_config.empty:
            return EmptyCatalog()
        type = catalog_config.catalog_type
        if type == 'hive':
            return HiveCatalog(storage_config, catalog_config)
        elif type == 'glue':
            return GlueCatalog(storage_config, catalog_config)
        elif type == 'hadoop':
            return HadoopCatalog(storage_config, catalog_config)
        else:
            raise ValueError(f"Unknown catalog type: {type}. Supported types: 'hive', 'glue', 's3', 'hadoop'")

    def get_spark_configs(self) -> Dict[str, str]:
        configs = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.sql.adaptive.optimizeSkewedJoin.enabled": "true",
            "spark.sql.adaptive.forceApply": "true"
        }

        if os.getenv("SPARK_MASTER_URL") is not None:
            configs["spark.master"] = os.getenv("SPARK_MASTER_URL")

        return configs

    def get_catalog_configs(self) -> Dict[str, str]:
        """Get catalog configuration using polymorphic catalog backend."""
        return self.catalog_backend.get_catalog_configs()
    
    def get_storage_config(self) -> Dict[str, str]:
        """Get warehouse paths for the current storage backend."""
        return self.storage_backend.get_spark_storage_config()

    def get_all_configs(self) -> Dict[str, str]:
        return {**self.get_spark_configs(), **self.get_catalog_configs(), **self.get_storage_config()}

# Global config manager instance
config_manager = ConfigManager()


