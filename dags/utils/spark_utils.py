import json
import os
from abc import ABC, abstractmethod
from typing import Set, Dict, Callable, Any

import boto3
import yaml

from .config_manager import ConfigManager

class Type(ABC):
    @abstractmethod
    def type(self) -> str:
        pass

    def __eq__(self, other):
        return self.type() == other.type()

    def __hash__(self):
        return hash(self.type())

class _Basic(Type):
    def type(self) -> str:
        return "simple"

class _Storage(Type):
    def type(self) -> str:
        return "storage"

class _Catalog(Type):
    def type(self) -> str:
        return "catalog"

_basic = _Basic()
_catalog = _Catalog()
_storage = _Storage()

class ConfigTypes:
    @staticmethod
    def basic() -> Type:
        return _basic

    @staticmethod
    def storage() -> Type:
        return _storage

    @staticmethod
    def catalog() -> Type:
        return _catalog

class Overrider:
    def __init__(self, config_manager: ConfigManager, spark_template: str, config_types: Set[Type], load_dict_func: Callable[[str], dict[str, Any]]):
        self.config_manager = config_manager
        self.spark_template = spark_template
        self.configs = self._get_configs(config_types)
        self.load_dict_func = load_dict_func

    def _get_configs(self, types: Set[Type]) -> Dict[str, str]:
        conf = {}
        for tp in types:
            if tp == ConfigTypes.basic():
                conf.update(self.config_manager.get_spark_configs())
            elif tp == ConfigTypes.storage():
                conf.update(self.config_manager.get_storage_config())
            elif tp == ConfigTypes.catalog():
                conf.update(self.config_manager.get_catalog_configs())
        return conf

    @staticmethod
    def override_template_fields(spark_template: str, configs: dict, job_file_name: str, env_vars: dict, load_dict_func) -> dict:
        main_file = f"local:///opt/airflow/dags/spark/{job_file_name}"
        spark_image_versions = os.getenv("SPARK_IMAGE_VERSION", "latest")

        spec_dict = load_dict_func(spark_template)

        hadoop_conf = {}
        spark_conf = {}
        for key, value in configs.items():
            if key.startswith("spark.hadoop."):
                hadoop_conf[key.replace("spark.hadoop.", "")] = value
            else:
                spark_conf[key] = value

        spec_dict["sparkConf"] = spark_conf
        spec_dict["hadoopConf"] = hadoop_conf
        spec_dict["mainApplicationFile"] = main_file
        spec_dict["image"] = f"py-spark-spark:{spark_image_versions}"
        spec_dict["driver"]["envVars"] = env_vars

        return {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "spec": spec_dict
        }

    def override_with(self, job_file_name: str, env_vars: dict) -> dict:
        return Overrider.override_template_fields(self.spark_template, self.configs, job_file_name, env_vars, self.load_dict_func)

def load_template(config_manager: ConfigManager, config_types: Set[Type] = None, bucket: str = "spark", key: str = "templates/spark_operator_spec.json"):
    """
    Loads template from s3
    :param config_manager: config manager that contains all configs include data to connect to s3 to download template
    :param config_types: tuple of configuration to insert into template spec. One or more between: Type.basic(), Type.storage(), Type.catalog()
    :param bucket: bucket name where spark spec template is stored
    :param key: path in bucket where spark spec template is stored
    :return: Overrider that has only one public method: override_with
    """
    tps = config_types if config_types is not None else (ConfigTypes.basic(), ConfigTypes.storage(), ConfigTypes.catalog())
    s3_client = boto3.client('s3',
         region_name=config_manager.storage_config.region,
         use_ssl=config_manager.storage_config.ssl_enabled,
         endpoint_url=config_manager.storage_config.endpoint,
         aws_access_key_id=config_manager.storage_config.access_key,
         aws_secret_access_key=config_manager.storage_config.secret_key,
     )
    s3_obj = s3_client.get_object(Bucket=bucket, Key=key)

    if key.lower().endswith("yaml") or key.lower().endswith("yml"):
        load_template_function = lambda content: yaml.safe_load(content)
    elif key.lower().endswith("json"):
        load_template_function = lambda content: json.loads(content)
    else:
        raise ValueError(f"Unsupported template format: nor 'json' neither 'yaml'")

    return Overrider(config_manager, s3_obj['Body'].read(), tps, load_template_function)


