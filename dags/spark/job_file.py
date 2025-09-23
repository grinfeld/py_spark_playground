import argparse
import logging
import os
from abc import abstractmethod, ABC

from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

class RequiredArgumentError(Exception):
    """Error informing that a required argument is missing"""
    pass

class ParamExtractor(ABC):
    @abstractmethod
    def get_param(self, name: str, default: str = None, required: bool = False) -> str:
        pass

class ArgParserParamExtractor(ParamExtractor):
    """
    Extract parameters from command line arguments
    You should register all arguments in the parser before sending to this class,
    since the first line it does parser.parse_args() and then works with arguments that have benn registered until this moment.
    """
    def __init__(self, parser: argparse.ArgumentParser, dash_to_underscore: bool = True):
        self._dash_to_underscore = dash_to_underscore
        self.args = parser.parse_args().__dict__

    def get_param(self, name: str, default: str = None, required: bool = False) -> str:
        nname = name.replace("-", "_") if self._dash_to_underscore else name
        val = self.args.get(nname) if nname in self.args else None
        if required and val is None:
            raise RequiredArgumentError(f"{nname} is required")
        return val if val is not None else default

    def __repr__(self):
        sb = "{\n"
        for key, val in self.args.items():
            sb += f"\t{key}: {val},\n"
        sb += "}"
        return sb

class EnvVarParamExtractor(ParamExtractor):
    """
    Extract parameters from environment variables
    Converts names to upper case if you use_upper_names flag is True
    Converts dahses to underscores if _dash_to_underscore flag is True
    """
    def __init__(self, use_upper_names: bool = True, dash_to_underscore: bool = True):
        self._use_upper_names = use_upper_names
        self._dash_to_underscore = dash_to_underscore

    def get_param(self, name: str, default: str = None, required: bool = False) -> str:
        nname = name.upper() if self._use_upper_names else name
        nname = nname.replace("-", "_") if self._dash_to_underscore else nname

        val = os.getenv(nname, default)
        if required and val is None:
            raise RequiredArgumentError(f"{nname} is required")
        return val

class BothEnvAndArgsExtractor(ParamExtractor):
    def __init__(self, arg_extractor: ArgParserParamExtractor, env_extractor: EnvVarParamExtractor):
        self.arg_extractor = arg_extractor
        self.env_extractor = env_extractor

    def get_param(self, name: str, default: str = None, required: bool = False) -> str:
        val = self.arg_extractor.get_param(name, default=default, required=False)
        val = val if val is not None else self.env_extractor.get_param(name, default=default, required=False)
        val = val if val is not None else default
        if required and val is None:
            raise RequiredArgumentError(f"{name} is required")
        return val


class PythonSparkJob(ABC):

    def __init__(self, parser: ParamExtractor, spark_conf: dict = None):
        self.job_name = parser.get_param("spark-job-name", required=False)
        self.master = parser.get_param("spark-master-url", required=False, default=None)
        self.spark_conf = {} if spark_conf is None else spark_conf

    def create_spark_session(self) -> SparkSession:
        builder = SparkSession.builder.appName(self.job_name)
        builder = builder if self.master is None or self.master == "" else builder.master(self.master)
        for k, v in self.spark_conf.items():
            builder.config(k, v)
        return builder.getOrCreate()

    @abstractmethod
    def run(self):
        pass