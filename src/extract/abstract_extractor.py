from abc import ABCMeta, abstractmethod
from typing import Union,List

from pyspark.sql import DataFrame as SDF

from src.types import InputConfig


class AbstractExtractor(metaclass=ABCMeta):
    @abstractmethod
    def extract(self, uri: Union[str, List[str]], spark, logger) -> SDF:
        pass

    @staticmethod
    def from_config(config: InputConfig) -> AbstractExtractor:
        if config.type == "file":
            return Extractor.from_config(config)
        if config.type == "jdbc":
            return JdbcExtractor.from_config(config)
        raise ValueError(f"Extractor type {config.type} not supported")