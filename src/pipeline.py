import logging
from typing import Dict

from pyspark.sql import DataFrame

from src.extract.JdbcExtractor import JDBCExtractor
from src.types import Config


class Pipeline:
    def __init(self,config : Config):
        self.config=config
        self.logger=logging

    def extract(self, dataframe: Dict[str,DataFrame])->None:
        for config in self.config.inputs:
            dataframe[config.name]=JDBCExtractor.from_config(config).extract(config.uri,self.logger)

    def transform(self, dataframe: Dict[str,DataFrame])->None:
        pass

    def validate(self, dataframe: Dict[str,DataFrame])->None:
        pass

    def load(self, dataframe: Dict[str,DataFrame])->None:
        pass

    def run(self):
        data:Dict[str,DataFrame]={}
        self.extract(data)
        self.transform(data)
        self.validate(data)
        self.load(data)
