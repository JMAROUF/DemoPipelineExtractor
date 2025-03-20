from src.extract.abstract_extractor import AbstractExtractor
from src.types import Config


class JDBCExtractor(AbstractExtractor):
    def __init__(self,config : Config):
        self.config=config