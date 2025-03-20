import yaml
import json
import logging
from dataclasses import dataclass
from typing import List, Dict, Optional, Union, Any
from pyspark.sql import SparkSession, DataFrame as SDF
from pyspark.sql import functions as F
from importlib import import_module

# Configuration Classes
@dataclass
class InputConfig:
    name: str
    uri: str
    parameters: Dict[str, Any]
    ddl_schema: str

@dataclass
class TransformConfig:
    name: str
    transformer: str
    inputs: str
    parameters: Dict[str, Any]

@dataclass
class OutputConfig:
    name: str
    uri: str
    parameters: Dict[str, Any]

@dataclass
class OperatorConfig:
    name: str
    operator: str
    parameters: Dict[str, Any]

@dataclass
class Config:
    name: str
    enable: bool
    order: int
    inputs: Optional[List[InputConfig]] = None
    transforms: Optional[List[TransformConfig]] = None
    outputs: Optional[List[OutputConfig]] = None
    custom_operators: Optional[List[OperatorConfig]] = None

@dataclass
class GroupConfig:
    configs: Optional[List[Config]] = None

# Extractor and Transformer Classes
class AbstractExtractor:
    @staticmethod
    def from_config(config: InputConfig) -> 'AbstractExtractor':
        mod_name, cls_name = config.extractor.rsplit('.', 1)
        mod = import_module(mod_name)
        kls = getattr(mod, cls_name)
        if config.parameters:
            return kls(**config.parameters)
        return kls()

class SingleFreeStyleTransformer:
    def __init__(self, function_name: str, function_parameters: Dict[str, Any]):
        self.function_name = function_name
        self.function_parameters = function_parameters

    def transform(self, dataframe: SDF) -> SDF:
        # Example transformation logic
        for col, expr in self.function_parameters['audit_columns'].items():
            dataframe = dataframe.withColumn(col, F.expr(expr))
        return dataframe

class Hub_Factory:
    def __init__(self, tech_id: str, table_name: str, location: str, columns: List[str]):
        self.tech_id = tech_id
        self.table_name = table_name
        self.location = location
        self.columns = columns

    def transform(self, dataframe: SDF) -> SDF:
        # Example transformation logic
        return dataframe.select(*self.columns)

# Execution Context and Pipeline Classes
class ExecutionContext:
    def __init__(self, env: str, event_date: str, specification_file: str, additional_variables: Dict[str, str] = {}, secrets: Dict[str, str] = None):
        self.env = env
        self.event_date = event_date
        self.specification_file = specification_file
        self.additional_variables = additional_variables
        self.secrets = secrets

    def get_spark(self) -> SparkSession:
        return SparkSession.builder.appName("BusinessReviewJob").getOrCreate()

class Pipeline:
    def __init__(self, config: Config, spark: SparkSession, logger: logging.Logger):
        self.config = config
        self.spark = spark
        self.logger = logger

    def run(self):
        self.logger.info(f"Running job: {self.config.name}")
        for input_config in self.config.inputs:
            extractor = AbstractExtractor.from_config(input_config)
            dataframe = extractor.extract(input_config.uri, self.spark, self.logger)
            for transform_config in self.config.transforms:
                transformer = self.get_transformer(transform_config)
                dataframe = transformer.transform(dataframe)
            dataframe.show()

    def get_transformer(self, config: TransformConfig):
        mod_name, cls_name = config.transformer.rsplit('.', 1)
        mod = import_module(mod_name)
        kls = getattr(mod, cls_name)
        return kls(**config.parameters)

# Main Script to Execute the Job
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    # Read the YAML configuration
    with open("/c:/AMC/corp_inv_glue_job/config/DATA_VAULT/SFF/business_review.yaml", 'r') as file:
        config_data = yaml.safe_load(file)

    # Convert YAML to Config object
    config = Config(**config_data)

    # Initialize the execution context
    context = ExecutionContext(env="dev", event_date="2023-10-01", specification_file="business_review.yaml")

    # Initialize Spark session
    spark = context.get_spark()

    # Create and run the pipeline
    pipeline = Pipeline(config=config, spark=spark, logger=logger)
    pipeline.run()