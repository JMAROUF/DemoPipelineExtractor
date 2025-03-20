import os
from string import Template

import boto3

from src.aws.aws_parameter_store import AwsParameterStore
from src.execution_context import ExecutionContext
from src.types import GroupConfig, Config

if __name__=='__main__':


    context=ExecutionContext( "staging", "13/03/2025", '../config/test.yaml')



    groupConfigs=context.get_configs("utf-8")

    for config in groupConfigs.configs:
     if isinstance(config, Config):
         print(config.enable)
         for input_config in config.inputs:
             print(input_config.name)
             print(input_config.uri)
             print(input_config.parameters)



