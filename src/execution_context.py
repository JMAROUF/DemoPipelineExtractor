import json
import os
from string import Template

import yaml

from src.aws.aws_parameter_store import AwsParameterStore
from src.aws.aws_sts import AwsSts
from src.types import GroupConfig, Config
from src.helper.config_handler import ConfigHandler
from src.utils.constants import file_mode


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]

class ExecutionContext(metaclass=Singleton):
    def __init__(self,env,event_date,specification_file):
        self.env = env
        self.event_date = event_date
        self.specification_file = specification_file
        self.additional_variables = {}
        self.secrets = {}
        self.assume_flag = "False"
        self.client_sts = AwsSts()
        self.client_ssm=AwsParameterStore(self.get_profile_name())
        self.config_handler = ConfigHandler()

    def get_configs(self,encoding:str)->GroupConfig:
        groupConfigs=[]
        for config in self.config_handler._read_as_string_local(self.specification_file,encoding):
            groupConfigs.append(self.get_config(config))
        return GroupConfig(configs=groupConfigs)

    def get_config(self,config:str)-> Config:
        render_text=self.render(config)
        render_text = render_text.replace('SLASH','/').replace('HYPHEN','-')
        if file_mode==("yaml","yml"):
            render_text = yaml.load(render_text, Loader=yaml.FullLoader)
        json_config=json.dumps(render_text)
        configInstance=Config.from_json(json_config)
        return configInstance



    def render(self,config:str)->str:
        parameters=self.get_paramerters()
        parameters_dict={}
        for key,value in parameters.items():
            formated_key = key.replace('/', 'SLASH').replace('-', 'HYPHEN')
            parameters_dict[formated_key]=value
            config = config.replace(key, formated_key)
        template_config=Template(config)
        return template_config.substitute(**parameters_dict)

    def get_paramerters(self):
        parameters=['/rin-staging/postgres_uri','/rin-staging/postgres_db_name']
        parameters_dict = {}
        for parameter in parameters:
            parameters_dict[parameter]=self.client_ssm.get_secret_parameter(parameter)
        return parameters_dict

    def get_profile_name(self):
        return os.getenv('AWS_PROFILE')


