import json
import os
from dataclasses import dataclass
from os.path import join, isfile
from string import Template
from typing import List, Any, Dict, Optional

import yaml
from dataclasses_json import dataclass_json


class InputConfig:
    name: str
    uri: str
    extractor: str
    parameters: Dict[str, Any]

@dataclass_json
@dataclass
class OutputConfig:
    name: str
    uri: str
    input: str = None
    loader: str = "src.load.Loader"
    parameters: Optional[Dict[str, Any]] = None



@dataclass_json
@dataclass
class Config:
    name: str = "default_name"
    enable: bool = True
    order: int = 9999
    inputs: List[InputConfig] = None
    outputs: List[OutputConfig] = None



@dataclass_json
@dataclass
class GroupConfig:
    configs: List[Config] = None
    ddls: Dict[str, Any] = None

    def __eq__(self, other):
        if isinstance(other, GroupConfig):
            return self.configs == other.configs

class ExecutionContext:

    def __init__(self,specification_file,env:str):
        self.specification_file=specification_file
        self.env=env
        self.s3_object_mode = "s3a"
        self.bucket_trusted_rin = f"bpi-fr-{self.env}-rin-dlk-trusted"
        self.placeholders={}
        self.placeholders["bucket_trusted_rin"]=f"{self.s3_object_mode}://{self.bucket_trusted_rin}/CORPORATE/COMPTOIR_INVESTISSEMENT/TECHNICAL/checkpoint"
        self.schema={}
        self.schema['schema_athena_mou_switched'] = f'corp_rin_mou_invest{self.env}'
    file_mode = ("yaml", "yml")

    def get_configs(self) -> GroupConfig:

        configs = []
        for config in self.get_config_text():
            configs.append(self.get_config(config))
        return GroupConfig(configs=configs)

    def get_config_text(self) -> List[str]:

            return self.read_as_string_local(self.specification_file)


    def read_as_string_local(self, path, encoding: str = "utf-8") -> List[str]:
        """
        List all the files in the folder and read them as string
        """
        if not self.check_if_folder_local(path):
            return [open(path, encoding=encoding).read()]
        return [open(join(path, file), encoding=encoding).read() for file in self.list_of_files(path)]

    @staticmethod
    def check_if_folder_local(path):
        """
        Check if the path is a folder or a file
        """
        return os.path.isdir(path)

    @staticmethod
    def list_of_files(path) -> List[str]:
        """
        List all the files in the folder
        """
        files = [f for f in os.listdir(path) if isfile(join(path, f))]
        return filter(lambda file: str(file).endswith(ExecutionContext.file_mode), files)

    def get_config(self, content_config) -> Config:
        render_text = self.render(content_config)
        if ExecutionContext.file_mode == ("yaml", "yml"):
            render_text = yaml.load(render_text, Loader=yaml.FullLoader)
        render_text = json.dumps(render_text)
        print(f"render_text_json : {render_text}")
        return Config.from_json(render_text)

    def render(self, content) -> str:

        template_config = Template(content)
        full_text = template_config.substitute({
                                                **self.placeholders,
                                                **self.schema
                                                })
        return full_text


if __name__=='__main__':
    ec = ExecutionContext(specification_file="sff.yaml",env='dev')
    GroupConfigs=ec.get_configs()

    for config in GroupConfigs.configs:
        if isinstance(config, Config):
            print(config.name)