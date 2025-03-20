from tkinter.font import names
from typing import Dict, Any, Optional, List

from dataclasses import dataclass
from dataclasses_json import dataclass_json

@dataclass_json
@dataclass
class InputConfig:
    name: str
    extractor:str
    uri:str
    parameters: Optional[Dict[str, Any]]

    def __str__(self):
        return f"MyClass(field1={self.uri})"


@dataclass_json
@dataclass
class OutputConfig:
    name: str
    loader: str
    uri: str
    parameters: Optional[Dict[str, Any]]

@dataclass_json
@dataclass
class Config:
    enable:bool
    inputs:  Optional[list[InputConfig]]
    outputs: Optional[list[OutputConfig]]


    def __eq__(self, other):
        if isinstance(other, Config):
            return self.inputs == other.inputs and \
                self.outputs == other.outputs

    @staticmethod
    def from_yaml(parsed_yaml: Dict[Any, Any]):
        return Config(**parsed_yaml)


@dataclass_json
@dataclass
class GroupConfig:
    Configs: Optional[List[Config]]
    def __init__(self,configs:List[Config]):
        self.configs=configs