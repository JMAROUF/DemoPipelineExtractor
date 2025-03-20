from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import Optional, Dict, Any, List
import yaml
import json

@dataclass_json
@dataclass
class InputConfig:
    extractor: str
    name: str
    uri: str
    parameters: Optional[Dict[str, Any]] = None

@dataclass_json
@dataclass
class OutputConfig:
    loader: str
    name: str
    uri: str
    parameters: Optional[Dict[str, Any]] = None

@dataclass_json
@dataclass
class Config:
    inputs: InputConfig
    outputs: List[OutputConfig]

yaml_config = '''
inputs:
  extractor: input_extractor
  name: example_name
  uri: example_uri
  parameters:
    param1: value1
    param2: value2
outputs:
  - loader: example_loader
    name: example_output_name
    uri: example_output_uri
    parameters:
      paramA: valueA
      paramB: valueB
'''

# Load YAML content
config_dict = yaml.safe_load(yaml_config)
print(config_dict)
# Convert to JSON string
json_config = json.dumps(config_dict)

# Use from_json to deserialize
config = Config.from_json(json_config)

# Display the attribute extractor of InputConfig
print(config.inputs.extractor)