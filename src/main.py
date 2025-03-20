import yaml
import json
from dataclasses import dataclass
from dataclasses_json import dataclass_json
from string import Template

@dataclass_json
@dataclass
class ChildClass:
    name: str
    value: int

@dataclass_json
@dataclass
class ParentClass:
    name: str
    children: list[ChildClass]

@dataclass_json
@dataclass
class Famille:
    parents: list[ParentClass]

def load_config(file_path, substitutions, encoding='utf-8'):
    with open(file_path, 'r', encoding=encoding) as file:
        template_content = file.read()
    template = Template(template_content)
    substituted_content = template.substitute(substitutions)
    config = yaml.load(substituted_content, Loader=yaml.FullLoader)
    familles = [Famille.from_json(json.dumps(famille)) for famille in config['familles']]
    return familles

# Substitutions
substitutions = {
    'parent1_name': 'ParentClass1',
    'child1_name': 'ChildClass1',
    'child1_value': 42,
    'child2_name': 'ChildClass2',
    'child2_value': 84,
    'parent2_name': 'ParentClass2',
    'child3_name': 'ChildClass3',
    'child3_value': 21,
    'child4_name': 'ChildClass4',
    'child4_value': 63,
    'parent3_name': 'ParentClass3',
    'child5_name': 'ChildClass5',
    'child5_value': 35,
    'child6_name': 'ChildClass6',
    'child6_value': 70,
    'parent4_name': 'ParentClass4',
    'child7_name': 'ChildClass7',
    'child7_value': 28,
    'child8_name': 'ChildClass8',
    'child8_value': 56,
    'parent5_name': 'ParentClass5',
    'child9_name': 'ChildClass9',
    'child9_value': 14,
    'child10_name': 'ChildClass10',
    'child10_value': 29,
    'parent6_name': 'ParentClass6',
    'child11_name': 'ChildClass11',
    'child11_value': 7,
    'child12_name': 'ChildClass12',
    'child12_value': 14
}

# Utilisation
familles_instances = load_config('../config/config.yaml', substitutions)
for famille in familles_instances:
    print(famille)