import yaml
from functools import reduce
from os import getcwd

def load_configurations(path: str) -> dict:
    with open("configurations.yaml", "r") as stream:
        try:
            result = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    return result

def mount_kafka_config(configs: dict) -> dict:
    merged = dict(reduce(lambda a, b: dict(a, **b), configs))
    return merged

def mount_source(configs: dict) -> dict:
    return configs['kafka']

def mount_destination(configs: dict) -> dict:
    return configs['kafka']

CONFIGURATION = load_configurations(f"{getcwd()}/configurations.yaml")

KAFKA_CONFIG = mount_kafka_config(CONFIGURATION['kafka'])
SCHEMA_REGISTRY_URL = KAFKA_CONFIG.pop('schema.registry.url')
SOURCE_CONFIG = mount_source(CONFIGURATION['source'])
DESTINATION_CONFIG = mount_destination(CONFIGURATION['destination'])
