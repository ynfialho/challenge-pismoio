from services.kafka import KafkaService
from models.mock_payloads import ClientA, ClientB, ClientC
from dataclasses import asdict
from os import getcwd
import json

TOPIC_PATTERN = '{client}-events'
QTDY_MESSAGES = range(3000)


def load_schemas(map_schemas: dict) -> dict:
    """Load AVRO schema stored in project.
    Args:
        map_schemas (dict): map of clients and paths

    Returns:
        dict: map of client and schemas
    """    
    result = map_schemas.copy()
    for index in range(len(map_schemas)):
        map_item = map_schemas[index]
        map_item['schema'] = json.dumps(json.load(open(f"{getcwd()}/{map_item['path']}")))
        result[index] = map_item
    return result

conf = {'bootstrap.servers': 'localhost:9094',
        'group.id': "validate",
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True}

schemas = [
    {"client": "client_a", "path": 'challenge_pismoio/models/client_a.json', "payload": ClientA},
    {"client": "client_b", "path": 'challenge_pismoio/models/client_b.json', "payload": ClientB},
    {"client": "client_c", "path": 'challenge_pismoio/models/client_c.json', "payload": ClientC}
]

schemas = load_schemas(schemas)


kafka = KafkaService(configs=conf, schema_registry_url='http://0.0.0.0:8081')
for idx in QTDY_MESSAGES:
    for item in schemas:
        topic_name = TOPIC_PATTERN.format(client=item['client'])
        for _ in range(3):
            kafka.avro_produce(topic_name, asdict(item['payload']()), item['schema'])
