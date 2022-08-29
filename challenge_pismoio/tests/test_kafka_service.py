import pytest
import time
import json
from challenge_pismoio.services.kafka import KafkaService
from challenge_pismoio.models.mock_payloads import ClientA
from  dataclasses import asdict


CONF = {'bootstrap.servers': 'localhost:9094',
        'group.id': "debugger",
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True}
SCHEMA_REGISTRY_URL = 'http://0.0.0.0:8081'
TEST_TOPIC = 'challenge-pismoio-events'
TEST_AVRO_TOPIC = 'challenge-pismoio-avro-events'

@pytest.fixture
def kafka_service():
    return KafkaService(CONF, SCHEMA_REGISTRY_URL)


def current_timestamp():
    return round(time.time() * 1000)

def write_file(data, tmp_obj):
    with open(f"{tmp_obj.name}/test.txt", 'w') as f:
        f.write(data)
    return True


@pytest.mark.usefixtures("kafka_service")
def test_producer(kafka_service: KafkaService):
    payload = {"event_type": "test", "name": "test_producer", "event_timestamp": current_timestamp()}
    result = kafka_service.produce(TEST_TOPIC, json.dumps(payload))
    assert result

@pytest.mark.usefixtures("kafka_service")
def test_consume(kafka_service: KafkaService):
    processor = lambda x: print(x)
    result = kafka_service.consume([TEST_TOPIC], processor, trigger_once=True)
    assert result


@pytest.mark.usefixtures("kafka_service")
def test_avro_producer(kafka_service: KafkaService):
    schema = open('challenge_pismoio/models/client_a.json', 'r').read()
    result = kafka_service.avro_produce(TEST_AVRO_TOPIC, asdict(ClientA()), schema)
    assert result

@pytest.mark.usefixtures("kafka_service")
def test_get_avro_consumer(kafka_service: KafkaService):
    consumer = kafka_service.get_avro_consumer(['dummy-topic'])
    assert consumer.assignment() == []
