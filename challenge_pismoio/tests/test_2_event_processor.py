import pytest
from challenge_pismoio.event_processor import EventProcessor


destinations = {
    "sales": 'client_a-good-events',
    "lead": 'client_b-good-events',
    "geolocation": 'client_c-good-events'
}

CONF = {'bootstrap.servers': 'localhost:9094',
        'group.id': "debugger",
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True}
SCHEMA_REGISTRY_URL = 'http://0.0.0.0:8081'
TEST_TOPIC = 'challenge-pismoio-events'
TEST_AVRO_TOPIC = 'challenge-pismoio-avro-events'

@pytest.fixture
def event_processor():
    return EventProcessor(CONF, SCHEMA_REGISTRY_URL, destinations)


@pytest.mark.usefixtures("event_processor")
def test_validate_persist(event_processor: EventProcessor):
    result = event_processor.validate_persist([TEST_AVRO_TOPIC], True)
    assert result
