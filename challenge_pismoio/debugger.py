""" This module show message in Apache Kafka.
"""
from services.kafka import KafkaService
from common.logger import settings_logger
from common.constants import KAFKA_CONFIG, SCHEMA_REGISTRY_URL

logger = settings_logger('debugger')
good_topics = ['client_a-good-events', 'client_b-good-events', 'client_c-good-events']

kafka = KafkaService(configs=KAFKA_CONFIG, schema_registry_url=SCHEMA_REGISTRY_URL)
kafka.consume(good_topics, logger.info)
