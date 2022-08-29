""" This module show message in Apache Kafka.
"""
from services.kafka import KafkaService
from common.logger import settings_logger

logger = settings_logger('debugger')

conf = {'bootstrap.servers': 'localhost:9094',
        'group.id': "debugger",
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': True}


kafka = KafkaService(configs=conf, schema_registry_url='http://0.0.0.0:8081')

kafka.consume(['client_a-good-events', 'client_b-good-events', 'client_c-good-events'], logger.info)
