from __future__ import annotations
from typing import Callable
from common.logger import settings_logger
from confluent_kafka import Consumer, Producer, Message


class KafkaService:
    def __init__(self, configs: dict) -> None:
        self.wait_time: float = .5
        self.configs: dict = configs
        self.logger = settings_logger(self.__class__.__name__)

    def _process_message_value(self, message: Message, func: Callable) -> str:
        message_value = message.value()
        if message_value:
            message_value = func(message_value.decode('utf-8'))
        else:
            self.logger.debug("Kafka message without value field")
        return message_value

    def _check_valid_message(self, message: Message) -> bool:
        if message is None:
            self.logger.debug("Kafka message is null.")
            return False
        if message.error():
            self.logger.error("Consumer error: %s", message.error())
            return False
        return True

    def consume(self, topics: list, processor: Callable = None) -> object:
        consumer = Consumer(self.configs)

        self.logger.info("Start consumer, subscribe in topics: %s", topics)
        consumer.subscribe(topics)
        
        while True:
            raw_message = consumer.poll(self.wait_time)
            if self._check_valid_message(raw_message):
                _ = self._process_message_value(raw_message, processor)
            else:
                continue
            consumer.commit()
    
    def produce(self, topic: str, message: str, key: str = None):
        self.logger.info("Start producer for topic: %s", topic)
        producer = Producer(self.configs)
        producer.produce(topic, key=key, value=message)
        producer.flush()
        self.logger.info('Produced message.')
        return True
