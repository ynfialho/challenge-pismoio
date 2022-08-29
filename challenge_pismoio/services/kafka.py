from __future__ import annotations
from typing import Callable
from challenge_pismoio.common.logger import settings_logger
from confluent_kafka import Message, SerializingProducer, DeserializingConsumer, Producer, Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import StringSerializer


class KafkaService:
    """An Apache Kafka wrapper.
    Args:
        configs (dict): Apache Kafka configurations.
        schema_registry_url (str, optional): Schema Registry URL. Defaults to None.
    """   
    def __init__(self, configs: dict, schema_registry_url: str = None) -> None:     
        self.wait_time: float = .5
        self.configs: dict = configs
        self.schema_registry_url = schema_registry_url
        self.logger = settings_logger(self.__class__.__name__)

    def _process_message_value(self, message: Message, func: Callable) -> str:
        message_value = message.value()
        if message_value:
            message_value = func(message_value)
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
    
    def _schema_registry_client(self):
        return SchemaRegistryClient(conf={'url': self.schema_registry_url})

    def get_avro_consumer(self, topics: list) -> Consumer:
        """ Get AVRO consumer.
        Args:
            topics (list): list of topics
        Returns:
            Consumer: confluent_kafka consumer
        """        
        avro_serializer = AvroDeserializer(self._schema_registry_client())
        string_serializer = StringSerializer('utf_8')

        consumer_conf = self.configs.copy()
        consumer_conf['key.deserializer'] = string_serializer
        consumer_conf['value.deserializer'] = avro_serializer

        consumer = DeserializingConsumer(consumer_conf)

        self.logger.info("Create a consumer, subscribe in topics: %s", topics)
        consumer.subscribe(topics)
        return consumer
    
    def avro_produce(self, topic: str, message: str, schema: str, key: str = None) -> bool:
        """Produce AVRO message to Kafka topic.
        Args:
            topic (str): topic name
            message (str): message for produce
            schema (str): AVRO schema 
            key (str, optional): key of message. Defaults to None.

        Returns:
            bool: _description_
        """        
        avro_serializer = AvroSerializer(self._schema_registry_client(), schema)
        string_serializer = StringSerializer('utf_8')

        producer_conf = {}
        producer_conf['bootstrap.servers'] = self.configs['bootstrap.servers']
        producer_conf['key.serializer'] = string_serializer
        producer_conf['value.serializer'] = avro_serializer

        avro_producer = SerializingProducer(producer_conf)

        self.logger.info('Producing AVRO messages...')
        avro_producer.produce(topic=topic, value=message, key=key)
        avro_producer.flush()
        return True

    def produce(self, topic: str, message: str, key: str = None) -> bool:
        """Produce message to Kafka topic.
        Args:
            topic (str): topic name
            message (str): message for produce
            key (str, optional): key of message. Defaults to None.

        Returns:
            bool: _description_
        """        
        self.logger.info("Producing message for topic: %s", topic)
        producer = Producer(self.configs)
        producer.produce(topic, key=key, value=message)
        producer.flush()
        return True

    def consume(self, topics: list, processor: Callable = None, trigger_once: bool = False):
        """Consume messages in Kafka topics.
        Args:
            topics (list): list of topics
            processor (Callable, optional): function applicable in the message. Defaults to None.
        """        
        consumer = Consumer(self.configs)

        self.logger.info("Start consumer, subscribe in topics: %s", topics)
        consumer.subscribe(topics)
        
        while True:
            try:
                raw_message = consumer.poll(self.wait_time)
                if self._check_valid_message(raw_message):
                    _ = self._process_message_value(raw_message, processor)
                    if trigger_once:
                        return True
                else:
                    continue
            except Exception as err:
                if err.name == 'UNKNOWN_TOPIC_OR_PART':
                    self.logger.warning('UNKNOWN_TOPIC_OR_PART')
                    continue
