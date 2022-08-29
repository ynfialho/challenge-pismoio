from services.kafka import KafkaService
import json


class EventProcessor(KafkaService):
    """Consume, validate and serves messages in Apache Kafka brokers.

    Args:
        configs (dict): Apache Kafka configurations.
        schema_registry_url (str): Schema Registry URL.
        destinations (dict): Map of event_type and topics.
    """  
    def __init__(self, configs: dict, schema_registry_url: str, destinations: dict) -> None:      
        super().__init__(configs, schema_registry_url)
        self.destinations = destinations

    def validate_persist(self, topics: list):
        """Validate and write messages in Kafka topics.
        Args:
            topics (list): Source Kafka topics 
        """        
        consumer = self.get_avro_consumer(topics=topics)

        while True:
            try:
                raw_message = consumer.poll(.5)
                if self._check_valid_message(raw_message):
                    _ = self._process_message_value(raw_message, self.delilvery_to_kafka)
                else:
                    self.logger.debug('Message isn`t valid or null.')
                    continue
            except Exception as err:
                if err.name == 'UNKNOWN_TOPIC_OR_PART':
                    self.logger.warning('UNKNOWN_TOPIC_OR_PART')
                    continue

    def delilvery_to_kafka(self, message: str) -> str:
        """ Write message in Kafka topic.
        Args:
            message (str): _description_

        Returns:
            str: _description_
        """        
        dest_topic = self.destinations.get(message.get('event_type', ''), None)
        self.produce(topic=dest_topic, message=json.dumps(message))
        return dest_topic

conf = {'bootstrap.servers': 'localhost:9094',
    'group.id': 'validate6',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True}

destinations = {
    "sales": 'client_a-good-events',
    "lead": 'client_b-good-events',
    "geolocation": 'client_c-good-events'
}

event_processor = EventProcessor(conf, 'http://0.0.0.0:8081', destinations)
event_processor.validate_persist(['client_a-events', 'client_b-events', 'client_c-events'])
