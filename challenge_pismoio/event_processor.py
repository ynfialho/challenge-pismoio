from services.kafka import KafkaService
from common.constants import KAFKA_CONFIG, SOURCE_CONFIG, DESTINATION_CONFIG, SCHEMA_REGISTRY_URL
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

    def validate_persist(self, topics: list, trigger_once: bool = False):
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
                    if trigger_once:
                        return True
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
        dest_topics = self.destinations.get(message.get('event_type', ''), None)
        for topic in dest_topics:
            self.produce(topic=topic, message=json.dumps(message))
        return True

def main():
    event_processor = EventProcessor(KAFKA_CONFIG, SCHEMA_REGISTRY_URL, DESTINATION_CONFIG)
    event_processor.validate_persist(SOURCE_CONFIG)

if __name__ == '__main__':
    main()
