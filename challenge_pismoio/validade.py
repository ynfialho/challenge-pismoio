from services.kafka import KafkaService


brokers = ['localhost:9092']

conf = {'bootstrap.servers': ','.join(brokers),
        'group.id': "validate",
        'auto.offset.reset': 'latest',
        'enable.auto.commit': False}

kafka = KafkaService(configs=conf)

kafka.produce(topic='client0-events', message='{"client": "client1", "name": "client0_rodrigao"}')

kafka.consume(topics=['client0-events'], processor=lambda x: print(x))
