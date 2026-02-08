from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# 1. Configuration
# Note: Using localhost as defined in your docker-compose ports
sr_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_conf)

# The deserializer will automatically fetch the correct schema from the Registry
avro_deserializer = AvroDeserializer(schema_registry_client)

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
topic = "user_updates"
consumer.subscribe([topic])

print(f"Starting consumer on topic: {topic}...")

# 2. Poll and Print Messages
try:
    while True:
        msg = consumer.poll(1.0) # Wait for a message for up to 1 second

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Deserialize the Avro binary value
        user_data = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        
        if user_data is not None:
            print(f"Successfully consumed record: {user_data}")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()