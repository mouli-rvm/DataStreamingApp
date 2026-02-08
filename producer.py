from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField

# 1. Define the Avro Schema
schema_str = """
{
  "namespace": "example.avro",
  "type": "record",
  "name": "User",
  "fields": [
    {"name": "name", "type": "string"},
    {"name": "favorite_number", "type": "int"},
    {"name": "favorite_color", "type": "string"}
  ]
}
"""

# 2. Configuration
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

avro_serializer = AvroSerializer(schema_registry_client,
                                 schema_str)

producer_conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# 3. Produce Data
topic = "user_updates"
user_data = {"name": "Mouli", "favorite_number": 5, "favorite_color": "Blue"}

producer.produce(topic=topic,
                 key=str(user_data['name']),
                 value=avro_serializer(user_data, SerializationContext(topic, MessageField.VALUE)),
                 on_delivery=delivery_report)

producer.flush()