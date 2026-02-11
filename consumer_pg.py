import psycopg2
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# 1. Database Configuration
db_config = {
    "host": "localhost",
    "database": "mydb",
    "user": "mouli",
    "password": "mypassword"
}

# 2. Kafka & Schema Registry Configuration
sr_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_conf)
avro_deserializer = AvroDeserializer(schema_registry_client)

consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'postgres-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(consumer_conf)
consumer.subscribe(["user_updates"])

def save_to_db(user_data):
    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()
        
        # Ensure the table exists
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                name TEXT,
                favorite_number INTEGER,
                favorite_color TEXT
            )
        """)
        
        # Insert the data
        insert_query = "INSERT INTO users (name, favorite_number, favorite_color) VALUES (%s, %s, %s)"
        cur.execute(insert_query, (user_data['name'], user_data['favorite_number'], user_data['favorite_color']))
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"Stored in DB: {user_data['name']}")
    except Exception as e:
        print(f"Database error: {e}")

# 3. Main Loop
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Deserialize and write to DB
        user_data = avro_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        if user_data:
            save_to_db(user_data)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()