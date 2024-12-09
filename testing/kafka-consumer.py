from kafka import KafkaConsumer

# Kafka consumer configuration
consumer = KafkaConsumer(
    'csv-data',  # Topic name
    bootstrap_servers=['localhost:9092'],  # Kafka server
    group_id='csv-group',
    value_deserializer=lambda m: m.decode('utf-8')  # Deserialization
)

# Consume messages
for message in consumer:
    print(f"Received message: {message.value}")

