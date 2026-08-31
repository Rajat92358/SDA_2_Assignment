from kafka import KafkaConsumer
import json

# Initialize Kafka Consumer listening on the same topic
consumer = KafkaConsumer(
    'crypto-fraud-detection',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Starting Kafka Consumer... Waiting for trade messages...\n")

for message in consumer:
    trade = message.value
    print(f"Received Trade ID: {trade.get('trade_id')} | Price: {trade.get('price')} | Qty: {trade.get('quantity')}")