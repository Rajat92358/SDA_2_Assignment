import csv
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'crypto-fraud-detection'
print(f"Starting Kafka Producer... Streaming data to topic: {topic_name}\n")

with open('binance_live_trades_readable.csv', mode='r') as file:
    reader = csv.DictReader(file)
    count = 0
    for row in reader:
        producer.send(topic_name, value=row)
        count += 1
        print(f"[{count}] Sent: {json.dumps(row)}")
        producer.flush()
        time.sleep(1)