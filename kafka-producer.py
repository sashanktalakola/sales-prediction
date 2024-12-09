from kafka import KafkaProducer
import csv
import argparse
import os
from dotenv import load_dotenv

load_dotenv()


parser = argparse.ArgumentParser()
parser.add_argument('--month', type=int, required=True)
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=[f"{os.getenv("KAFKA_IP")}:{os.getenv("KAFKA_PORT")}", ],
    value_serializer=lambda v: str(v).encode('utf-8')
)

csv_file = f'./data/streamData-{args.month}.csv'

topic = 'csv-data'

with open(csv_file, 'r') as file:
    reader = csv.reader(file)
    for row in reader:

        message = ','.join(row)
        producer.send(topic, value=message)

producer.flush()
producer.close()

