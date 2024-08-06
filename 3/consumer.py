import os
import json
from kafka import KafkaConsumer
from clickhouse_driver import Client

def main():
    consumer = KafkaConsumer(
        'topic',
        bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        client_id=os.environ.get('KAFKA_CLIENT_ID', 'python-producer'),
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username=os.environ.get('KAFKA_USERNAME', 'demo'),
        sasl_plain_password=os.environ.get('KAFKA_PASSWORD', 'demo-password'),
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest', 
        enable_auto_commit=False,
    )

    for message in consumer:
        print ("%d: %s" % (message.offset, message.value))

if __name__ == '__main__':
    main()