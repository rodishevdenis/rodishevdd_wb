import os
import json
from kafka import KafkaProducer
from clickhouse_driver import Client
import pandas as pd

ch_connection_string = "clickhouse://admin:admin@localhost:9000/db"

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)

def on_send_error(excp):
    print(excp)

def main():
    producer = KafkaProducer(
        bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
        client_id=os.environ.get('KAFKA_CLIENT_ID', 'python-producer'),
        security_protocol='SASL_PLAINTEXT',
        sasl_mechanism='PLAIN',
        sasl_plain_username=os.environ.get('KAFKA_USERNAME', 'demo'),
        sasl_plain_password=os.environ.get('KAFKA_PASSWORD', 'demo-password'),
        value_serializer=lambda m: json.dumps(m).encode('utf-8'),
        retries=3,
    )

    df = pd.read_csv('data.csv', sep=';')

    with Client.from_url(ch_connection_string) as ch:
        # df = ch.query_dataframe(reports_query)
        # df['dt'] = df['dt'].astype(str)
        
        for row in df.to_dict('records'):
            producer.send('topic', row).add_callback(on_send_success).add_errback(on_send_error)

    producer.flush()

if __name__ == '__main__':
    main()