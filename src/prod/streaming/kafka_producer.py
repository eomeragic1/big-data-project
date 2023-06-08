import inspect
import json
import os
import random
import sys
import time

import pandas as pd
from kafka import KafkaProducer

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)


def on_send_success(record_metadata):
    print(
        f'Sent successfully at topic "{record_metadata.topic}", partition "{record_metadata.partition}", offset "{record_metadata.offset}".')


def on_send_error(e):
    print('Exception: ', e)


def custom_partitioner(key: bytes, partitions: int, available: list):
    return int(key.decode('UTF-8')) % len(partitions)


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             client_id='streamz-producer',
                             buffer_memory=134217728,
                             value_serializer=lambda m: json.dumps(m).encode('ascii'),
                             partitioner=custom_partitioner)
    files = os.listdir('../../../data/parquet/AUGMENTED_PARKING_VIOLATION_ISSUED.parquet')
    for file in files:
        data = pd.read_parquet('../../../data/parquet/AUGMENTED_PARKING_VIOLATION_ISSUED.parquet' + '/' + file)
        data = data.sample(frac=1)
        for idx, (i, datapoint) in enumerate(data.iterrows()):
            producer.send(
                topic=f'topic-parking-violations-issued',
                key=str(i).encode(),
                value=datapoint.to_json()
            ).add_callback(on_send_success).add_errback(on_send_error)

            time.sleep(1)
            producer.flush()
