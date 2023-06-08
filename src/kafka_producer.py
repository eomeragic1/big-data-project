from kafka import KafkaProducer
import pandas as pd
import os
import json
import time
import sys
import inspect

current_dir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    print('Exception: ', excp)


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092', client_id='streamz-producer', buffer_memory=134217728,
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))
    files = os.listdir('./data/parquet/AUGMENTED_PARKING_VIOLATION_ISSUED.parquet')
    for file in files:
        data = pd.read_parquet('./data/parquet/AUGMENTED_PARKING_VIOLATION_ISSUED.parquet' + '/' + file)
        for idx, (i, datapoint) in enumerate(data.iterrows()):
            producer.send('streamz-topic', datapoint.to_json()).add_callback(on_send_success).add_errback(on_send_error)
            time.sleep(0.01)
            if idx % 10000 == 0:
                producer.flush()
