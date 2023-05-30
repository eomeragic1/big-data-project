from kafka import KafkaProducer
import pandas as pd
import os
import json
import time


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)


def on_send_error(excp):
    print('Exception: ', excp)


if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092', client_id='streamz-producer', buffer_memory=134217728,
                             value_serializer=lambda m: json.dumps(m).encode('ascii'))
    files = os.listdir('../data/Parking_Violations_Issued_-_Fiscal_Year_2023.parquet')
    codes = pd.read_csv('../data/fine_codes.csv', index_col=False)
    for file in files:
        data = pd.read_parquet('../data/Parking_Violations_Issued_-_Fiscal_Year_2023.parquet' + '/' + file)
        data['Violation Code'] = data['Violation Code'].astype('int64')
        codes['Violation Code'] = codes['Violation Code'].astype('int64')
        data = data.join(codes.set_index('Violation Code'), on='Violation Code', how="left", lsuffix='_left',
                         rsuffix='_right')
        for idx, (i, datapoint) in enumerate(data.iterrows()):
            producer.send('streamz-topic', datapoint.to_json()).add_callback(on_send_success).add_errback(on_send_error)
            time.sleep(0.01)
            if idx % 10000 == 0:
                producer.flush()
