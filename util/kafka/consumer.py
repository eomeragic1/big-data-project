# consumer recive messages from kafka

import datetime
from confluent_kafka import Consumer, TopicPartition
import json
from collections import deque
from time import sleep


class KafkaConnect:
    def __init__(self, topic, group, que_len=1000):
        self.topic = topic

        self.conf = {
            'bootstrap.servers': 'localhost:9092',
            'group.id': group,
            'group.instance.id': 'plot-consumer2',
            'enable.auto.commit': True,
        }

        # the application needs a maximum of 180 data units
        self.data = {
            'Issue Date': deque(maxlen=que_len),
            'Temperature': deque(maxlen=que_len),
            'Violation County': deque(maxlen=que_len),
        }

        consumer = Consumer(self.conf)
        consumer.subscribe([self.topic])

        # download first 180 messges
        self.partition = TopicPartition(topic=self.topic, partition=0)
        low_offset, high_offset = consumer.get_watermark_offsets(self.partition)

        # move offset back on 180 messages
        if high_offset > que_len:
            self.partition.offset = high_offset - que_len
        else:
            self.partition.offset = low_offset

        # set the moved offset to consumer
        consumer.assign([self.partition])

        self.__update_que(consumer)

    # https://docs.confluent.io/current/clients/python.html#delivery-guarantees
    def __update_que(self, consumer):
        try:
            while True:
                msg = consumer.poll(timeout=1)
                if msg is None:
                    break
                elif msg.error():
                    print('error: {}'.format(msg.error()))
                    break
                else:
                    record_value = msg.value()
                    json_data = json.loads(record_value.decode('utf-8'))

                    self.data['Temperature'].append(json_data['Temperature'])
                    self.data['Issue Date'].append(json_data['Issue Date'])
                    self.data['Violation County'].append(json_data['Violation County'])

                    # save local offset
                    self.partition.offset += 1
        finally:
            # Close down consumer to commit final offsets.
            # It may take some time, that why I save offset locally
            consumer.close()

    def get_graph_data(self):
        consumer = Consumer(self.conf)
        consumer.subscribe([self.topic])

        # update low and high offsets (don't work without it)
        consumer.get_watermark_offsets(self.partition)

        # set local offset
        consumer.assign([self.partition])

        self.__update_que(consumer)

        # convert data to compatible format
        o = {key: list(value) for key, value in self.data.items()}
        return o

    def get_last(self):
        date = self.data['Issue Date'][-1]
        temperature = self.data['Temperature'][-1]
        county = self.data['Violation County'][-1]

        return date, temperature, county


# for test
if __name__ == '__main__':
    connect = KafkaConnect(topic='output-topic-parking-violations-issued', group='test_group')

    while True:
        test = connect.get_graph_data()

        print('number of messages:', len(test['Issue Date']))
        sleep(0.1)