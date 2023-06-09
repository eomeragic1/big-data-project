import faust
import json


def transform_borough(x):
    ## x: (issue date, ticket price, borough)
    if x[2] in ['BK', 'K', 'Kings']:
        return x[0], x[1], 'Brooklyn'
    elif x[2] in ['ABX', 'BX', 'Bronx']:
        return x[0], x[1], 'Bronx'
    elif x[2] in ['MN', 'NY']:
        return x[0], x[1], 'Manhattan'
    elif x[2] in ['Q', 'QN', 'QNS', 'QUEEN', 'Qns']:
        return x[0], x[1], 'Queens'
    elif x[2] in ['R', 'Rich', 'ST']:
        return x[0], x[1], 'Staten Island'
    else:
        return x[0], x[1], ''

def filter_eliminate_empty_boroughs(x):
    if x[2]=='':
        return False
    return True


app = faust.App('Faust-Consumer', broker='kafka://localhost:9092', topic_partitions=4)
input_topic = app.topic('topic-parking-violations-issued', value_type=str, value_serializer='json')
output_topic = app.topic('output-topic-parking-violations-issued')


@app.agent(input_topic)
async def consume_parking_violation_tickets(stream):
    async for message in stream:
        message = json.loads(message)
        new_msg = {'Issue Date': message['Issue Date'], 'Temperature': message['W - Temperature'], 'Violation County': message['Violation County Name']}
        await output_topic.send(value=new_msg)


