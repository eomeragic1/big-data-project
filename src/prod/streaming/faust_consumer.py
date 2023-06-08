import faust

app = faust.App('Faust-Consumer', broker='kafka://localhost:9092', topic_partitions=2)

weather_topic = app.topic('topic-WEATHER2', value_type=str, value_serializer='json')


@app.agent(weather_topic)
async def consume_weather(stream):
    async for message in stream:
        print(message)
