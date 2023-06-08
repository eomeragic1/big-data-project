# Scaling Streaming with Kafka and Faust

In this document, we explain how to scale the ingestion and processing of data streams with libraries that support
distributed computing.

Pre-requisites:

- Java, at least version 11
- Kafka, ([download](https://kafka.apache.org/downloads)): Binary, Scala 2.13
    - Unzip to a folder named `kafka`

To change configuration files edit:

- For Zookeeper: `kafka/zookeeper.properties`
- For Kafka: `kafka/server.properties`

Append the following setting to `kafka/zookeper.properties`:

```text
fileslog.dirs=C:\kafka\kafka-logs
```

or on SLURM cluster:

```text
fileslog.dirs=/d/hpc/projects/FRI/data/kafka-logs
```

Now, run Zookeeper and Kafka instances. Move to Kafka root folder and execute the following commands:

```shell
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

and

```shell 
 .\bin\windows\kafka-server-start.bat .\config\server.properties
```

Note: different commands are used to start Zookeeper and Kafka in Linux (e.g. on a cluster). Omit the `/windows` folder
in the commands to start them in Linux.

The broker is now running on port 9092 by default.

To create a new topic, we run the following command:

```shell
.\bin\windows\kafka-topics.bat --create --topic <TOPIC_NAME> --bootstrap-server localhost:9092 --replication-factor 1 --partitions 4
```

This command creates a new topic that enables distributed computing due to the `--partitions` option. It defines that
the topic can have 4 distinct consumers. This will later allow us to scale our data streams.

### Simulating Kafka producers with Pandas DataFrames

If we do not have any processes that actually generate the Kafka messages, or if we want to stream rows of dataframes,
we can use the Pandas library to read data and then use `kafka-python` library to send the
messages to the broker. To install the `kafka-python` library, run:

```shell
pip install kafka-python
```

Kafka producer in Python example can be found in `src/prod/streaming/kafka_producer.py` Python script. The script:

1. Connects to the Kafka broker running on localhost:9092.
2. Reads a partition of a Parquet file, so that it can fit into the working memory.
3. Iterates through each row of the dataframe.
4. Sends the dataframe row to the broker by a pre-defined topic.
5. We limit the bandwidth of the messages by utilizing *time.sleep()* after we send a message.

We can run the script by navigating to the root folder and
executing:

```shell
python src/prod/streaming/kafka_producer.py
```

*Note that Kafka producer is written in a way to enable partitioning, hence distributed computation.*

### Running distributed consumers on Faust

Faust can be used to consume data streams in a distributed manner. Multiple Faust workers can subscribe to the same
topic, thus scaling the data transformation process.

An example of a Faust subscription to a Kafka topic can be found in `src/prod/streaming/faust_consumer.py`.

We must first run Faust consumers, so that we do not lose any data from the streams. Only after the consumers are
running may we start the Kafka producer.

In this example, we will run 2 Faust workers.

```shell
faust --datadir=./worker1 -A faust_consumer -l info worker --web-port=6066
```

```shell
faust --datadir=./worker2 -A faust_consumer -l info worker --web-port=6067
```

Now, we have 2 workers running on the same topic and thus data streaming can be scaled.
*Note that workers cannot share data directories or web ports.*

To run this setting on remote cluster, we propose
provisioning tools that can automate these procedures.

### Visualizing data streams on Plotly