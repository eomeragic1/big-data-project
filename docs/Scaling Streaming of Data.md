# Scaling Streaming of Data with Kafka and Faust

In this document, we explain how to scale the ingestion and processing of data streams with libraries that support distributed computing. 

Before continuing, make sure to have Java installed, version 11 at least.

We first start by downloading the Kafka broker from Apache's site:

https://kafka.apache.org/downloads

We recommend downloading the binary version with Scala 2.13. After download is finished, we extract the contents of the archived file.



For Windows users, it is important to rename the folder from 'kafka_2.13-3.x.x' to just 'kafka' (the server won't start if the path has special characters).

We can then change the config files in the /config/ folder as we like. To change Zookeeper's config (which is a server coordinator), edit the zookeeper.properties file, and to edit Kafka's config, edit the server.properties file.

It is recommended to change the directory of logs in Zookeeper's config. We do this by appending a line at the end of zookeeper.properties file:

```text
fileslog.dirs=C:\kafka\kafka-logs
```
or
```text
fileslog.dirs=/d/hpc/projects/FRI/data/kafka-logs
```

We can now run Zookeeper and Kafka instances. We do this by entering the kafka root folder (where we extracted the archived file) and running the following two commands in two separate terminal sessions:

```bash
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```
and 
```bash 
 .\bin\windows\kafka-server-start.bat .\config\server.properties
```

Note: different commands are used to start Zookeeper and Kafka in Linux (e.g. on a cluster). Just omit the \windows\ folder in the commands to start them in Linux.

That is it! Now the Kafka broker is running by default on port 9092 and we can start creating topics and submitting the data to the broker.

### Simulating Kafka producers with Pandas DataFrames

If we do not have any processes that actually generate the Kafka messages, or if we want to stream rows of dataframes, we can use the Pandas library to read data from CSV or Parquet files, and then use *kafka-python* library to send the messages to the broker. We first need to install the kafka-python library:

```shell
pip install kafka-python
```

An example of this is found in \src\kafka_producer.py Python script. The script first connects to the Kafka broker running on localhost:9092, then reads each partition of a Parquet file, so it can fit into the working memory. It then iterates through each row of the dataframe, and sends the dataframe row to the broker. We limit the bandwidth of the messages by utilizing *time.sleep()* after we send a message. We can run the script by navigating to the root folder and executing:

```shell
python .\src\kafka_producer.py
```


