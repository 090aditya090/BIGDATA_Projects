# Kafka-SparkStreaming-Cassandra__Streaming-Data-Pipeline
This is end-to-end streaming project using Spark Streaming, Kafka, and Cassandra provides a scalable and fault-tolerant solution for processing and storing large volumes of streaming data. The project can be used for a wide range of applications, including real-time analytics, fraud detection, and IoT data processing.

#### DESCRIPTION:
This project is a demonstration of a data pipeline that streams data from the Iris dataset using Apache Kafka, processes the data using PySpark Streaming, and writes the result into DataStax Astra Cassandra database.

The pipeline includes a Kafka producer that generates Iris data and sends it to the 'stream_data' topic in Kafka. A Kafka consumer reads the data from the 'stream_data' topic, and a PySpark Streaming application consumes the data from Kafka, processes it, and writes the result into the 'output' table in the Astra database.

The project also includes instructions on how to configure the PySpark application to connect to the Astra database securely using SSL encryption and authentication. Overall, this project serves as a starting point for building a scalable and secure data pipeline using open-source technologies.

#### Built a data pipeline using Kafka, Spark Streaming, and DataStax Astra Cassandra to analyze and store Iris dataset
#### Configured a Kafka producer to send Iris data to a Kafka topic
#### Set up a Kafka consumer to read from the Kafka topic
#### Used Spark Streaming to process the incoming Kafka messages and convert them into a structured DataFrame
#### Loaded the processed data into a DataStax Astra Cassandra table using the Spark Cassandra connector
#### Configured Spark to connect to DataStax Astra Cassandra with SSL authentication and encryption
#### Utilized Python libraries including sklearn, json, and kafka-python to build and deploy the data pipeline
#### Managed and monitored the Spark Streaming application with PySpark APIs and console output
#### Collaborated with team members to ensure efficient and effective data processing and storage
