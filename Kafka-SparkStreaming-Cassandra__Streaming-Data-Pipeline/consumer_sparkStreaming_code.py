from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from kafka import KafkaConsumer
import json

# Initialize Spark and Cassandra sessions
spark = SparkSession.builder.appName("KafkaToCassandra").config("spark.cassandra.connection.host", "localhost").getOrCreate()

# Define schema for incoming Kafka messages
schema = StructType([
    StructField("sepal_length", DoubleType()),
    StructField("sepal_width", DoubleType()),
    StructField("petal_length", DoubleType()),
    StructField("petal_width", DoubleType()),
    StructField("target", StringType())
])

# Initialize Spark Streaming context with 10 second batches
ssc = StreamingContext(SparkContext.getOrCreate(), 1)

# Initialize Kafka consumer
consumer = KafkaConsumer(
    'stream_data',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda m: m.decode('utf-8'))

# Read from Kafka topic 'stream_data'
kafkaStream = ssc.queueStream([], default=consumer)

# Convert Kafka messages to structured Spark DataFrame
inputDF = kafkaStream.map(lambda x: x.value) \
    .map(lambda x: json.loads(x)) \
    .toDF(schema)

# Write result to Cassandra table 'output'
inputDF.write.format("org.apache.spark.sql.cassandra") \
    .mode('append') \
    .options(table="output", keyspace="mykeyspace") \
    .save()

# Start Spark Streaming context
ssc.start()

# Wait for the streaming context to terminate
ssc.awaitTermination()
