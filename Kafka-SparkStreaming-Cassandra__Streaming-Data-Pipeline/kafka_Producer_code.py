from kafka import KafkaProducer
import json
import random
import time
import pandas as pd

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], \
                         value_serializer=lambda m: json.dumps(m).encode('utf-8'))

# Read data from local CSV file
iris_data = pd.read_csv('/Downloads/Kaggle_Dataseta/iris.csv')

# Send fake Iris data to Kafka topic with 1 sec delay
while True:
    # Generate random index to select random row from dataset
    idx = random.randint(0, len(iris_data) - 1)
    
    # Extract random Iris data from dataset
    sepal_length = iris_data.iloc[idx]['sepal_length']
    sepal_width = iris_data.iloc[idx]['sepal_width']
    petal_length = iris_data.iloc[idx]['petal_length']
    petal_width = iris_data.iloc[idx]['petal_width']
    target = iris_data.iloc[idx]['target']
    
    # Create message payload
    message = {
        "sepal_length": sepal_length,
        "sepal_width": sepal_width,
        "petal_length": petal_length,
        "petal_width": petal_width,
        "target": target
    }
    
    # Send message to Kafka topic
    producer.send('stream_data', message)
    time.sleep(1)
