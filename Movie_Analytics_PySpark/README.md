# Big Data Project using Google Dataproc, PySpark, and Python
This project is a big data solution that uses Google Dataproc, Apache Spark, and Python/Scala to analyze a semi-structured dataset of movie ratings. The dataset used in this project is the MovieLens dataset containing one million records.



## Objective
The objective of this project is to showcase the use of big data technologies to answer analytical questions on a large-scale dataset. The project aims to demonstrate the capabilities of Google Dataproc Hadoop cluster with pySpark, Hive and Python in processing, analyzing data.



## Architecture
The system architecture of the project consists of the following components:

#### Google Cloud Storage: 
Used to store the raw data files in the form of CSV files.
#### Google BigQuery: 
Used to explore and clean the data using SQL queries.
#### Google Dataproc: 
Used to create a Hadoop cluster to process the data using Apache Spark and PySpark.
#### Apache Hive Metastore: 
Used to store the processed data tables in a centralized location for easy access and querying.
#### Jupyter Notebook: 
Used to submit PySpark jobs to the Dataproc cluster for data processing and analysis.



## Setup
To run this project, you will need the following:

#### A Google Cloud Platform account with billing enabled.
#### A Google Cloud Storage bucket to store the raw data files.
#### A Google BigQuery dataset to explore and clean the data.
#### A Google Dataproc cluster to process the data using PySpark and Python.
#### Make sure that PySpark and Python installed on master/manager node is working fine.
#### Jupyter Notebook should congigured with Dataproc Hadoop cluster.



## Running the Project
To run the project, follow these steps:

Upload the raw data files to a Google Cloud Storage bucket.

Use Google BigQuery to explore and clean the data using SQL queries.

Create a Google Dataproc cluster with one master node and two worker nodes.

Connect to the cluster using SSH.

Use PySpark to process and analyze the data using the Python programming language.

Save the processed data tables in the Apache Hive Metastore for easy querying.

Use Jupyter Notebook to submit PySpark jobs to the Dataproc cluster for data processing and analysis.



## Conclusion
This project demonstrates the use of big data technologies to process and analyze large-scale datasets. By using Google Dataproc, Apache Spark, and Python, we were able to perform complex data processing and analysis tasks quickly and efficiently. The project highlights the importance of using the right tools and technologies for processing and analyzing big data, and how these tools can help to unlock valuable insights from large-scale datasets.




