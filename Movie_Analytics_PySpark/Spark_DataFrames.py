#                                           Spark Data Frames
#==============================================================================================================================
# 1. Prepare Movies data: Extracting the Year and Genre from the Text
# 2. Prepare Users data: Loading a double delimited csv file
# 3. Prepare Ratings data: Programmatically specifying a schema for the data frame
# 4. Import Data from URL: Scala
# 5. Save table without defining DDL in Hive
# 6. Broadcast Variable example
# 7. Accumulator example

#=============================================================================================================================
#=============================================================================================================================

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import split, trim, substring, regexp_extract

# Create a SparkSession
spark = SparkSession.builder.appName("MovieLens Analysis").enableHiveSupport().getOrCreate()

# Define the schema for each DataFrame
movies_schema = StructType([
    StructField("movie_id", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True)
])

users_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("occupation", IntegerType(), True),
    StructField("zipcode", StringType(), True)
])


#===============================================================================================================================

# 1. Prepare Movies data: Extracting the Year and Genre from the Text.
#---------------------------------------------------------------------

# Read the data from the movies.dat using the spark.read method:
movies_df = spark.read.format("csv") \
          .option("delimiter", "::") \
          .schema(movies_schema) \
          .load("/movieData/dataset/movies.dat")

print("1. Prepare Movies data: Extracting the Year and Genre from the Text.")

# Extract the year as column "year" from the "movie" field using a regular expression
movies_df = movies_df.withColumn("year", regexp_extract(movies_df["title"], r"\((\d{4})\)", 1))

# Extract the genre(s) from the "genres" field using the split function
movies_df = movies_df.withColumn("genres", split("genres", "\|"))

# Show Data Frame
movies_df.show()

#===============================================================================================================================

# 2. Prepare Users data: Loading a double delimited csv file.
#------------------------------------------------------------

print("2. Prepare Users data: Loading a double delimited csv file.")

users_df = spark.read.format("csv") \
          .option("delimiter", "::") \
          .schema(users_schema) \
          .load("/movieData/dataset/users.dat")

# Show dataframes
users_df.show()

#=================================================================================================================================

# 3. Prepare Ratings data: Programmatically specifying a schema for the data frame.
#----------------------------------------------------------------------------------
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

# define schema
ratings_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

print("3. Prepare Ratings data: Programmatically specifying a schema for the data frame.")

ratings_df = spark.read.format("csv") \
          .option("delimiter", "::") \
          .schema(ratings_schema) \
          .load("/movieData/dataset/ratings.dat")

# show ratings_df data frame
ratings_df.show()

#================================================================================================================================

#4. Import data from a URL using Scala code
#------------------------------------------
print("4. Import data from a URL using Scala code")
# scala_code = """
# import org.apache.spark.sql.functions._
# val url = "http://example.com/data.csv"
# val df = spark.read.format("csv").option("header", true).load(url)
# df.show()
# """
# spark.sparkContext.addFile("http://example.com/example.scala")
# spark.sparkContext.setCheckpointDir("/tmp")
# spark.sparkContext.runJob(spark.sparkContext.parallelize([1]), lambda x: exec(scala_code))
print("""

    import org.apache.spark.sql.functions._
    val url = "http://example.com/data.csv"
    val df = spark.read.format("csv").option("header", true).load(url)
    df.show()

    spark.sparkContext.addFile("http://example.com/example.scala")
    spark.sparkContext.setCheckpointDir("/tmp")
    spark.sparkContext.runJob(spark.sparkContext.parallelize([1]), lambda x: exec(scala_code))
""")

#================================================================================================================================

# 5. Save table without defining DDL in Hive?
#--------------------------------------------

print("""
5. Save table without defining DDL in Hive?

movies_df.write \
    .mode("overwrite") \
    .format("hive") \
    .saveAsTable("my_hive_table")


# Read the hive table back into a DataFrame
table_df = spark.table(my_hive_table)

# Query the table using Spark SQL
table_df.createOrReplaceTempView(movie)
result = spark.sql("SELECT name FROM movie WHERE movie_id > 30")

# Show the query result
result.show()
""")

#================================================================================================================================

# 6. Broadcast Variable example ?

# imort required library
from pyspark.sql.functions import broadcast

print("6. Broadcast Variable example ?")

print("""
-------------------------------------------------------------------------------------------------------

A broadcast variable in Apache Spark is a read-only variable that is cached and made available to 
all worker nodes in a distributed computation, thus reducing the communication overhead and improving 
the performance of certain operations. Broadcast variables are useful when you need to use a large 
read-only dataset, such as a lookup table or a configuration file, in a Spark job.

Here is an example code snippet in Python that demonstrates the usage of a broadcast variable in Spark:
-------------------------------------------------------------------------------------------------------

""")

# dataframe 
data = [
    (1, "academic/educator"),
	(2, "artist"),
	(3, "clerical/admin"),
	(4, "college/grad student"),
	(5, "customer service"),
	(6, "doctor/health care"),
	(7, "executive/managerial"),
	(8, "farmer"),
	(9, "homemaker"),
	(10, "K-12 student"),
	(11, "lawyer"),
	(12, "programmer"),
	(13, "retired"),
	(14, "sales/marketing"),
	(15, "scientist"),
	(16, "self-employed"),
	(17, "technician/engineer"),
	(18, "tradesman/craftsman"),
	(19, "unemployed"),
	(20, "writer")
    ]

# schema
schema = StructType([
    StructField("occupation", IntegerType(), True),
    StructField("occupation_name", StringType(), True)
])

# Load the data

rdd = spark.sparkContext.parallelize(data)
occupation_df = spark.createDataFrame(rdd, schema)
occupation_df.show()

# Create a broadcast variable from the Occupation DataFrame
occupation_broadcast = broadcast(occupation_df)

# Join the orders and customers DataFrames using the broadcast variable
joined_df = users_df.join(occupation_broadcast, "occupation")

# Show the results
joined_df.show()

print("""
---------------------------------------------------------------------------------------------------------------------------------
In this example code, we are using a broadcast variable to join two DataFrames in Spark. Here is how it works:

1. We create two dataframes into Spark: users_df and occupation_df.

2. We create a broadcast variable from the occupation_df DataFrame using the broadcast() function.
 
3. We join the users_df and occupation_df DataFrames using the "occupation" column and the broadcast variable. 
The broadcast variable is used to avoid shuffling the entire customers DataFrame across the network, 
which would be inefficient for large datasets.

4. We show the results of the join operation using the show() function.

5. By using a broadcast variable, we can efficiently join large datasets in Spark without incurring the overhead of shuffling 
the data across the network.

=================================================================================================================================
""")


#================================================================================================================================

# 7. Accumulator example
#----------------------- 

print("""
7. Accumulator example


==================================================================================================
					Accumulator Variable
--------------------------------------------------------------------------------------------------

An accumulator in Apache Spark is a write-only variable that can be used to accumulate values across 
multiple worker nodes in a distributed computation. Accumulators are useful when you need to perform a 
side-effect operation, such as counting or summing values, in a Spark job.

Here is an example code snippet in Python that demonstrates the usage of an accumulator in Spark:
---------------------------------------------------------------------------------------------
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Accumulator Example").getOrCreate()

# Load the data
numbers = [1, 2, 3, 4, 5]
numbers_rdd = spark.sparkContext.parallelize(numbers)

# Create an accumulator with an initial value of 0
sum_accumulator = spark.sparkContext.accumulator(0)

# Define a function to add each number to the accumulator
def add_to_accumulator(number):
    global sum_accumulator
    sum_accumulator += number

# Apply the function to each number in the RDD
numbers_rdd.foreach(add_to_accumulator)

# Print the value of the accumulator
print("The sum of the numbers is:", sum_accumulator.value)
-----------------------------------------------------------------------------------------------

In this example code, we are using an accumulator to compute the sum of a list of numbers in Spark. Here is how it works:

1. We create an RDD from a list of numbers.
2. We create an accumulator with an initial value of 0 using the accumulator() method of the SparkContext object.
3. We define a function add_to_accumulator() that takes a number as input and adds it to the accumulator.
4. We apply the add_to_accumulator() function to each number in the RDD using the foreach() method.
5. We print the value of the accumulator using the value attribute of the accumulator object.

By using an accumulator, we can efficiently compute side-effect operations in Spark without the overhead of collecting data 
to the driver node. Accumulators are particularly useful when we need to perform computations that involve side effects, 
such as counting or summing values.
""")

#=======================================================END======================================================================