#								                SparkSQL
#===================================================================================================================================================
#1. Create tables for movies.dat, users.dat and ratings.dat: Saving Tables from Spark SQL
#2. Find the list of the oldest released movies.
#3. How many movies are released each year?
#4. How many number of movies are there for each rating?
#5. How many users have rated each movie?
#6. What is the total rating for each movie?
#7. What is the average rating for each movie?

#===================================================================================================================================================
#===================================================================================================================================================

#1. Create tables for movies.dat, users.dat and ratings.dat: Saving Tables from Spark SQL
#----------------------------------------------------------------------------------------
#To create tables for movies.dat, users.dat, and ratings.dat in Spark SQL, you can follow these steps:
print("1. Create tables for movies.dat, users.dat and ratings.dat: Saving Tables from Spark SQL ?")

#1. Create a SparkSession object:

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CreateTablesExample").getOrCreate()

#2. Define the schema for each DataFrame using the StructType and StructField classes:

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

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

ratings_schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("movie_id", IntegerType(), True),
    StructField("rating", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])


#3. Read the data from the files using the spark.read method:

movies_df = spark.read.format("csv") \
          .option("delimiter", "::") \
          .schema(movies_schema) \
          .load("/movieData/dataset/movies.dat")

ratings_df = spark.read.format("csv") \
          .option("delimiter", "::") \
          .schema(ratings_schema) \
          .load("/movieData/dataset/ratings.dat")

users_df = spark.read.format("csv") \
          .option("delimiter", "::") \
          .schema(users_schema) \
          .load("/movieData/dataset/users.dat")



#4. Create temporary tables for each DataFrame using the "createOrReplaceTempView" method:

# Registering the temporary tables
movies_df.createOrReplaceTempView("movies")
ratings_df.createOrReplaceTempView("rating")
users_df.createOrReplaceTempView("user")

spark.sql("select * from movies").show(10)
spark.sql("select * from rating").show(10)
spark.sql("select * from user").show(10)

# #5. Save the DataFrames as permanent tables in a Hive metastore using the saveAsTable() function.

# # Saving the tables as permanent tables in Hive metastore
# # movies_df.write.saveAsTable("movies_table")
# # users_df.write.saveAsTable("users_table")
# # ratings_df.write.saveAsTable("ratings_table")


#=====================================================================================================================

#2. Find the list of the oldest released movies.
#-----------------------------------------------
print("2. Find the list of the oldest released movies ?")
# Follow  these steps to query with respect to year in title column.

#1. First we have to import split, trim, substring, regexp_extract
from pyspark.sql.functions import split, trim, substring, regexp_extract

#2. Extract the year as column "year" from the "movie" field using a regular expression
movies_year_df = movies_df.withColumn("year", regexp_extract(trim(substring("title", -6, 6)), "\d{4}", 0))
movies_year_df.show(10)

#3. create temp view for 'movies_year_df' dateframe
movies_year_df.createOrReplaceTempView("moviesbyYear")

#4. Now we can see top 10 oldest movies
spark.sql("SELECT title as movies, year FROM moviesbyYear \
            ORDER BY year ASC").show(10)

# This query will select all columns from the movies table, sort the result by title in ascending order, 
# and limit the result to the first 10 rows.

#===================================================================================================================================

#3. How many movies are released each year?
#------------------------------------------

print("3. How many movies are released each year?")

spark.sql("SELECT distinct year, count(title) as movie_count FROM moviesbyYear \
            GROUP BY year \
            ORDER BY year").show()

#===================================================================================================================================

#4. How many number of movies are there for each rating?
#-------------------------------------------------------

print("4. How many number of movies are there for each rating?")

spark.sql("SELECT r.rating, COUNT(m.movie_id) AS movie_count \
            FROM moviesbyYear m INNER JOIN rating r \
            ON m.movie_id = r.movie_id \
            GROUP BY r.rating").show()

#===================================================================================================================================

#5. How many users have rated each movie?
#----------------------------------------
print("5. How many users have rated each movie?")

spark.sql("SELECT movie_id, COUNT(DISTINCT user_id) as user_count FROM rating \
            GROUP BY movie_id \
            ORDER BY movie_id").show()

#===================================================================================================================================

#6. What is the total rating for each movie?
#-------------------------------------------
print("6. What is the total rating for each movie?")

spark.sql("SELECT movie_id, COUNT(rating) AS Tot_rating FROM rating \
            GROUP BY movie_id \
            ORDER BY movie_id").show()

#==================================================================================================================================

#7. What is the average rating for each movie?
#---------------------------------------------
print("7. What is the average rating for each movie?")

spark.sql("SELECT distinct movie_id, \
            ROUND(AVG(rating) OVER(PARTITION BY movie_id),2) AS AVG_Rating \
            FROM rating \
            ORDER BY AVG_Rating DESC").show()

#******************************************************END OF CODE*********************************************************************	   
