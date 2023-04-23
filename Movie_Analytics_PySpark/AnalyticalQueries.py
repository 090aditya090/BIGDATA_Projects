#                                               Analytical Queries
#===============================================================================================================================

# 1. What are the top 10 most viewed movies?
# 2. What are the distinct list of genres available?
# 3. How many movies for each genre?
# 4. How many movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)?
# 5. List the latest released movies

#===============================================================================================================================
# import required Library and spark Session
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import split, trim, substring, regexp_extract
from pyspark.sql.functions import *

# Create a SparkSession
spark = SparkSession.builder.appName("Query And Analysis").getOrCreate()

print("""
                                            ANALYTICAL QUARIES
                                           --------------------

1. What are the top 10 most viewed movies?
2. What are the distinct list of genres available?
3. How many movies for each genre?
4. How many movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)?
5. List the latest released movies

=================================================================================================================================
""")

# create fields for dataframes
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


# Read the data from the files using the spark.read method:

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

#================================================================================================================================

# show all dataframe
movies_df.show()
ratings_df.show()
users_df.show()

# 1. What are the top 10 most viewed movies?
#-------------------------------------------
print("1. What are the top 10 most viewed movies?")

# group the DataFrame by movie_id and count the number of occurrences
# and sort by desending order on count 
sorted_view_count_df = ratings_df.groupBy('movie_id').count().sort(desc('count')).limit(10)

# join the sorted_view_count_df with result_df to get the movie titles
top_movies_df = sorted_view_count_df.join(movies_df, 'movie_id')

# select the top 10 movies by count and show the results
top_10_movies_df = top_movies_df.select('title', 'count').sort(desc('count'))
top_10_movies_df.show()

#===============================================================================================================================

# 2. What are the distinct list of genres available?
#---------------------------------------------------

print("2. What are the distinct list of genres available?")

distinct_genres = movies_df.select("genres").distinct()
distinct_genres.show(truncate=False)

#===============================================================================================================================

# 3. How many movies for each genre?
#-----------------------------------
print("3. How many movies for each genre?")

movies_per_genre = movies_df.groupBy("genres").agg(count("movie_id").alias("move_count"))
movies_per_genre.show(truncate = False)

#===============================================================================================================================

# 4. How many movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)?
#---------------------------------------------------------------------------------------------------

print("4. How many movies are starting with numbers or letters (Example: Starting with 1/2/3../A/B/C..Z)?")

movies_starting_with_numbers = movies_df.filter(regexp_extract(col("title"), "^[0-9]", 0) != "")
movies_starting_with_letters = movies_df.filter(regexp_extract(col("title"), "^[^0-9]", 0) != "")
num_movies_starting_with_numbers = movies_starting_with_numbers.count()
num_movies_starting_with_letters = movies_starting_with_letters.count()
print(num_movies_starting_with_letters + num_movies_starting_with_numbers)

#================================================================================================================================

# 5. List the latest released movies
#-----------------------------------

print("5. List the latest released movies")

# extract year from title field from movies_df dataframe
movies_year_df = movies_df.withColumn("year", regexp_extract(trim(substring("title", -6, 6)), "\d{4}", 0))

# latest releases
latest_releases = movies_year_df.select('title').orderBy(desc('year')).limit(10)
latest_releases.show(truncate = False)

#=============================================================END================================================================