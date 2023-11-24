#!/usr/bin/env python
# coding: utf-8

# Newday Technical Assessment


# Newday Technical Assessment - by Adam Noble

# Declare the imports that are required for the script to run.
import os
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import avg, min, max, asc, desc, rank, col
from pyspark.sql.functions import from_unixtime


# Task 1 - Read in movies.dat and ratings.dat to spark dataframes.


# Create the Spark Session, with App Name 'Newday Technical Task'
spark = SparkSession.builder.appName('Newday Technical Task').getOrCreate()
        
# Declare the relative file path to the Movie Data.
pathMovieData = "data/movies.dat"

# Declare the relative file to the Ratings Data.
pathRatingData = "data/ratings.dat"

# Declare the Movie Schema.
movieSchema = StructType([
    StructField("Movie ID", IntegerType(), False),
    StructField("Movie Title (Year)",StringType(),True),
    StructField("Movie Genre", StringType(), True)
])

# Read the Movie Data from the .dat file, recognising the data is delimited by '::'. Use the schema above, and convert the data into a Risilient Distributed Dataset (RDD) 
# for usage with the 'createDataFrame function'.
moviesData = spark.read.option("delimiter", "::").csv(pathMovieData, header=False, schema=movieSchema).rdd.map(lambda x: (x[0], x[1], x[2]))

# Create the Movies Dataframe
moviesDf = spark.createDataFrame(moviesData, movieSchema)

# Declare the Ratings Schema
ratingsSchema = StructType([
    StructField("User ID", IntegerType(), False),
    StructField("Movie ID", IntegerType(), False),
    StructField("Rating",IntegerType(),True),
    StructField("Timestamp", IntegerType(), True)
])

# Read the Ratings Data from the .dat file, recognising the data is delimited by '::'. Use the schema above, and convert the data into a Risilient Distributed Dataset (RDD) 
# for usage with the 'createDataFrame function'.
ratingData = spark.read.option("delimiter", "::").csv(pathRatingData, header=False, schema=ratingsSchema).rdd.map(lambda x: (x[0], x[1], x[2], x[3]))

# Create the Ratings Dataframe
ratingDf = spark.createDataFrame(ratingData, ratingsSchema)

# Convert the Timestamp from Seconds (per the Readme) into a Timestamp with type TimestampType, and overwrite the existing Ratings Dataframe.
ratingDf = ratingDf.withColumn("Timestamp", from_unixtime("Timestamp").cast(TimestampType()))


# Task 2 - Creates a new dataframe, which contains the movies data and 3 new columns max, min and average rating for that movie from the ratings data.


# Initially aggregate the Ratings Dataframe by grouping by the Movie Id. This is to gain the minimum rating, maximum rating and average rating per Movie ID.
# Order by Movie ID for cleanliness.
movieRatingAggregationsDf = ratingDf.groupBy("Movie Id").agg(
    avg("Rating").alias("Average Rating"),
    min("Rating").alias("Minimum Rating"),
    max("Rating").alias("Maximum Rating")
).orderBy(asc("Movie Id"))

# Join the Aggregated Rating Data with the Movie Data on the 'Movie ID', this will then complete the full data set required.
moviesWithAggregateRatingsDf = movieRatingAggregationsDf.join(moviesDf,"Movie Id").select("Movie Id","Movie Title (Year)", "Movie Genre", "Minimum Rating","Maximum Rating","Average Rating").orderBy(asc("Movie Id"))


# Task 3 - Create a new dataframe which contains each user’s (userId in the ratings data) top 3 movies based on their rating.


# Create individual paritions by User Id, and order by Rating and Timestamp.
# I chose a timestamp order as there's more than 3 films per users in some cases that are top rated, so I have given the 3 most recent, top-rated reviews per user.
# There are some cases where there is the exact same timestamp for more than one film per user, and they are included in the list.
window = Window.partitionBy("User Id").orderBy(col("Rating").desc(), col("Timestamp").desc())

# Select all data from Rating Dataframe, plus the top 3 ranked films per user (in most cases, except where there is more than one film on the same timestamp).
top3PerUserDf = ratingDf.select('*', rank().over(window).alias('Rank')).filter(col("Rank") <= 3)

# Join with the Movie Dataframe to give the fuller analysis of what movies the user selected in their top 3 ranked.
top3PerUserWithDetailDf = top3PerUserDf.join(moviesDf,"Movie Id").select("User Id","Movie Id","Movie Title (Year)", col("Timestamp").alias("Time of Review"),"Rank")


# Task 4 - Write out the original and new dataframes in an efficient format of your choice.


# Write File Paths of each Dataframe

# Movies Dataframe
movieDfOutputPath = "output/movieDf.csv"
# Ratings Dataframe
ratingsDfOutputPath = 'output/ratingsDf.csv'
# movieRatingsJoinDf
movieWithAggregateRatingDfPath = 'output/movieWithAggregateRatingDf.csv'
# top3PerUserWithDetailDf
top3PerUserWithDetailDfPath = 'output/top3PerUserWithDetailDf.csv'

# Create the CSVs for each Dataframe

# Write the Movies Dataframe to CSV, using coalesce to 'glue' the partitioned data back together to create a singular CSV.
moviesDf.coalesce(1).write.mode("overwrite").option("header", True).option("delimiter",",").csv(movieDfOutputPath)
# Write the Ratings Dataframe to CSV
ratingDf.coalesce(1).write.mode("overwrite").option("header", True).option("delimiter",",").csv(ratingsDfOutputPath)
# Write the Movies with Aggregate Ratings Dataframe to CSV
moviesWithAggregateRatingsDf.coalesce(1).write.mode("overwrite").option("header", True).option("delimiter",",").csv(movieWithAggregateRatingDfPath)
# Write the Top 3 Movie Ratings Per User With Detail Dataframe to CSV
top3PerUserWithDetailDf.coalesce(1).write.mode("overwrite").option("header", True).option("delimiter",",").csv(top3PerUserWithDetailDfPath)

# End the Spark Session
SparkSession.stop()
