import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min, desc, round, lit
from pyspark.sql.types import FloatType  # Import FloatType

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

#load the data
input_path = f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df = spark.read.option("header", True).csv(input_path)
df = df.withColumn("Rating", col("Rating").cast(FloatType()))

#extracts the two cities with the highest and lowest average rating per restaurant
df_filtered = df.filter(
    col("City").isNotNull() &
    col("Rating").isNotNull()
)

avg_rating_df = df_filtered.groupBy("City").agg(
    round(avg("Rating"), 10).alias("AverageRating")
)

top_2 = avg_rating_df.orderBy(col("AverageRating").desc()).limit(2).withColumn("RatingGroup", lit("Top"))
bottom_2 = avg_rating_df.orderBy(col("AverageRating").asc()).limit(2).withColumn("RatingGroup", lit("Bottom"))

#Combine top and bottom
combined_df = top_2.unionByName(bottom_2).orderBy(desc("RatingGroup"), desc("AverageRating"))

# Write the result to HDFS
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question3/"
combined_df.write.mode("overwrite").csv(output_path, header=True)