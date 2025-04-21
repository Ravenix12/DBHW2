import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min, round, desc
# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 1").getOrCreate()
# YOUR CODE GOES BELOW

#load the data
input_path = f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df = spark.read.option("header", True).csv(input_path)

#removing rows with no reviews or rating < 3.0 as we want to only show positive reviews
df_filtered = df.filter(df["Number of Reviews"] > 0)
df_filtered = df.filter(df["Rating"] >= 3.0)

#writing the data to hdfs
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question1/"
df_filtered.write.mode("overwrite").csv(output_path, header=True)


