import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 3").getOrCreate()
# YOUR CODE GOES BELOW

#load the data
input_path = f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df = spark.read.option("header", True).csv(input_path)

#extracts the two cities with the highest and lowest average rating per restaurant


# Write the result to HDFS
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question3/"
combined_df.write.mode("overwrite").csv(output_path, header=True)