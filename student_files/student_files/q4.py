import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

#load the data
input_path = f"hdfs://{hdfs_nn}:3000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df = spark.read.option("header", True).csv(input_path)

#counts the number of restaurants by city and cuisine style
df_filtered = df.filter(
    col("City").isNotNull() & 
    col("Cuisine Style").isNotNull()
)

grouped_df = df_filtered.groupBy("City", "Cuisine Style").count()
result_df = grouped_df.withColumnRenamed("Cuisine Style", "Cuisine").withColumnRenamed("count", "count")

# Write the result to HDFS
output_path = f"hdfs://{hdfs_nn}:3000/assignment2/output/question4/"
result_df.write.mode("overwrite").csv(output_path, header=True)