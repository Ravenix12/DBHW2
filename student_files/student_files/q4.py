import sys
from pyspark.sql import SparkSession

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 4").getOrCreate()
# YOUR CODE GOES BELOW

#load the data
input_path = f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df = spark.read.option("header", True).csv(input_path)

#counts the number of restaurants by city and cuisine style
df_filtered = df.filter(
    col("City").isNotNull() & 
    col("Cuisine Style").isNotNull()
)
print(f"\033[32mFiltered DataFrame count: {df_filtered.count()}\033[0m")  # Green for valid data

grouped_df = df_filtered.groupBy("City", "Cuisine Style").count()
result_df = grouped_df.withColumnRenamed("Cuisine Style", "Cuisine").withColumnRenamed("count", "count")
print(f"\033[32mFiltered DataFrame count: {result_df.count()}\033[0m")  # Green for valid data

# Write the result to HDFS
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question4/"
result_df.write.mode("overwrite").csv(output_path, header=True)
print(f"\033[32mReached here\033[0m")  # Green for valid data