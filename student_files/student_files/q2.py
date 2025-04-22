import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max, min, round, desc
from pyspark.sql.types import FloatType  # Import FloatType

# you may add more import if you need to


# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 2").getOrCreate()
# YOUR CODE GOES BELOW

#load the data
input_path = f"hdfs://{hdfs_nn}:9000/assignment2/part1/input/TA_restaurants_curated_cleaned.csv"
df = spark.read.option("header", True).csv(input_path)
df = df.filter(~(col("Name") == "Name"))
df = df.withColumn("Rating", col("Rating").cast(FloatType()))

#finds the best and the worst restaurants for each city for each price range in terms of rating
df_filtered=df.filter(
    col("Price Range").isNotNull() & 
    col("City").isNotNull() & 
    col("Rating").isNotNull()
    )

#print(f"\033[32mFiltered DataFrame count: {df_filtered.count()}\033[0m")  # Green for valid data

grouped_df = df_filtered.groupBy("City", "Price Range").agg(
    max("Rating").alias("Max Rating"),
    min("Rating").alias("Min Rating")
)

#print(f"\033[32mFiltered DataFrame count: {grouped_df.count()}\033[0m")  # Green for valid data


best_restaurants = df_filtered.join(
    grouped_df,
    (df_filtered["City"] == grouped_df["City"]) &
    (df_filtered["Price Range"] == grouped_df["Price Range"]) &
    (df_filtered["Rating"] == grouped_df["Max Rating"])
).select(df_filtered["*"])

#print(f"\033[32mFiltered DataFrame count: {best_restaurants.count()}\033[0m")  # Green for valid data

worst_restaurants = df_filtered.join(
    grouped_df,
    (df_filtered["City"] == grouped_df["City"]) &
    (df_filtered["Price Range"] == grouped_df["Price Range"]) &
    (df_filtered["Rating"] == grouped_df["Min Rating"])
).select(df_filtered["*"])

#print(f"\033[32mFiltered DataFrame count: {worst_restaurants.count()}\033[0m")  # Green for valid data

# Combine best and worst restaurants
combined_df = best_restaurants.unionByName(worst_restaurants).dropDuplicates()

# Write the result to HDFS
output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question2/"
combined_df.write.mode("overwrite").csv(output_path, header=True)
print(f"\033[32mReached here\033[0m")  # Green for valid data