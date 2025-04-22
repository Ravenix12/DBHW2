import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, collect_list, array, asc, struct
from pyspark.sql.types import StringType, ArrayType
from itertools import combinations

# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
input_path = f"hdfs://{hdfs_nn}:9000/assignment2/part2/input/tmdb_5000_credits.parquet"
df = spark.read.option("header", True).csv(input_path)

def generate_pairs(cast):
    actors = [actor.strip() for actor in cast.split(',')] if cast else []
    pairs = set()
    for a1, a2 in combinations(actors, 2):
        # Always sort the pair to avoid duplicates
        pairs.add(tuple(sorted((a1, a2))))
    return list(pairs)

# Register UDF
from pyspark.sql.functions import udf
generate_pairs_udf = udf(generate_pairs, ArrayType(ArrayType(StringType())))

# Apply UDF to get actor pairs
pairs_df = df.withColumn("actor_pairs", generate_pairs_udf(col("cast")))
pairs_exploded = pairs_df.select("movie_id", "title", explode("actor_pairs").alias("pair"))

# Separate actor1 and actor2 into columns
cleaned_pairs = pairs_exploded.select(
    col("movie_id"),
    col("title"),
    col("pair")[0].alias("actor1"),
    col("pair")[1].alias("actor2")
)

# Count how many times each actor pair has appeared together
from pyspark.sql.functions import count

pair_counts = cleaned_pairs.groupBy("actor1", "actor2").count()

# Filter those who appeared together at least twice
repeated_pairs = pair_counts.filter(col("count") >= 2).select("actor1", "actor2")

# Join back to original to get only rows where actor pairs appeared at least twice
result_df = cleaned_pairs.join(repeated_pairs, on=["actor1", "actor2"])

output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question5/"
result_df.write.mode("overwrite").csv(output_path, header=True)