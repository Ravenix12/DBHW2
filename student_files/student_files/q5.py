import sys 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, collect_list, array, asc, struct
from pyspark.sql.types import StringType, ArrayType
from itertools import combinations
import json
from pyspark.sql.functions import udf
from pyspark.sql.functions import count


# you may add more import if you need to

# don't change this line
hdfs_nn = sys.argv[1]

spark = SparkSession.builder.appName("Assigment 2 Question 5").getOrCreate()
# YOUR CODE GOES BELOW
input_path = f"hdfs://{hdfs_nn}:9000/assignment2/part2/input/tmdb_5000_credits.parquet"
df = spark.read.parquet(input_path)

# UDF to parse 'cast' JSON and return sorted actor pairs
def extract_pairs(cast_json):
    try:
        cast = json.loads(cast_json)
        names = sorted([actor['name'] for actor in cast])
        return [sorted(list(pair)) for pair in combinations(names, 2)]
    except:
        return []

schema = ArrayType(ArrayType(StringType()))
generate_pairs_udf = udf(extract_pairs, schema)

pairs_df = df.withColumn("actor_pairs", generate_pairs_udf(col("cast")))

exploded_df = pairs_df.select(
    col("movie_id"),
    col("title"),
    explode("actor_pairs").alias("pair")
)

pair_df = exploded_df.select(
    "movie_id",
    "title",
    col("pair")[0].alias("actor1"),
    col("pair")[1].alias("actor2")
)


pair_count_df = pair_df.groupBy("actor1", "actor2").count().filter("count >= 2").select("actor1", "actor2")

# Join back to original to get full rows with movie_id and title
result_df = pair_df.join(pair_count_df, on=["actor1", "actor2"], how="inner").select("movie_id", "title", "actor1", "actor2")


output_path = f"hdfs://{hdfs_nn}:9000/assignment2/output/question5/"
result_df.write.mode("overwrite").parquet(output_path)