from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg

spark = SparkSession.builder.appName("cacheandcheckpoint").master("local[*]").getOrCreate()

spark.sparkContext.setCheckpointDir("checkpoint_dir")


users_df = spark.read.csv("users.csv", header=True, inferSchema=True)
activity_df = spark.read.csv("activity.csv", header=True, inferSchema=True)

filter_age_df = users_df.filter("age > 20")

joined_df = activity_df.join(filter_age_df, users_df.id == activity_df.user_id, how = "inner")

pipeline_df = joined_df.groupby('name', 'region').agg(
    sum('duration_min').alias('total_duration'),
    avg('duration_min').alias('avg_duration')
)
cache_df = pipeline_df.cache()

sorted_cache_df=cache_df.sort('name')

cache_df.show()

checkpoint_df = pipeline_df.checkpoint(eager=True)
sorted_checkpoint_df = checkpoint_df.sort('name')
sorted_checkpoint_df.show()

# spark.stop()
input()
