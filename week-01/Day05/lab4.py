from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg

spark = SparkSession.builder.appName("activity_pipeline").master("local[*]").getOrCreate()

# Step 1: Load CSVs
users_df = spark.read.csv("users.csv", header=True, inferSchema=True)
activity_df = spark.read.csv("activity.csv", header=True, inferSchema=True)

# Step 2: Filter users with age > 25
filtered_users_df = users_df.filter(col("age") > 25)

# Step 3: Join with activity logs
joined_df = activity_df.join(filtered_users_df, activity_df.user_id == filtered_users_df.id, how="inner")

# Step 4: Aggregate total and average activity time per user
summary_df = joined_df.groupBy("name", "region").agg(
    _sum("duration_min").alias("total_duration"),
    avg("duration_min").alias("average_duration")
)

# Step 5: Filter users with total_duration > 50
final_df = summary_df.filter(col("total_duration") > 50)

# Step 6: Save to Parquet
final_df.write.mode("overwrite").parquet("output/active_users_summary")
# Read parquet file or folder
df = spark.read.parquet("output\active_users_summary")

# Show data (prints to console)
df.show()

