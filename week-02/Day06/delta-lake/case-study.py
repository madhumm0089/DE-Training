from pyspark.sql import SparkSession
import os
from pathlib import Path

# Start SparkSession with Delta support
spark = SparkSession.builder \
    .appName("case-study") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()


# data = [
#     (1, 'Alice', 'UK'),
#     (2, 'Bob', 'Germany'),
#     (3, 'Carlos', 'Spain')
# ]

# Create DataFrame
# df = spark.createDataFrame(data, ['user_id', 'name', 'country'])

# (Optional) Write as Parquet
# df.write.mode("overwrite").parquet("tmp/customer_parquet")

# Read back from Parquet
# parquet_df = spark.read.parquet("tmp/customer_parquet")

# Write as Delta format
# parquet_df.write.format("delta").mode("overwrite").save("/tmp/customer_delta")

# Register Delta table with SQL (using full path)
# spark.sql("CREATE TABLE customer_delta USING DELTA LOCATION '/tmp/customer_delta'")


# Query registered Delta table
# spark.sql("SELECT * FROM delta.`/tmp/customer_delta`").show()

# spark.sql("Delete from delta.`/tmp/customer_delta` where user_id = 2")
# spark.sql("select * from delta.`/tmp/customer_delta`").show()
spark.sql("select * from delta.`/tmp/customer_delta` version as of 0").show()

# Stop SparkSession
spark.stop()




# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col
# from delta import configure_spark_with_delta_pip

# builder = SparkSession.builder \
#     .appName("Case Study Example") \
#     .master("local[*]") \
#     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
#     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
#     .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# spark = configure_spark_with_delta_pip(builder).getOrCreate()

# data = [
#     (1, 'Alice', 'UK'),
#     (2, 'Bob', 'Germany'),
#     (3, 'Carlos', 'Spain')
# ]

# df = spark.createDataFrame(data, ['user_id', 'name', 'country'])

# df.write.mode("overwrite").parquet('/tmp/customer_parquet')

# parquet_df = spark.read.parquet("/tmp/customer_parquet")


# parquet_df.write.mode("overwrite").format("delta").save("/tmp/customer_delta")


# spark.sql("CREATE TABLE IF NOT EXISTS customer_delta USING DELTA LOCATION '/tmp/customer_delta'")

# spark.sql("SELECT * FROM customer_delta").show()

# spark.sql("DELETE FROM customer_delta WHERE user_id = 2")


# spark.sql("SELECT * FROM customer_delta").show()
