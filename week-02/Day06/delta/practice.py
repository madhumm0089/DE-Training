from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta.tables import DeltaTable
import os

# Setup SparkSession
builder = SparkSession.builder \
    .appName("DeltaLakeExample") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.delta.logStore.class", "org.apache.spark.sql.delta.storage.LocalLogStore") \
    .config("spark.hadoop.io.native.lib.available", "false")

spark = builder.getOrCreate()

delta_path = "output/"

df = spark.read.csv('data.csv', header=True, inferSchema=True)
df.write.format("delta").mode('overwrite').save(delta_path)

print("Delta Table Content:")
spark.read.format("delta").load(delta_path).show()

DeltaTable.forPath(spark, delta_path).vacuum(0.0)

log_path = os.path.join(delta_path, "_delta_log")
print("Files inside _delta_log:")
print(os.listdir(log_path))