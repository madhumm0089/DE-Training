from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import *

builder = SparkSession.builder \
    .appName("SCD-Type-2") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

incoming_df = [
    ("C011", "Alice Smith", "New York", "12345"),
    ("C012", "Bob Brown", "San Francisco", "54321"),
    ("C013", "Charlie White", "Boston", "88888")
]
columns = ["customer_id", "customer_name", "address", "contact"]

incoming_df = spark.createDataFrame(incoming_df, columns)\
    .withColumn("load_time", current_timestamp())\
    .withColumn("end_time", lit(None).cast("timestamp"))\
    .withColumn("is_current", lit(True))\
    .withColumn("record_hash", sha2(concat_ws("||", *columns), 256))

sat_path = "delta/vault/sat_customer"

try:
    existing_df = spark.read.format("delta").load(sat_path).filter("is_current = True")
except:
    existing_df = spark.createDataFrame([], incoming_df.schema)

changes_df = incoming_df.alias("incoming") \
    .join(existing_df.alias("existing"), on="customer_id", how="left") \
    .filter("incoming.record_hash != existing.record_hash OR existing.record_hash IS NULL") \
    .select("incoming.*")

changes_df = changes_df.withColumn(
    "sat_customer_HK", sha2(concat_ws("||", "customer_id"), 256)
)

updates_to_expire = changes_df.select("customer_id").join(existing_df, "customer_id") \
    .withColumn("is_current", lit(False)) \
    .withColumn("end_time", current_timestamp())

final_df = existing_df.subtract(updates_to_expire.select(existing_df.columns))\
    .unionByName(updates_to_expire)\
    .unionByName(changes_df)

final_df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .save("delta/vault/sat_customer")


print("SCD type 2 table:")
spark.read.format("delta").load(sat_path).show(truncate=False)
