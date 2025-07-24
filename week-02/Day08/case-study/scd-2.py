from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date, monotonically_increasing_id
from delta import configure_spark_with_delta_pip

# Step 1: Create Spark Session with Delta support
builder = SparkSession.builder \
    .appName("SCD Type-2 Implementation") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Path to store delta table (update this if needed)
dim_customer_path = '/delta/dim_customer'

# Step 2: Create source DataFrame (incoming data)
source_df = spark.createDataFrame([
    (1, "Alice", "New York", "Gold"),
    (2, "Bob", "San Francisco", "Silver"),
    (3, "Charlie", "Los Angeles", "Platinum")
], ["CustomerID", "Name", "Address", "LoyaltyTier"])

# Step 3: Initialize dim_customer table if not exists
try:
    existing_df = spark.read.format("delta").load(dim_customer_path)
except Exception:
    print("Delta table doesn't exist. Creating for the first time...")
    scd2_df = source_df \
        .withColumn("CustomerSK", monotonically_increasing_id()) \
        .withColumn("StartDate", current_date()) \
        .withColumn("EndDate", lit(None).cast("date")) \
        .withColumn("IsCurrent", lit(True))
    
    scd2_df.write.format("delta").mode("overwrite").save(dim_customer_path)
    existing_df = spark.read.format("delta").load(dim_customer_path)

# Step 4: Join on CustomerID to compare
joined_df = source_df.alias("src").join(
    existing_df.filter("IsCurrent = true").alias("tgt"),
    on="CustomerID",
    how="left"
)

# Step 5: Detect changes in Address or LoyaltyTier
changed_df = joined_df.filter(
    (col("src.Address") != col("tgt.Address")) |
    (col("src.LoyaltyTier") != col("tgt.LoyaltyTier"))
).select("src.*")

# Step 6: Expire existing rows
expired_df = joined_df.filter(
    (col("src.Address") != col("tgt.Address")) |
    (col("src.LoyaltyTier") != col("tgt.LoyaltyTier"))
).select("tgt.*") \
 .withColumn("EndDate", current_date()) \
 .withColumn("IsCurrent", lit(False))

# Step 7: Insert new rows for changed customers
new_version_df = changed_df \
    .withColumn("CustomerSK", monotonically_increasing_id()) \
    .withColumn("StartDate", current_date()) \
    .withColumn("EndDate", lit(None).cast("date")) \
    .withColumn("IsCurrent", lit(True))

# Step 8: Union active + expired + new rows
final_df = existing_df \
    .filter("IsCurrent = true") \
    .join(expired_df.select("CustomerSK"), on="CustomerSK", how="left_anti") \
    .unionByName(expired_df) \
    .unionByName(new_version_df)

# Step 9: Overwrite the Delta table
final_df.write.format("delta").mode("overwrite").save(dim_customer_path)
final_df.orderBy("CustomerID", "StartDate").show(truncate=False)