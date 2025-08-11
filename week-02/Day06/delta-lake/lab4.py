from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import shutil
import os

# Create SparkSession with Delta support
spark = SparkSession.builder \
    .appName("DeltaRetention") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define local file system path
# Use absolute path (double backslashes or raw string)
delta_path = "file:///C:/Users/madhu.m/Desktop/DE-Training/week-02/Day06/delta-lake/delta_retention_test"

local_path = "C:/Users/madhu.m/Desktop/DE-Training/week-02/Day06/delta-lake/delta_retention_test"

# Spark needs file URI format
delta_path = "file:///" + local_path.replace("\\", "/")

# Clean up any existing data
if os.path.exists(local_path):
    shutil.rmtree(local_path)

# Drop table if it exists
spark.sql("DROP TABLE IF EXISTS delta_retention_test")

# Create DataFrame
df = spark.createDataFrame([(1, 'alpha'), (2, 'beta'), (3, 'gamma')], ['id', 'value'])

# Write as Delta
df.write.format("delta").save(delta_path)

# Register as table
spark.sql(f"CREATE TABLE delta_retention_test USING DELTA LOCATION '{delta_path}'")

# Delete a row
spark.sql("DELETE FROM delta_retention_test WHERE id = 2")

# Show commit history
print("=== Commit History ===")
spark.sql("DESCRIBE HISTORY delta_retention_test").show(truncate=False)

# Try VACUUM with 0 hours (should fail)
try:
    spark.sql("VACUUM delta_retention_test RETAIN 0 HOURS")
except Exception as e:
    print("\nExpected error during VACUUM with 0 HOURS:\n", e)

# Disable safety check and retry VACUUM
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.sql("VACUUM delta_retention_test RETAIN 0 HOURS")

# Time travel to version 0
print("\n=== Attempting Time Travel to Version 0 ===")
try:
    df_old = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)
    df_old.show()
except Exception as e:
    print("Time travel failed as expected:\n", e)
