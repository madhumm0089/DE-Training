import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from delta import configure_spark_with_delta_pip

# Step 1: Configure Spark session with Delta support
builder = SparkSession.builder \
    .appName("Delta Optimization Task") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Step 2: Create DataFrame with 100 rows and region
df = spark.range(1, 101) \
    .withColumn("name", expr("concat('user_', id)")) \
    .withColumn("region", expr("CASE WHEN id % 4 = 0 THEN 'us' " +
                               "WHEN id % 4 = 1 THEN 'eu' " +
                               "WHEN id % 4 = 2 THEN 'in' " +
                               "ELSE 'apac' END"))

# Step 3: Save Delta table
delta_path = "output/delta/region_table"
df.write.format("delta").mode("overwrite").save(delta_path)

# Step 4: Load into DataFrame
df_delta = spark.read.format("delta").load(delta_path)
df_delta.createOrReplaceTempView("region_table")

# Step 5: Query before optimization
print("\n Query BEFORE optimization (region = 'us'):")
start_time = time.time()
spark.sql("SELECT * FROM region_table WHERE region = 'us'").show()
end_time = time.time()
print(f" Execution Time (before ZORDER): {round(end_time - start_time, 3)} seconds")

# Step 6: Simulate ZORDER by sorting data by region
print("\nSimulating OPTIMIZE ZORDER BY region...")
df_delta.orderBy("region").write.format("delta").mode("overwrite").save(delta_path)

# Step 7: Reload and register again
df_delta = spark.read.format("delta").load(delta_path)
df_delta.createOrReplaceTempView("region_table")

# Step 8: Query after simulated optimization
print("\n Query AFTER optimization (region = 'us'):")
start_time = time.time()
spark.sql("SELECT * FROM region_table WHERE region = 'us'").show()
end_time = time.time()
print(f" Execution Time (after ZORDER): {round(end_time - start_time, 3)} seconds")

print("\nTask completed.")
