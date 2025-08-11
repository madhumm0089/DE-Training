from pyspark.sql.functions import split, trim, col
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("Case Study Example") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Assuming df is created as before
df = spark.createDataFrame(
    [("123 Main St, Springfield, IL, 62701", "John Doe | john@example.com | +91-99999-88888")],
    ["FullAddress", "ContactInfo "]  # Note the trailing space in column name
)

print("Before rename:", df.columns)  # ['FullAddress', 'ContactInfo ']

# Rename columns to remove trailing spaces
df = df.toDF(*[c.strip() for c in df.columns])

print("After rename:", df.columns)  # ['FullAddress', 'ContactInfo']

# Now you can safely split columns
df = df.withColumn("StreetAddress", trim(split(col("FullAddress"), ",")[0])) \
       .withColumn("City", trim(split(col("FullAddress"), ",")[1])) \
       .withColumn("State", trim(split(col("FullAddress"), ",")[2])) \
       .withColumn("ZipCode", trim(split(col("FullAddress"), ",")[3])) \
       .withColumn("Name", trim(split(col("ContactInfo"), "\\|")[0])) \
       .withColumn("Email", trim(split(col("ContactInfo"), "\\|")[1])) \
       .withColumn("Phone", trim(split(col("ContactInfo"), "\\|")[2]))

df.select("StreetAddress", "City", "State", "ZipCode", "Name", "Email", "Phone").show(truncate=False)
