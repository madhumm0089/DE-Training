from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("delta_demo") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

delta_path = "delta/vault/sat_customer"

def optimize_zorder(path, table_name):
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{path}'")
    
    print(f"Optimizing table: {table_name}")
    spark.sql(f"OPTIMIZE {table_name}")

optimize_zorder("delta/vault/sat_customer", "sat_customer")


def time_travel(path, version=0):
    print(f"Reading version {version} of: {path}")
    df = spark.read.format("delta").option("versionAsOf", version).load(path)
    df.show()

time_travel(delta_path)

def restore_customer(path, customer_id, version):
    print(f"Restoring customer_id = {customer_id} from version {version}")
    old_df = spark.read.format("delta").option("versionAsOf", version).load(path)
    restored = old_df.filter(f"customer_id = '{customer_id}'")
    restored.write.format("delta").mode("append").save(path)

restore_customer(delta_path, "C002", 0)