from delta.tables import DeltaTable
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = SparkSession.builder \
    .appName("gdpr_masking") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

sat_customer_path = "delta/vault/sat_customer"

spark.read.format("delta").load("delta/vault/sat_customer")\
    .filter("customer_id = 'C002'").show(truncate=False)


sat_customer = DeltaTable.forPath(spark, sat_customer_path)

customer_to_mask = "C002"

sat_customer.update(
    condition=f"customer_id = '{customer_to_mask}'",
    set={
        "customer_name": lit(None),
        "address": lit(None),
        "contact": lit(None)
    }
)
spark.read.format("delta").load("delta/vault/sat_customer")\
    .filter("customer_id = 'C002'").show(truncate=False)
