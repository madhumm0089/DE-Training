from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip


builder = SparkSession.builder \
    .appName("raw_data_model") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def dim_customer(pit_path, star_path):
    df = spark.read.format("delta").load(pit_path)
    # df.printSchema()

    dim_customer_df = df.select(
        "customer_id",
        "customer_name",
        "address",
        "contact",
        "snapshot_date"
    ).dropDuplicates(["customer_id"])

    dim_customer_df.write.format("delta").mode("overwrite").save(star_path)

dim_customer("delta/vault/pit_customer", "delta/star/dim_customer")

spark.read.format("delta").load("delta/star/dim_customer").show()



def fact_sales(pit_trans_path, star_path):
    df = spark.read.format("delta").load(pit_trans_path)
    # df.printSchema()

    fact_df = df.select(
        "transaction_id",
        "customer_id",
        "product_sku",
        "product_name",
        "price"
    )
    fact_df.write.format("delta").mode("overwrite").save(star_path)
   
fact_sales("delta/vault/pit_transactions", "delta/star/fact_transactions")

spark.read.format("delta").load("delta/star/fact_transactions").show()