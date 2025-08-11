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

cus_path = "data/customers.csv"
product_path = "data/products.csv"
trans_path = "data/transactions.csv"

def ingest(path, table_name):
    raw_df = spark.read.csv(path, header=True)
    raw_df.write.format("delta").mode("overwrite").save(f"delta/raw/{table_name}")

ingest(cus_path, "customers")
ingest(product_path, "products")
ingest(trans_path, "transactions")

def create_hub(delta_path, tableName, buskey):
    raw_df = spark.read.format("delta").load(delta_path)

    hub_df = raw_df.select(col(f"{buskey}"))\
                    .dropDuplicates([buskey])\
                    .withColumn(f"{buskey}_HK", sha2(concat_ws("||", buskey), 256))\
                    .withColumn("load_time", current_timestamp())\
                    .withColumn("record_source", lit(tableName))
    
    hub_df.write.format("delta").mode("overwrite").save(f"delta/vault/hub_{tableName}")

create_hub("delta/raw/customers", "customers", "customer_id")
create_hub("delta/raw/products", "products", "product_sku")

def create_link(delta_path, idList, link_name):
    raw_df = spark.read.format("delta").load(delta_path)
    link_df = raw_df.select(*[col(c) for c in idList])\
                        .withColumn(f"{link_name}_HK", sha2(concat_ws("||", *idList), 256)) \
                        .withColumn("load_time", current_timestamp())\
                        .withColumn("record_src", lit(link_name))
    link_df.write.format("delta").mode("overwrite").save("delta/vault/link_transaction")
    
idList = ["transaction_id", "customer_id", "product_sku"]
create_link("delta/raw/transactions", idList, "transaction_link")

spark.read.format("delta").load("delta/vault/link_transaction").show()

def create_satellite(delta_path, table_name, attr_list):
    raw_df = spark.read.format("delta").load(delta_path)

    satallite_df = raw_df.select(*[col(c) for c in attr_list])\
                        .withColumn(f"{table_name}_HK", sha2(concat_ws("||", attr_list[0]), 256))\
                        .withColumn("load_time", current_timestamp())
    
    satallite_df.write.format("delta").mode("overwrite").save(f"delta/vault/{table_name}")


create_satellite("delta/raw/customers", "sat_customer", ["customer_id", "customer_name", "address", "contact"])
create_satellite("delta/raw/products", "sat_product", ["product_sku", "product_name", "category", "price"])
create_satellite("delta/raw/transactions", "sat_transaction", ["transaction_id", "customer_id", "product_sku", "purchase_date", "quantity", "sales_amount"])

spark.read.format("delta").load("delta/vault/sat_customer").show()

