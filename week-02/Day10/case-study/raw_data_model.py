from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import configure_spark_with_delta_pip

# Step 1: Create Spark Session with Delta support
builder = SparkSession.builder \
    .appName("raw_data_model") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

cus_path = "C:\\Users\\madhu.m\\Desktop\\DE-Training\\week-02\\Day10\\case-study\\data\\customers.csv"
product_path = "C:\\Users\\madhu.m\\Desktop\\DE-Training\\week-02\\Day10\\case-study\\data\\products.csv"
trans_path = "C:\\Users\\madhu.m\\Desktop\\DE-Training\\week-02\\Day10\\case-study\\data\\transactions.csv"

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

def create_link(delta_path, table_name, buskey):
    raw_df = spark.read.format("delta").load(delta_path)

    link_df = raw_df.select(*(col[c] for c in idList)))\
                    .withColumn()

# def create_link(delta_path, linkName, idList):
#     raw_df = spark.read.format("delta")

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, sha2, concat_ws, current_timestamp, lit

# # Initialize Spark Session
# spark = SparkSession.builder \
#         .appName("day10-case-study") \
#         .getOrCreate()

# # File paths
# custPath = "data/customers.csv"
# productsPath = "data/products.csv"
# transactionsPath = "data/transactions.csv"

# # Ingest raw CSVs into Delta format
# def ingest(path, tableName):
#     raw_df = spark.read.csv(path, header=True)
#     raw_df.write.format("delta").mode("overwrite").save(f"delta/raw/{tableName}")

# ingest(custPath, "customers")
# ingest(productsPath, "products")
# ingest(transactionsPath, "transactions")

# # Create Hub Table
# def createHub(deltaPath, tableName, busKey):
#     raw_df = spark.read.format("delta").load(deltaPath)

#     hub_df = raw_df.select(col(f"{busKey}")) \
#                     .dropDuplicates([busKey]) \
#                     .withColumn(f"{busKey}_HK", sha2(concat_ws("||", busKey), 256)) \
#                     .withColumn("load_time", current_timestamp()) \
#                     .withColumn("source", lit(tableName))
    
#     hub_df.write.format("delta").mode("overwrite").save(f"delta/vault/hub_{tableName}")


# createHub("delta/raw/customers", "customers", "customer_id")
# createHub("delta/raw/products", "products", "product_sku")

# # Create Link Table
# def createLink(deltaPath, linkName, idList):
#     raw_df = spark.read.format("delta").load(deltaPath)

#     link_df = raw_df.select(*[col(c) for c in idList]) \
#                     .withColumn(f"{linkName}_HK", sha2(concat_ws("||", *idList), 256)) \
#                     .withColumn("load_time", current_timestamp()) 
    
#     link_df.write.format("delta").mode("overwrite").save(f"delta/vault/{linkName}")

# # Create Link Table for Transactions
# idList = ["transaction_id", "customer_id", "product_sku"]
# createLink("delta/raw/transactions", "link-transactions", idList)

# # Create Satellite Table
# def createSatellite(deltaPath, tableName, attrList):
#     raw_df = spark.read.format("delta").load(deltaPath)

#     sat_df = raw_df.select(*[col(c) for c in attrList]) \
#                     .withColumn(f"sat_{tableName}_HK", sha2(concat_ws("||", attrList[0]), 256)) \
#                     .withColumn(f"hash_diff_{tableName}", sha2(concat_ws("||", *[col(c) for c in attrList]), 256)) \
#                     .withColumn("load_time", current_timestamp())
    
#     sat_df.write.format("delta").mode("overwrite").save(f"delta/vault/sat_{tableName}")

# createSatellite("delta/raw/customers", "customers", ["customer_id", "customer_name", "address", "contact"])
# createSatellite("delta/raw/products", "products", ["product_sku", "product_name", "category", "price"])
# createSatellite("delta/raw/transactions", "transactions", ["transaction_id", "customer_id", "product_sku", "purchase_date", "quantity", "sales_amount"])
