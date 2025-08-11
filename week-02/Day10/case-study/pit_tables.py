from pyspark.sql.window import Window
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

def create_pit_customer(hub_path, sat_path,load_date):
    hub_df = spark.read.format("delta").load(hub_path)
    sat_df = spark.read.format("delta").load(sat_path)

    sat_df = sat_df.withColumnRenamed("load_time", "sat_load_time")\
                    .withColumnRenamed("customer_id", "sat_customer_id")

    
    joined_df = hub_df.alias("h") \
        .join(sat_df.alias("s"), sha2(col("h.customer_id"), 256) == col("s.sat_customer_HK")) \
        .filter(col("s.sat_load_time") <= load_date)
    
    # joined_df.show()
    window_spe = Window.partitionBy("h.customer_id").orderBy(col("s.sat_load_time").desc())
    
    pit_df = joined_df.withColumn("rank", row_number().over(window_spe))\
                       .filter(col("rank") == 1)\
                       .drop("rank") \
                       .select(
                            col("h.customer_id"),
                            col("h.customer_id_HK"),
                            col("h.load_time").alias("hub_load_time"),
                            col("h.record_source"),
                            col("s.sat_customer_id"),
                            col("s.customer_name"),
                            col("s.address"),
                            col("s.contact"),
                            col("s.sat_customer_HK"),
                            col("s.sat_load_time"),
                            lit(load_date).alias("snapshot_date")
                       )
    # pit_df.printSchema()
    pit_df.write.format("delta").mode("overwrite").save("delta/vault/pit_customer")

create_pit_customer("delta/vault/hub_customers", "delta/vault/sat_customer", current_timestamp())

spark.read.format("delta").load("delta/vault/pit_customer").show()

def create_pit_product(hub_path, sat_path, load_timestamp):
    hub_df = spark.read.format("delta").load(hub_path)
    sat_df = spark.read.format("delta").load(sat_path)

    sat_df = sat_df.withColumnRenamed("load_time", "sat_load_time")\
                    .withColumnRenamed("product_sku","sat_product_sku")
    # sat_df.printSchema()
    
    joined_df = hub_df.alias("h")\
                        .join(sat_df.alias("s"), sha2(col("h.product_sku"), 256) == col("s.sat_product_HK"))\
                        .filter(col("s.sat_load_time") <= load_timestamp)
    # joined_df.printSchema()

    window_spe = Window.partitionBy("h.product_sku").orderBy(col("s.sat_load_time").desc())

    pit_df = joined_df.withColumn("rank", row_number().over(window_spe))\
                        .filter(col("rank") == 1)\
                        .drop("rank")\
                        .select(
                            col("h.product_sku"),
                            col("h.product_sku_HK"),
                            col("h.load_time").alias("hub_load_time"),
                            col("h.record_source"),
                            col("s.sat_product_sku"),
                            col("s.product_name"),
                            col("s.category"),
                            col("s.price"),
                            col("s.sat_product_HK"),
                            col("s.sat_load_time"),
                            lit(load_timestamp).alias("snapshot_date")
                        )
    
    pit_df.write.format("delta").mode("overwrite").save("delta/vault/pit_products")


create_pit_product("delta/vault/hub_products", "delta/vault/sat_product", current_timestamp())

spark.read.format("delta").load("delta/vault/pit_products").show()

def create_pit_transaction(link_trans_path, pit_cus_path, pit_prod_path):
    link_df = spark.read.format("delta").load(link_trans_path)
    pit_customer_df = spark.read.format("delta").load(pit_cus_path)
    pit_product_df = spark.read.format("delta").load(pit_prod_path)

    # link_df.printSchema()
    # pit_customer_df.printSchema()
    # pit_product_df.printSchema()

    pit_df = link_df.alias("l")\
                        .join(pit_customer_df.alias("c"), on="customer_id")\
                        .join(pit_product_df.alias("p"), on="product_sku" )\
                        .withColumn("snaptshot_time", current_timestamp())\
                        .select(
                            col("l.transaction_id"),
                            col("l.transaction_link_HK"),
                            col("l.load_time").alias("link_load_time"),
                            col("l.record_src").alias("link_record_source"),
                            col("l.customer_id"),
                            col("l.product_sku"),

                            col("c.customer_id_HK"),
                            col("c.hub_load_time").alias("hub_customer_load_time"),
                            col("c.record_source").alias("hub_customer_record_source"),
                            col("c.customer_name"),
                            col("c.address"),
                            col("c.contact"),
                            col("c.sat_customer_HK"),
                            col("c.sat_load_time").alias("sat_customer_load_time"),

                            col("p.product_sku_HK"),
                            col("p.hub_load_time").alias("hub_product_load_time"),
                            col("p.record_source").alias("hub_product_record_source"),
                            col("p.product_name"),
                            col("p.category"),
                            col("p.price"),
                            col("p.sat_product_HK"),
                            col("p.sat_load_time").alias("sat_product_load_time"),

                            col("c.snapshot_date").alias("customer_snapshot_date"),
                            col("p.snapshot_date").alias("product_snapshot_date"),
                            
                        )
    # pit_df.printSchema()
    pit_df.write.format("delta").mode("overwrite").save("delta/vault/pit_trnasactions")

create_pit_transaction("delta/vault/link_transaction","delta/vault/pit_customer","delta/vault/pit_products" )

spark.read.format("delta").load("delta/vault/pit_trnasactions").show()
