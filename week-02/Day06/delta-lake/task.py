from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, expr

spark = SparkSession.builder.appName("DeltaLogAnatomy") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .getOrCreate()

emp_df = spark.read.json("data/emp.json")
notes_df_raw = spark.read.text("data/madhu.txt")

notes_df = notes_df_raw.withColumn("id_str", split(col("value"), "\\|")[0]) \
                       .withColumn("id", expr("try_cast(id_str as int)")) \
                       .withColumn("content", split(col("value"), "\\|")[1]) \
                       .drop("value", "id_str") \
                       .filter(col("id").isNotNull())

final_df = emp_df.join(notes_df, on="id", how="inner")

final_df.select("id", "empname", "content").show(truncate=False)
