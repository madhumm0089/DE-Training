from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def optimize_zorder(spark: SparkSession, delta_path: str):
    print(f"Optimizing: {delta_path}")
    spark.sql(f"OPTIMIZE delta.`{delta_path}`")

def vaccum(path, retension_hrs = 0):
    print(f"vaccum path {path} with retension hours {retension_hrs}")
    spark.sql(f"vaccum delta.`{path}`, Reatain {retension_hrs} Hours")

def gdpr():
    pass

def restore_table(path, version):
    print(f"Restoring table {path} to version {version}")
    df = spark.read.format("delta").option("versionAsOf", version).load(path)
    df.write.format("delta").mode("overwrite").save(path)
    print(f"Restored complete!..{path}")

