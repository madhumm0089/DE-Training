import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, expr
from delta import configure_spark_with_delta_pip

# Step 1: Configure Spark session with Delta support
builder = SparkSession.builder \
    .appName("Delta Example with Notes and CVs") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Step 2: Load employee data from JSON
emp_path = "files/cvs/emp.json"
emp_df = spark.read.json(emp_path)

# Step 3: Load note text files
madhu_df_raw = spark.read.text("files/cvs/madhum.txt")
mahesh_df_raw = spark.read.text("files/cvs/mahesh.txt")
notes_df_raw = madhu_df_raw.union(mahesh_df_raw)

# Step 4: Parse notes (extract id and content)
notes_df = notes_df_raw.withColumn("id_str", split(col("value"), "\\|")[0]) \
                       .withColumn("id", expr("try_cast(id_str as int)")) \
                       .withColumn("content", split(col("value"), "\\|")[1]) \
                       .drop("value", "id_str") \
                       .filter(col("id").isNotNull())

# Step 5: Join employee + notes
final_df = emp_df.join(notes_df, on="id", how="inner")

# Step 6: Save to Delta
delta_path = "output/path/delta_table"
final_df.write.format("delta").mode("overwrite").save(delta_path)

print("[OK] Joined data (emp + notes):")
spark.read.format("delta").load(delta_path).show(truncate=False)

# Step 7: Load optional CV files
cv_folder = "files/cvs"
cv_rows = []

for row in emp_df.collect():
    empname = row["empname"]
    emp_id = row["id"]
    file_path = os.path.join(cv_folder, f"{empname}.txt")
    if os.path.exists(file_path):
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            cv_rows.append((emp_id, content))
            print(f"[INFO] Loaded CV for: {empname} (id={emp_id})")

# Step 8: Merge CVs and update Delta
if cv_rows:
    cv_df = spark.createDataFrame(cv_rows, ["id", "cv_data"])
    print("[INFO] CV Data:")
    cv_df.show(truncate=False)

    df_delta = spark.read.format("delta").load(delta_path)

    df_joined = df_delta.join(cv_df, on="id", how="left") \
                        .select(df_delta["*"], cv_df["cv_data"])

    print("[OK] Final Delta with CVs:")
    df_joined.show(truncate=False)

    # Save back to Delta
    df_joined.write.format("delta") \
             .option("mergeSchema", "true") \
             .mode("overwrite") \
             .save(delta_path)

    print(f"[DONE] Delta table updated at: {delta_path}")

    # Step 9: Save to CSV
    df_joined.select("id", "empname", "content", "cv_data") \
             .coalesce(1) \
             .write \
             .mode("overwrite") \
             .option("header", "true") \
             .csv("output/final_data_csv")

    # Step 10: Save to TXT
    df_joined.selectExpr("concat(id, '|', empname, '|', content, '|', cv_data) as line") \
             .coalesce(1) \
             .write \
             .mode("overwrite") \
             .text("output/final_data_txt")

    print("[DONE] Saved final data to CSV and TXT.")
else:
    print("[WARN] No CV files found.")
