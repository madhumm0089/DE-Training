# Databricks notebook source
import json
#step 1: Run ETL notebook

etl_output = dbutils.notebook.run("extract_and_transform", 300, {"process_date": "2021-01-01"})
metrics = json.loads(etl_output)

#step 2 apply buinsess rules
row_threshold = metrics["actual_rows"] / metrics["expected_rows"]
error_threshold = metrics["error_count"]
if row_threshold >= 0.95 and error_threshold < 100:
    print("Threshold met - triggering load notebook...")
    dbutils.notebook.run("loads", 300, {"process_date": "2021-01-01"})
else:
    print("Threshold not met - no action taken...")
    dbutils.notebook.run("alerts", 60, {"metrics": json.dumps(metrics)})