# Databricks notebook source
import json
from datetime import datetime

# Properly format the date
dbutils.widgets.text("processed_date", datetime.today().strftime('%Y-%m-%d'))
processed_date = dbutils.widgets.get("processed_date")

# Simulated metrics
expected_rows = 100000
actual_rows = 97000
error_count = 50

# Prepare metrics dictionary
metrics = {
    "date": processed_date,
    "expected_rows": expected_rows,
    "actual_rows": actual_rows,
    "error_count": error_count,
    "completion_time": datetime.now().isoformat()
}

# Return metrics as JSON
dbutils.notebook.exit(json.dumps(metrics))
