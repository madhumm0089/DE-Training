# Databricks notebook source
import json

# Create widget for metrics input
dbutils.widgets.text("metrics", "{}")

# Safely parse the JSON input
try:
    metrics = json.loads(dbutils.widgets.get("metrics"))
except json.JSONDecodeError:
    metrics = {}

# Safely access values with defaults to avoid KeyError
alert_msg = f"""
Data Quality Alert for {metrics.get('date', 'N/A')}:

- Actual Rows: {metrics.get('actual_rows', 'N/A')}
- Expected Rows: {metrics.get('expected_rows', 'N/A')}
- Error Count: {metrics.get('error_count', 'N/A')}
"""

print(alert_msg)
