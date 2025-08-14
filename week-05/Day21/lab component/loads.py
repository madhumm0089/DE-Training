# Databricks notebook source
dbutils.widgets.text("processed_date", "2025-04-20")
process_date = dbutils.widgets.get("processed_date")

print(f"Loading data for {process_date} into Delta Lake...")