# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the fast feed
# MAGIC
# MAGIC station_status changes every ~10 seconds, so this runs every 30 minutes and feeds
# MAGIC the incremental path. The catalog feeds live in `01_ingest_catalog` and run daily:
# MAGIC pulling something that changes every few weeks on a 30-minute schedule lands 48
# MAGIC identical files a day for nothing.

# COMMAND ----------

# MAGIC %run ./_gbfs_common

# COMMAND ----------

land("station_status")
