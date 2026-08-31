# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest the slow feeds
# MAGIC
# MAGIC The batch side. These describe what a station *is* rather than what it currently
# MAGIC holds, and they change on the order of weeks.
# MAGIC
# MAGIC - `station_information` : name, coordinates, capacity, region_id -> SCD2 dimension
# MAGIC - `system_regions` : 7 rows, region_id -> name -> static dimension

# COMMAND ----------

# MAGIC %run ./_gbfs_common

# COMMAND ----------

for feed_name in ("station_information", "system_regions"):
    land(feed_name)
