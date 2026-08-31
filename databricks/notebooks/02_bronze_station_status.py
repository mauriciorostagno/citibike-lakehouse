# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze · station_status (streaming)
# MAGIC
# MAGIC Auto Loader reads only the files it hasn't seen, tracked in the checkpoint.
# MAGIC `availableNow` processes what's pending and stops, which gives streaming semantics
# MAGIC without compute running 24/7.

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG    = "citibike"
LANDING    = f"/Volumes/{CATALOG}/bronze/landing"
CHECKPOINT = f"/Volumes/{CATALOG}/bronze/checkpoints"

# COMMAND ----------

raw = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", f"{CHECKPOINT}/station_status/_schema")
    # Without this Auto Loader infers every column as STRING and the explode below fails.
    .option("cloudFiles.inferColumnTypes", "true")
    # New upstream fields land in _rescued_data instead of breaking the run.
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    # Each landed file is one JSON object, not one object per line.
    .option("multiLine", "true")
    .load(f"{LANDING}/station_status"))

# COMMAND ----------

# One row per station per capture. Exploding here keeps the dbt models on flat data;
# the literal JSON is still in the volume.
exploded = (raw
    .select(
        F.col("captured_at").cast("timestamp").alias("captured_at"),
        F.explode("payload.data.stations").alias("station"),
        F.col("_rescued_data"),
        F.col("_metadata.file_path").alias("_source_file"),
    )
    .select(
        "captured_at",
        F.col("station.station_id").alias("station_id"),
        F.col("station.num_bikes_available").alias("num_bikes_available"),
        F.col("station.num_ebikes_available").alias("num_ebikes_available"),
        F.col("station.num_bikes_disabled").alias("num_bikes_disabled"),
        F.col("station.num_docks_available").alias("num_docks_available"),
        F.col("station.num_docks_disabled").alias("num_docks_disabled"),
        F.col("station.is_installed").alias("is_installed"),
        F.col("station.is_renting").alias("is_renting"),
        F.col("station.is_returning").alias("is_returning"),
        F.col("station.last_reported").alias("last_reported"),
        "_rescued_data",
        "_source_file",
        F.current_timestamp().alias("_ingested_at"),
    ))

# COMMAND ----------

query = (exploded.writeStream
    .option("checkpointLocation", f"{CHECKPOINT}/station_status")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(f"{CATALOG}.bronze.station_status_raw"))

query.awaitTermination()

# COMMAND ----------

# Spark Connect drops stream metrics once the query ends, so count the table instead.
display(spark.sql(f"""
    SELECT COUNT(*)                    AS rows,
           COUNT(DISTINCT captured_at) AS captures,
           COUNT(DISTINCT station_id)  AS stations,
           MAX(captured_at)            AS latest_capture
    FROM {CATALOG}.bronze.station_status_raw
"""))
