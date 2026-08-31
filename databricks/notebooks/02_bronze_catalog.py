""" BRONZE LAYER, BATCH BRANCH """

# The opposite of 02_bronze_station_status on purpose: no Auto Loader, no checkpoint, no
# incremental state. Every run reads the landed files and replaces the table.
#
# Full reload is the right call at 2,500 rows -- it costs nothing and removes the "did we
# miss a file?" question entirely. Intermediate states aren't kept here; history is the
# snapshot's job and the raw JSON stays in the volume either way.

from pyspark.sql import functions as F

CATALOG = "citibike"
LANDING = f"/Volumes/{CATALOG}/bronze/landing"


def read_latest(feed_name, record_key):
    """Read every landed file for a feed and keep only the most recent capture."""
    # No inferColumnTypes needed here: Spark's batch JSON reader infers nested structs
    # on its own. That option only exists to undo an Auto Loader default.
    raw = (spark.read
        .format("json")
        .option("multiLine", "true")
        .load(f"{LANDING}/{feed_name}"))

    latest_capture = raw.agg(F.max("captured_at")).collect()[0][0]

    return (raw
        .filter(F.col("captured_at") == latest_capture)
        .select(
            F.col("captured_at").cast("timestamp").alias("captured_at"),
            F.explode(f"payload.data.{record_key}").alias("record"),
        ))


stations = (read_latest("station_information", "stations")
    .select(
        "captured_at",
        F.col("record.station_id").alias("station_id"),
        F.col("record.name").alias("station_name"),
        F.col("record.short_name").alias("short_name"),
        F.col("record.lat").alias("latitude"),
        F.col("record.lon").alias("longitude"),
        F.col("record.capacity").alias("capacity"),
        F.col("record.region_id").alias("region_id"),
        F.col("record.station_type").alias("station_type"),
        F.col("record.has_kiosk").alias("has_kiosk"),
        F.current_timestamp().alias("_ingested_at"),
    ))

regions = (read_latest("system_regions", "regions")
    .select(
        "captured_at",
        F.col("record.region_id").alias("region_id"),
        F.col("record.name").alias("region_name"),
        F.current_timestamp().alias("_ingested_at"),
    ))

for df, table in ((stations, "station_information_raw"), (regions, "system_regions_raw")):
    row_count = df.count()
    (df.write
        .mode("overwrite")
        .option("overwriteSchema", "true")   # the catalog may gain columns upstream
        .saveAsTable(f"{CATALOG}.bronze.{table}"))
    print(f"OK  {table:26} {row_count:>5} rows")
