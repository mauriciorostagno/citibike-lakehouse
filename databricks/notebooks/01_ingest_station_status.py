""" INGEST THE FAST FEED """

# station_status changes every ~10 seconds, so this runs every 30 minutes and feeds the
# incremental path. The catalog feeds live in 01_ingest_catalog and run daily -- pulling
# something that changes every few weeks on a 30-minute schedule lands 48 identical
# files a day for nothing.

# Cell 1:  %run ./_gbfs_common

land("station_status")
