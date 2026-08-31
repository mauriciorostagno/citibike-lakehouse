""" INGEST THE SLOW FEEDS """

# The batch side. These describe what a station *is* rather than what it currently
# holds, and they change on the order of weeks.
#
#   station_information : name, coordinates, capacity, region_id  -> SCD2 dimension
#   system_regions      : 7 rows, region_id -> name               -> static dimension

# Cell 1:  %run ./_gbfs_common

for feed_name in ("station_information", "system_regions"):
    land(feed_name)
