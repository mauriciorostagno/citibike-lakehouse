-- Spot checks on the bronze layer.

-- Shape: rows, captures, stations, time window.
SELECT COUNT(*)                    AS rows,
       COUNT(DISTINCT captured_at) AS captures,
       COUNT(DISTINCT station_id)  AS stations,
       MIN(captured_at)            AS first_capture,
       MAX(captured_at)            AS latest_capture
FROM citibike.bronze.station_status_raw;

-- Per capture, rows should equal stations. Anything higher means duplicates, usually a
-- checkpoint reset without truncating the target table.
SELECT captured_at,
       COUNT(*)                   AS rows,
       COUNT(DISTINCT station_id) AS stations
FROM citibike.bronze.station_status_raw
GROUP BY captured_at
ORDER BY captured_at;

-- Schema drift: any row here means the feed added a field.
SELECT COUNT(*) AS rescued_rows
FROM citibike.bronze.station_status_raw
WHERE _rescued_data IS NOT NULL;
