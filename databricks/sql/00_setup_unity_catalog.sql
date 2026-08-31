-- Unity Catalog setup. Run once, in the SQL editor.

CREATE CATALOG IF NOT EXISTS citibike;
USE CATALOG citibike;

CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw data as it arrives from the API';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Typed, cleaned and deduplicated';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Dimensional model for consumption';

-- Landing zone: raw JSON lands here before Spark reads it.
CREATE VOLUME IF NOT EXISTS bronze.landing
  COMMENT 'Raw GBFS JSON, partitioned by capture date';

-- Separate volume so Auto Loader never tries to ingest its own checkpoints.
CREATE VOLUME IF NOT EXISTS bronze.checkpoints
  COMMENT 'Structured Streaming checkpoints and inferred schemas';

SHOW SCHEMAS IN citibike;
SHOW VOLUMES IN citibike.bronze;
