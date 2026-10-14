-- Bronze: Incremental ingestion of data with Auto Loader

CREATE OR REFRESH STREAMING TABLE bronze_recall_events
COMMENT 'Raw events of FDA food recalls'

AS SELECT *, _metadata.file_path   AS source_file,
  current_timestamp()   AS ingestion_ts
FROM STREAM read_files(
    '/Volumes/workspace/food_safety/landing/',
  format => 'json'
);