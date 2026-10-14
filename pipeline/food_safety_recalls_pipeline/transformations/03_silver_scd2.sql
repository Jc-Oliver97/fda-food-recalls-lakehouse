CREATE OR REFRESH STREAMING TABLE silver_recalls_history
COMMENT 'SCD2: one row per recall status/classification version';
 
CREATE FLOW recalls_scd2_flow AS AUTO CDC INTO silver_recalls_history
FROM STREAM(silver_recall_events)
KEYS (recall_number)                    -- business key
SEQUENCE BY event_ts                    -- orders events
COLUMNS * EXCEPT (event_type, source_file, ingestion_ts)
STORED AS SCD TYPE 2                    -- keeps versions with __START_AT / __END_AT
TRACK HISTORY ON status, classification;  -- only this changes create a new version
