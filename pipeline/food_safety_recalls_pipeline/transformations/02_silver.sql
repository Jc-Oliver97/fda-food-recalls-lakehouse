CREATE OR REFRESH STREAMING TABLE silver_recall_events (
  -- Rows get discarded
  CONSTRAINT valid_recall_number  EXPECT (recall_number IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_event_ts       EXPECT (event_ts IS NOT NULL)      ON VIOLATION DROP ROW,
  CONSTRAINT valid_classification EXPECT (classification IN ('Class I','Class II','Class III'))
                                  ON VIOLATION DROP ROW,
  CONSTRAINT valid_status         EXPECT (status IN ('Ongoing','Completed','Terminated'))
                                  ON VIOLATION DROP ROW,
  -- If feed fails, pipeline stops
  CONSTRAINT valid_event_type     EXPECT (event_type IN ('INITIAL','STATUS_CHANGE'))
                                  ON VIOLATION FAIL UPDATE,
  -- Row is conserved, gets measured
  CONSTRAINT initiation_before_report EXPECT (recall_initiation_date <= report_date),
  CONSTRAINT us_firm_has_state    EXPECT (country <> 'United States' OR coalesce(state, '') <> '')
)
COMMENT 'Valid recall events, type-checked and classified by hazard'
AS SELECT
  recall_number,
  event_id,
  event_type,
  CAST(event_ts AS TIMESTAMP) AS event_ts,
  status,
  classification,
  trim(recalling_firm) AS recalling_firm,
  city,
  state,
  country,
  product_description,
  reason_for_recall,
  product_quantity,
  distribution_pattern,
  voluntary_mandated,
  to_date(recall_initiation_date, 'yyyyMMdd') AS recall_initiation_date,
  to_date(report_date, 'yyyyMMdd') AS report_date,
  to_date(nullif(termination_date, ''), 'yyyyMMdd')  AS termination_date,
  -- Food science criteria into Business logic
  CASE
    WHEN lower(reason_for_recall) RLIKE 'listeria' THEN 'Listeria monocytogenes'
    WHEN lower(reason_for_recall) RLIKE 'salmonella' THEN 'Salmonella'
    WHEN lower(reason_for_recall) RLIKE 'e[.] ?coli|escherichia|o157|stec' THEN 'E. coli (STEC)'
    WHEN lower(reason_for_recall) RLIKE 'botulinum|clostridium' THEN 'Clostridium botulinum'
    WHEN lower(reason_for_recall) RLIKE 'undeclared|allergen' THEN 'Undeclared allergen'
    WHEN lower(reason_for_recall) RLIKE '(^|[^a-z])lead([^a-z]|$)|arsenic|cadmium|mercury'
                                                                          THEN 'Heavy metals'
    WHEN lower(reason_for_recall) RLIKE 'mold|aflatoxin|mycotoxin' THEN 'Mold / mycotoxins'
    WHEN lower(reason_for_recall) RLIKE 'foreign|metal|glass|plastic' THEN 'Foreign material'
    WHEN lower(reason_for_recall) RLIKE 'cgmp|insanitary|sanitation' THEN 'cGMP / sanitation'
    WHEN lower(reason_for_recall) RLIKE 'label|misbrand' THEN 'Labeling'
    ELSE 'Other'
  END AS hazard_category,
  source_file,
  ingestion_ts
FROM STREAM(bronze_recall_events);


-- Quarantine bad data
CREATE OR REFRESH STREAMING TABLE silver_recall_events_quarantine
COMMENT 'Rejected events by critical quality rules'
AS SELECT
  *,
  CASE
    WHEN recall_number IS NULL THEN 'missing_recall_number'
    WHEN event_ts IS NULL THEN 'missing_event_ts'
    WHEN coalesce(classification, '') NOT IN ('Class I','Class II','Class III')
                               THEN 'invalid_classification'
    ELSE 'invalid_status'
  END AS failed_rule
FROM STREAM(bronze_recall_events)
WHERE recall_number IS NULL
   OR event_ts IS NULL
   OR coalesce(classification, '') NOT IN ('Class I','Class II','Class III')
   OR coalesce(status, '') NOT IN ('Ongoing','Completed','Terminated');

