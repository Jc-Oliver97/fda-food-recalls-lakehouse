-- Status of each recall, existing version of SCD2
CREATE OR REFRESH MATERIALIZED VIEW gold_recalls_current
COMMENT 'Una fila por retiro con su estado vigente'
AS SELECT
  * EXCEPT (__START_AT, __END_AT),
  __START_AT AS status_since
FROM silver_recalls_history
WHERE __END_AT IS NULL;

-- Recalls by year, hazard, classification
CREATE OR REFRESH MATERIALIZED VIEW gold_recalls_by_year_hazard
AS SELECT
  year(report_date) AS report_year,
  hazard_category,
  classification,
  COUNT(*) AS recalls,
  COUNT_IF(status = 'Ongoing') AS open_recalls
FROM gold_recalls_current
GROUP BY ALL;

-- Days a recall stays open
CREATE OR REFRESH MATERIALIZED VIEW gold_recall_duration
AS SELECT
  hazard_category,
  classification,
  COUNT(*) AS closed_recalls,
  ROUND(AVG(datediff(__END_AT, __START_AT)), 1) AS avg_days_open,
  percentile_approx(datediff(__END_AT, __START_AT), 0.5) AS median_days_open
FROM silver_recalls_history
WHERE status = 'Ongoing' AND __END_AT IS NOT NULL
GROUP BY ALL;

-- Firms with the most recalls & Class I recalls
CREATE OR REFRESH MATERIALIZED VIEW gold_top_firms
AS SELECT
  recalling_firm,
  COUNT(*) AS total_recalls,
  COUNT_IF(classification = 'Class I') AS class_i_recalls,
  COUNT(DISTINCT hazard_category) AS distinct_hazards
FROM gold_recalls_current
GROUP BY recalling_firm
HAVING COUNT(*) >= 3;



