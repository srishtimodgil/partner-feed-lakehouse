-- Databricks notebook source
SET TIME ZONE 'UTC';

-- COMMAND ----------

CREATE OR REPLACE TABLE partner_feeds.gold.run_health AS
WITH log AS (
  SELECT
    run_id, partner, feed_type,
    to_date(run_date) AS run_date,
    try_to_timestamp(scheduled_at, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS scheduled_at,
    try_to_timestamp(started_at, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS started_at,
    try_to_timestamp(finished_at, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS finished_at,
    status,
    try_cast(rows_expected AS INT) AS rows_expected,
    nullif(error_message, '') AS error_message
  FROM partner_feeds.bronze.delivery_log
),
received AS (
  SELECT run_id, COUNT(*) AS rows_received FROM partner_feeds.silver.products GROUP BY run_id
  UNION ALL
  SELECT run_id, COUNT(*) FROM partner_feeds.silver.inventory GROUP BY run_id
),
bad AS (
  SELECT run_id, COUNT(*) AS rows_quarantined FROM partner_feeds.silver.quarantine GROUP BY run_id
),
combined AS (
  SELECT l.*,
         coalesce(r.rows_received, 0) AS rows_received,
         coalesce(b.rows_quarantined, 0) AS rows_quarantined
  FROM log l
  LEFT JOIN received r ON l.run_id = r.run_id
  LEFT JOIN bad b ON l.run_id = b.run_id
),
flags AS (
  SELECT
    run_id, partner, feed_type, run_date, scheduled_at, started_at, finished_at,
    status AS portal_status, error_message,
    rows_expected, rows_received, rows_quarantined,
    status = 'Failed' AS is_failed,
    status = 'Succeeded' AND finished_at > timestamp(run_date) + INTERVAL 8 HOURS AS is_late,
    status = 'Succeeded' AND rows_received < rows_expected * 0.95 AS is_truncated,
    CASE WHEN rows_received > 0
         THEN round(1 - rows_quarantined / rows_received, 4) END AS quality_rate
  FROM combined
)
SELECT *,
  CASE WHEN is_failed THEN 'Failed'
       WHEN is_truncated THEN 'Truncated'
       WHEN is_late THEN 'Late'
       ELSE 'Healthy' END AS run_outcome
FROM flags;

-- COMMAND ----------

SELECT run_outcome, COUNT(*) AS runs
FROM partner_feeds.gold.run_health
GROUP BY run_outcome
ORDER BY runs DESC;

-- COMMAND ----------

SELECT
  COUNT(*) AS scheduled_runs,
  SUM(CASE WHEN portal_status = 'Succeeded' THEN 1 ELSE 0 END) AS portal_says_success,
  SUM(CASE WHEN run_outcome = 'Healthy' THEN 1 ELSE 0 END) AS actually_healthy
FROM partner_feeds.gold.run_health;

-- COMMAND ----------

CREATE OR REPLACE TABLE partner_feeds.gold.partner_daily AS
SELECT
  partner,
  run_date,
  COUNT(*) AS scheduled_runs,
  SUM(CASE WHEN portal_status = 'Succeeded' THEN 1 ELSE 0 END) AS portal_success,
  SUM(CASE WHEN run_outcome = 'Healthy' THEN 1 ELSE 0 END) AS healthy_runs,
  SUM(rows_received) AS rows_received,
  SUM(rows_quarantined) AS rows_quarantined,
  round(1 - SUM(rows_quarantined) / NULLIF(SUM(rows_received), 0), 4) AS quality_rate
FROM partner_feeds.gold.run_health
GROUP BY partner, run_date;


-- COMMAND ----------

CREATE OR REPLACE TABLE partner_feeds.gold.quarantine_summary AS
SELECT partner, feed_type, failure_reason, run_date, COUNT(*) AS row_count
FROM partner_feeds.silver.quarantine
GROUP BY partner, feed_type, failure_reason, run_date;

-- COMMAND ----------

SELECT 'run_health' AS tbl, COUNT(*) AS rows FROM partner_feeds.gold.run_health
UNION ALL SELECT 'partner_daily', COUNT(*) FROM partner_feeds.gold.partner_daily
UNION ALL SELECT 'quarantine_summary', COUNT(*) FROM partner_feeds.gold.quarantine_summary;