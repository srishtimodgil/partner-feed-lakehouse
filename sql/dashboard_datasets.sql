-- Datasets behind the "Partner Feed Health" dashboard

-- headline
SELECT
  COUNT(*) AS scheduled_runs,
  SUM(CASE WHEN portal_status = 'Succeeded' THEN 1 ELSE 0 END) AS portal_says_success,
  SUM(CASE WHEN run_outcome = 'Healthy' THEN 1 ELSE 0 END) AS actually_healthy
FROM partner_feeds.gold.run_health;

-- outcomes_by_partner
SELECT partner, run_outcome, COUNT(*) AS runs
FROM partner_feeds.gold.run_health
GROUP BY partner, run_outcome;

-- quality_trend
SELECT partner, run_date, quality_rate
FROM partner_feeds.gold.partner_daily;

-- defect_breakdown
SELECT failure_reason, feed_type, SUM(row_count) AS quarantined_rows
FROM partner_feeds.gold.quarantine_summary
GROUP BY failure_reason, feed_type;

-- incidents
SELECT run_date, partner, feed_type, portal_status, run_outcome,
       rows_expected, rows_received, finished_at, error_message
FROM partner_feeds.gold.run_health
WHERE run_outcome <> 'Healthy'
ORDER BY run_date, partner;
