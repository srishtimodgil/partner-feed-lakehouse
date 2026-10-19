-- Databricks notebook source
-- MAGIC %python
-- MAGIC (spark.read.option("header", "true")
-- MAGIC     .csv("/Volumes/partner_feeds/bronze/source_data/ground_truth.csv")
-- MAGIC     .write.mode("overwrite").saveAsTable("partner_feeds.bronze.ground_truth"))
-- MAGIC print("ground truth loaded")

-- COMMAND ----------

SELECT defect, COUNT(*) AS planted
FROM partner_feeds.bronze.ground_truth
GROUP BY defect
ORDER BY planted DESC;

-- COMMAND ----------

CREATE OR REPLACE TABLE partner_feeds.silver.products_checked AS
WITH ranked AS (
  SELECT p.*,
         row_number() OVER (PARTITION BY p.run_id, p.sku ORDER BY p.source_file) AS rn
  FROM partner_feeds.silver.products p
),
joined AS (
  SELECT r.*, m.name AS master_name, m.price AS master_price
  FROM ranked r
  LEFT JOIN partner_feeds.bronze.master_products m ON r.sku = m.sku
)
SELECT
  run_id, partner, run_date, sku, name, brand, price, image_url, updated_at, source_file,
  CASE
    WHEN rn > 1 THEN 'duplicate_row'
    WHEN name IS NULL THEN 'missing_name'
    WHEN sku IS NULL OR NOT sku RLIKE '^([0-9]{8}|[0-9]{12,14})$' THEN 'bad_barcode'
    WHEN image_url IS NOT NULL AND NOT image_url RLIKE '^https?://' THEN 'bad_image_url'
    WHEN master_price IS NOT NULL AND abs(price - master_price) > 0.005 THEN 'price_mismatch'
    WHEN master_name IS NOT NULL AND name <> master_name THEN 'name_mismatch'
    ELSE NULL
  END AS failure_reason
FROM joined;

-- COMMAND ----------

CREATE OR REPLACE TABLE partner_feeds.silver.inventory_checked AS
WITH ranked AS (
  SELECT i.*,
         row_number() OVER (PARTITION BY i.run_id, i.sku ORDER BY i.source_file) AS rn
  FROM partner_feeds.silver.inventory i
),
joined AS (
  SELECT r.*, m.on_hand AS master_on_hand
  FROM ranked r
  LEFT JOIN partner_feeds.bronze.master_inventory m
    ON r.sku = m.sku AND r.run_date = to_date(m.run_date)
)
SELECT
  run_id, partner, run_date, sku, on_hand, location, as_of, source_file,
  CASE
    WHEN rn > 1 THEN 'duplicate_row'
    WHEN sku IS NULL OR NOT sku RLIKE '^([0-9]{8}|[0-9]{12,14})$' THEN 'bad_barcode'
    WHEN on_hand < 0 THEN 'negative_qty'
    WHEN master_on_hand IS NOT NULL AND on_hand <> master_on_hand THEN 'qty_mismatch'
    ELSE NULL
  END AS failure_reason
FROM joined;

-- COMMAND ----------

CREATE OR REPLACE TABLE partner_feeds.silver.quarantine AS
SELECT 'products' AS feed_type, run_id, partner, run_date, sku, failure_reason, source_file
FROM partner_feeds.silver.products_checked
WHERE failure_reason IS NOT NULL
UNION ALL
SELECT 'inventory', run_id, partner, run_date, sku, failure_reason, source_file
FROM partner_feeds.silver.inventory_checked
WHERE failure_reason IS NOT NULL;

-- COMMAND ----------

CREATE OR REPLACE TABLE partner_feeds.silver.products_valid AS
SELECT * EXCEPT (failure_reason)
FROM partner_feeds.silver.products_checked
WHERE failure_reason IS NULL;

-- COMMAND ----------

CREATE OR REPLACE TABLE partner_feeds.silver.inventory_valid AS
SELECT * EXCEPT (failure_reason)
FROM partner_feeds.silver.inventory_checked
WHERE failure_reason IS NULL;

-- COMMAND ----------

SELECT feed_type, failure_reason, COUNT(*) AS row_count
FROM partner_feeds.silver.quarantine
GROUP BY feed_type, failure_reason
ORDER BY feed_type, row_count DESC;

-- COMMAND ----------

WITH truth AS (
  SELECT defect, run_id, sku_as_sent AS sku, COUNT(*) AS n
  FROM partner_feeds.bronze.ground_truth
  WHERE sku_as_sent IS NOT NULL
  GROUP BY defect, run_id, sku_as_sent
),
found AS (
  SELECT failure_reason AS defect, run_id, sku, COUNT(*) AS n
  FROM partner_feeds.silver.quarantine
  GROUP BY failure_reason, run_id, sku
),
joined AS (
  SELECT
    coalesce(t.defect, f.defect) AS defect,
    coalesce(t.n, 0) AS planted,
    coalesce(f.n, 0) AS flagged,
    CASE WHEN t.n IS NOT NULL AND f.n IS NOT NULL THEN least(t.n, f.n) ELSE 0 END AS caught
  FROM truth t
  FULL OUTER JOIN found f
    ON t.defect = f.defect AND t.run_id = f.run_id AND t.sku = f.sku
)
SELECT
  defect,
  SUM(planted) AS planted,
  SUM(flagged) AS flagged,
  SUM(caught) AS caught,
  round(SUM(caught) / NULLIF(SUM(planted), 0), 3) AS recall_rate,
  round(SUM(caught) / NULLIF(SUM(flagged), 0), 3) AS precision_rate
FROM joined
GROUP BY defect
ORDER BY planted DESC;

-- COMMAND ----------

WITH truth AS (
  SELECT run_id, sku_as_sent AS sku, COUNT(*) AS planted
  FROM partner_feeds.bronze.ground_truth
  WHERE defect = 'bad_barcode'
  GROUP BY run_id, sku_as_sent
),
found AS (
  SELECT run_id, sku, COUNT(*) AS flagged
  FROM partner_feeds.silver.quarantine
  WHERE failure_reason = 'bad_barcode'
  GROUP BY run_id, sku
)
SELECT t.run_id, t.sku, t.planted, coalesce(f.flagged, 0) AS flagged
FROM truth t
LEFT JOIN found f ON t.run_id = f.run_id AND t.sku = f.sku
WHERE t.planted > coalesce(f.flagged, 0);

-- COMMAND ----------

WITH truth AS (
  SELECT run_id, sku_as_sent AS sku, COUNT(*) AS planted
  FROM partner_feeds.bronze.ground_truth
  WHERE defect = 'name_mismatch'
  GROUP BY run_id, sku_as_sent
),
found AS (
  SELECT run_id, sku, COUNT(*) AS flagged
  FROM partner_feeds.silver.quarantine
  WHERE failure_reason = 'name_mismatch'
  GROUP BY run_id, sku
)
SELECT t.run_id, t.sku, m.name AS master_name
FROM truth t
LEFT JOIN found f ON t.run_id = f.run_id AND t.sku = f.sku
JOIN partner_feeds.bronze.master_products m ON t.sku = m.sku
WHERE t.planted > coalesce(f.flagged, 0);