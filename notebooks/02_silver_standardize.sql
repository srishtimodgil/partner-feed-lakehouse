-- Databricks notebook source
SELECT 'a_xml' AS tbl, COUNT(DISTINCT _source_file) AS files, COUNT(*) AS row_count FROM partner_feeds.bronze.partner_a_products_xml
UNION ALL SELECT 'a_json', COUNT(DISTINCT _source_file), COUNT(*) FROM partner_feeds.bronze.partner_a_products_json
UNION ALL SELECT 'a_inventory', COUNT(DISTINCT _source_file), COUNT(*) FROM partner_feeds.bronze.partner_a_inventory
UNION ALL SELECT 'b_products', COUNT(DISTINCT _source_file), COUNT(*) FROM partner_feeds.bronze.partner_b_products
UNION ALL SELECT 'b_inventory', COUNT(DISTINCT _source_file), COUNT(*) FROM partner_feeds.bronze.partner_b_inventory
UNION ALL SELECT 'c_products', COUNT(DISTINCT _source_file), COUNT(*) FROM partner_feeds.bronze.partner_c_products
UNION ALL SELECT 'c_inventory', COUNT(DISTINCT _source_file), COUNT(*) FROM partner_feeds.bronze.partner_c_inventory;

-- COMMAND ----------

SET TIME ZONE 'UTC';

-- COMMAND ----------

CREATE OR REPLACE TABLE partner_feeds.silver.products AS
WITH unioned AS (

  SELECT 'partner_a' AS partner, 'xml' AS source_format,
         SKU AS sku, ProductName AS name, Brand AS brand,
         CAST(NULL AS STRING) AS category, Size AS size,
         CAST(NULL AS STRING) AS country, ImageURL AS image_url,
         try_cast(Price AS DOUBLE) AS price,
         try_to_timestamp(LastUpdated, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS updated_at,
         _source_file, _ingested_at
  FROM partner_feeds.bronze.partner_a_products_xml

  UNION ALL

  SELECT 'partner_a', 'json',
         sku, name, brand,
         CAST(NULL AS STRING), size,
         CAST(NULL AS STRING), image_url,
         try_cast(get_json_object(price, '$.amount') AS DOUBLE),
         try_to_timestamp(updated_at, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
         _source_file, _ingested_at
  FROM partner_feeds.bronze.partner_a_products_json

  UNION ALL

  SELECT 'partner_b', 'csv',
         product_id, title, brand_name,
         category, CAST(NULL AS STRING),
         market, CAST(NULL AS STRING),
         try_cast(list_price AS DOUBLE),
         timestamp_seconds(try_cast(last_modified_epoch AS BIGINT)),
         _source_file, _ingested_at
  FROM partner_feeds.bronze.partner_b_products

  UNION ALL

  SELECT 'partner_c', 'jsonl',
         gtin, item_name, manufacturer,
         department, qty_size,
         CAST(NULL AS STRING), picture,
         try_cast(price_usd AS DOUBLE),
         try_to_timestamp(last_update, 'MM/dd/yyyy HH:mm'),
         _source_file, _ingested_at
  FROM partner_feeds.bronze.partner_c_products
)
SELECT
  concat(partner, '-products-', regexp_extract(_source_file, '([0-9]{8})', 1)) AS run_id,
  partner,
  source_format,
  to_date(regexp_extract(_source_file, '([0-9]{8})', 1), 'yyyyMMdd') AS run_date,
  nullif(trim(sku), '') AS sku,
  nullif(trim(name), '') AS name,
  nullif(trim(brand), '') AS brand,
  nullif(trim(category), '') AS category,
  nullif(trim(size), '') AS size,
  nullif(trim(country), '') AS country,
  nullif(trim(image_url), '') AS image_url,
  price,
  updated_at,
  _source_file AS source_file,
  _ingested_at AS ingested_at
FROM unioned;

-- COMMAND ----------

SELECT
  source_format,
  COUNT(*) AS total,
  SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS null_prices
FROM partner_feeds.silver.products
WHERE partner = 'partner_a'
GROUP BY source_format;

-- COMMAND ----------

CREATE OR REPLACE TABLE partner_feeds.silver.inventory AS
WITH unioned AS (

  SELECT 'partner_a' AS partner, sku, on_hand AS on_hand_raw, location, as_of,
         _source_file, _ingested_at
  FROM partner_feeds.bronze.partner_a_inventory

  UNION ALL

  SELECT 'partner_b', product_id, available_qty, warehouse_code, snapshot_ts,
         _source_file, _ingested_at
  FROM partner_feeds.bronze.partner_b_inventory

  UNION ALL

  SELECT 'partner_c', gtin, stock, site, as_of,
         _source_file, _ingested_at
  FROM partner_feeds.bronze.partner_c_inventory
)
SELECT
  concat(partner, '-inventory-', regexp_extract(_source_file, '([0-9]{8})', 1)) AS run_id,
  partner,
  to_date(regexp_extract(_source_file, '([0-9]{8})', 1), 'yyyyMMdd') AS run_date,
  nullif(trim(sku), '') AS sku,
  try_cast(on_hand_raw AS INT) AS on_hand,
  location,
  try_to_timestamp(as_of, "yyyy-MM-dd'T'HH:mm:ss'Z'") AS as_of,
  _source_file AS source_file,
  _ingested_at AS ingested_at
FROM unioned;

-- COMMAND ----------

SELECT
  (SELECT COUNT(*) FROM partner_feeds.silver.products) AS silver_products,
  (SELECT COUNT(*) FROM partner_feeds.bronze.partner_a_products_xml)
  + (SELECT COUNT(*) FROM partner_feeds.bronze.partner_a_products_json)
  + (SELECT COUNT(*) FROM partner_feeds.bronze.partner_b_products)
  + (SELECT COUNT(*) FROM partner_feeds.bronze.partner_c_products) AS bronze_products,
  (SELECT COUNT(*) FROM partner_feeds.silver.inventory) AS silver_inventory,
  (SELECT COUNT(*) FROM partner_feeds.bronze.partner_a_inventory)
  + (SELECT COUNT(*) FROM partner_feeds.bronze.partner_b_inventory)
  + (SELECT COUNT(*) FROM partner_feeds.bronze.partner_c_inventory) AS bronze_inventory;

-- COMMAND ----------

SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN updated_at IS NULL THEN 1 ELSE 0 END) AS bad_timestamps,
  SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END) AS bad_prices,
  SUM(CASE WHEN name IS NULL THEN 1 ELSE 0 END) AS missing_names
FROM partner_feeds.silver.products;

-- COMMAND ----------

SELECT
  COUNT(*) AS total,
  SUM(CASE WHEN as_of IS NULL THEN 1 ELSE 0 END) AS bad_timestamps,
  SUM(CASE WHEN on_hand IS NULL THEN 1 ELSE 0 END) AS bad_quantities,
  SUM(CASE WHEN on_hand < 0 THEN 1 ELSE 0 END) AS negative_quantities
FROM partner_feeds.silver.inventory;

-- COMMAND ----------

SELECT partner, source_format, COUNT(*) AS row_count, COUNT(DISTINCT run_id) AS runs
FROM partner_feeds.silver.products
GROUP BY partner, source_format
ORDER BY partner, source_format;