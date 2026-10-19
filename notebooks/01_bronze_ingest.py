# Databricks notebook source
for p in ["partner_a", "partner_b", "partner_c"]:
    for t in ["products", "inventory"]:
        files = dbutils.fs.ls(f"/Volumes/partner_feeds/bronze/landing/{p}/{t}")
        print(p, t, len(files))

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("CREATE VOLUME IF NOT EXISTS partner_feeds.bronze.checkpoints")

base = "/Volumes/partner_feeds/bronze"
source_path = f"{base}/landing/partner_b/products/"
checkpoint = f"{base}/checkpoints/partner_b_products"
target_table = "partner_feeds.bronze.partner_b_products"

raw = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.schemaLocation", checkpoint)
    .load(source_path)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

(
    raw.writeStream
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(target_table)
    .awaitTermination()
)

print("done")

# COMMAND ----------

display(spark.sql("""
    SELECT _source_file, COUNT(*) AS rows
    FROM partner_feeds.bronze.partner_b_products
    GROUP BY _source_file
    ORDER BY _source_file
"""))

# COMMAND ----------

from pyspark.sql import functions as F

BASE = "/Volumes/partner_feeds/bronze"


def ingest(name, folder, fmt, glob, extra=None):
    checkpoint = f"{BASE}/checkpoints/{name}"
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", fmt)
        .option("cloudFiles.inferColumnTypes", "false")
        .option("cloudFiles.schemaLocation", checkpoint)
        .option("pathGlobFilter", glob)
    )
    for key, value in (extra or {}).items():
        reader = reader.option(key, value)

    df = (
        reader.load(f"{BASE}/landing/{folder}/")
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    (
        df.writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(f"partner_feeds.bronze.{name}")
        .awaitTermination()
    )
    print("done:", name)


ingest("partner_a_products_xml", "partner_a/products", "xml", "*.xml", {"rowTag": "Product"})

# COMMAND ----------

display(spark.sql("""
    SELECT _source_file, COUNT(*) AS row_count
    FROM partner_feeds.bronze.partner_a_products_xml
    GROUP BY _source_file
    ORDER BY _source_file
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT SKU, ProductName, Price
    FROM partner_feeds.bronze.partner_a_products_xml
    WHERE SKU LIKE '0%'
    LIMIT 5
"""))

# COMMAND ----------

ingest("partner_a_inventory", "partner_a/inventory", "csv", "*.csv", {"header": "true"})
ingest("partner_b_inventory", "partner_b/inventory", "csv", "*.csv", {"header": "true"})
ingest("partner_c_products", "partner_c/products", "json", "*.jsonl")
ingest("partner_c_inventory", "partner_c/inventory", "json", "*.jsonl")

# COMMAND ----------

src = "/Volumes/partner_feeds/bronze/source_data"

(spark.read.option("header", "true").csv(f"{src}/delivery_log.csv")
    .write.mode("overwrite").saveAsTable("partner_feeds.bronze.delivery_log"))

(spark.read.parquet(f"{src}/master_products.parquet")
    .write.mode("overwrite").saveAsTable("partner_feeds.bronze.master_products"))

(spark.read.parquet(f"{src}/master_inventory.parquet")
    .write.mode("overwrite").saveAsTable("partner_feeds.bronze.master_inventory"))

print("reference tables loaded")

# COMMAND ----------

display(spark.sql("""
    SELECT 'partner_a_products_xml' AS tbl, COUNT(DISTINCT _source_file) AS files, COUNT(*) AS row_count FROM partner_feeds.bronze.partner_a_products_xml
    UNION ALL SELECT 'partner_a_inventory', COUNT(DISTINCT _source_file), COUNT(*) FROM partner_feeds.bronze.partner_a_inventory
    UNION ALL SELECT 'partner_b_products', COUNT(DISTINCT _source_file), COUNT(*) FROM partner_feeds.bronze.partner_b_products
    UNION ALL SELECT 'partner_b_inventory', COUNT(DISTINCT _source_file), COUNT(*) FROM partner_feeds.bronze.partner_b_inventory
    UNION ALL SELECT 'partner_c_products', COUNT(DISTINCT _source_file), COUNT(*) FROM partner_feeds.bronze.partner_c_products
    UNION ALL SELECT 'partner_c_inventory', COUNT(DISTINCT _source_file), COUNT(*) FROM partner_feeds.bronze.partner_c_inventory
"""))

# COMMAND ----------

display(spark.sql("SELECT status, COUNT(*) AS runs FROM partner_feeds.bronze.delivery_log GROUP BY status"))

# COMMAND ----------

# MAGIC %md
# MAGIC