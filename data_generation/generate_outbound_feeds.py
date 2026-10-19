import csv
import json
import random
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

random.seed(42)

BASE_FILE = "data/raw/obf_clean.parquet"
MASTER_DIR = Path("data/master")
FEED_DIR = Path("data/outbound")
LOG_FILE = Path("data/delivery_log.csv")
TRUTH_FILE = Path("data/ground_truth.csv")

N_DAYS = 10
FIRST_DAY = datetime(2026, 10, 1, tzinfo=timezone.utc)
SCHEDULE_HOUR = 6          # feeds are scheduled for 06:00 UTC
SLA_HOUR = 8               # a run finishing after 08:00 UTC counts as late
MIGRATION_DAY = 6          # partner A products switch from XML to JSON on this day
UNIVERSE_SIZE = 6000
SKUS_PER_PARTNER = 2000

# Planned incidents: (partner, feed_type, day number) -> problem.
# Fixed on purpose, so every run of the generator tells the same story.
INCIDENTS = {
    ("partner_a", "inventory", 3): "failed",
    ("partner_b", "products", 5): "failed",
    ("partner_c", "inventory", 5): "failed",
    ("partner_c", "products", 8): "failed",
    ("partner_c", "products", 2): "late",
    ("partner_a", "products", 4): "late",
    ("partner_b", "inventory", 7): "late",
    ("partner_a", "inventory", 9): "late",
    ("partner_b", "products", 3): "truncated",
    ("partner_c", "inventory", 6): "truncated",
    ("partner_a", "products", 8): "truncated",
}

PARTNERS = ["partner_a", "partner_b", "partner_c"]
FEED_TYPES = ["products", "inventory"]

ERRORS = [
    "SFTP connection timed out",
    "SFTP authentication failed",
    "Source query returned no rows",
    "File write to SFTP server failed",
]

PRODUCT_DEFECTS = {
    "partner_a": [("missing_name", 0.010), ("bad_barcode", 0.007), ("bad_image_url", 0.010),
                  ("price_mismatch", 0.015), ("name_mismatch", 0.010)],
    "partner_b": [("missing_name", 0.010), ("bad_barcode", 0.007),
                  ("price_mismatch", 0.015), ("name_mismatch", 0.010)],
    "partner_c": [("missing_name", 0.010), ("bad_barcode", 0.007), ("bad_image_url", 0.010),
                  ("price_mismatch", 0.015), ("name_mismatch", 0.010)],
}
INVENTORY_DEFECTS = [("bad_barcode", 0.005), ("qty_mismatch", 0.020), ("negative_qty", 0.005)]

CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")


def clean(value):
    if value is None:
        return None
    return CONTROL_CHARS.sub("", str(value)).strip() or None


def iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def load_master():
    df = pd.read_parquet(BASE_FILE)
    df = df.astype(object).where(df.notna(), None)
    master = []
    for r in df.to_dict("records"):
        rec = {
            "sku": clean(r["code"]),
            "name": clean(r["product_name"]),
            "brand": clean(r["brands"]),
            "category": clean(r["categories"]),
            "size": clean(r["quantity"]),
            "country": clean(r["countries_en"]),
            "image_url": clean(r["image_url"]),
            "price": round(random.uniform(4, 90), 2),
        }
        if rec["sku"] and rec["name"]:
            master.append(rec)
    return master


def build_inventory(catalogs):
    skus = sorted({m["sku"] for cat in catalogs.values() for m in cat})
    inv = {}
    for sku in skus:
        base = random.randint(0, 400)
        for d in range(N_DAYS):
            inv[(sku, d)] = max(0, base + random.randint(-30, 30))
    return inv


def pick_defect(options):
    r = random.random()
    total = 0.0
    for name, p in options:
        total += p
        if r < total:
            return name
    return None


def apply_defect(rec, defect):
    if defect == "missing_name":
        rec["name"] = None
    elif defect == "bad_barcode":
        rec["sku"] = rec["sku"][:5] if random.random() < 0.5 else "ABC" + rec["sku"][3:]
    elif defect == "bad_image_url":
        rec["image_url"] = "not-a-url"
    elif defect == "price_mismatch":
        rec["price"] = round(rec["price"] * random.choice([0.7, 0.8, 1.2, 1.3]), 2)
    elif defect == "name_mismatch":
        if rec["name"]:
            rec["name"] = rec["name"].upper()[:20]
    elif defect == "qty_mismatch":
        rec["on_hand"] += random.randint(5, 40)
    elif defect == "negative_qty":
        rec["on_hand"] = -random.randint(1, 20)


def build_rows(partner, feed_type, catalog, day_idx, scheduled, inv):
    rows, flagged = [], []
    for m in catalog:
        if feed_type == "products":
            rec = dict(m)
            options = PRODUCT_DEFECTS[partner]
        else:
            rec = {"sku": m["sku"], "on_hand": inv[(m["sku"], day_idx)]}
            options = INVENTORY_DEFECTS
        rec["updated_at"] = scheduled - timedelta(minutes=random.randint(5, 600))
        defect = pick_defect(options)
        if defect:
            apply_defect(rec, defect)
            flagged.append((m["sku"], rec["sku"], defect))
        rows.append(rec)
    for rec in random.sample(rows, len(rows) // 200):
        rows.append(dict(rec))
        flagged.append((rec["sku"], rec["sku"], "duplicate_row"))
    return rows, flagged


def write_a_products_xml(rows, path):
    root = ET.Element("Catalog")
    fields = [("SKU", "sku"), ("ProductName", "name"), ("Brand", "brand"),
              ("Size", "size"), ("ImageURL", "image_url"), ("Price", "price")]
    for r in rows:
        product = ET.SubElement(root, "Product")
        for tag, key in fields:
            if r[key] is not None:
                ET.SubElement(product, tag).text = str(r[key])
        ET.SubElement(product, "LastUpdated").text = iso(r["updated_at"])
    ET.ElementTree(root).write(str(path), encoding="utf-8", xml_declaration=True)


def write_a_products_json(rows, path):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps({
                "sku": r["sku"],
                "name": r["name"],
                "brand": r["brand"],
                "size": r["size"],
                "image_url": r["image_url"],
                "price": {"amount": r["price"], "currency": "USD"},
                "updated_at": iso(r["updated_at"]),
            }, ensure_ascii=False) + "\n")


def write_a_inventory_csv(rows, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["sku", "on_hand", "location", "as_of"])
        for r in rows:
            w.writerow([r["sku"], r["on_hand"], "NYC-DC", iso(r["updated_at"])])


def write_b_products_csv(rows, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "title", "brand_name", "category", "market",
                    "list_price", "last_modified_epoch"])
        for r in rows:
            w.writerow([r["sku"], r["name"] or "", r["brand"] or "", r["category"] or "",
                        r["country"] or "", r["price"], int(r["updated_at"].timestamp())])


def write_b_inventory_csv(rows, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["product_id", "available_qty", "warehouse_code", "snapshot_ts"])
        for r in rows:
            w.writerow([r["sku"], r["on_hand"], "W-02", iso(r["updated_at"])])


def write_c_products_jsonl(rows, path):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps({
                "gtin": r["sku"],
                "item_name": r["name"],
                "manufacturer": r["brand"],
                "department": r["category"],
                "qty_size": r["size"],
                "picture": r["image_url"],
                "price_usd": r["price"],
                "last_update": r["updated_at"].strftime("%m/%d/%Y %H:%M"),
            }, ensure_ascii=False) + "\n")


def write_c_inventory_jsonl(rows, path):
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps({
                "gtin": r["sku"],
                "stock": r["on_hand"],
                "site": "STORE-NET",
                "as_of": iso(r["updated_at"]),
            }) + "\n")


WRITERS = {
    ("partner_a", "products", "xml"): write_a_products_xml,
    ("partner_a", "products", "json"): write_a_products_json,
    ("partner_a", "inventory", "csv"): write_a_inventory_csv,
    ("partner_b", "products", "csv"): write_b_products_csv,
    ("partner_b", "inventory", "csv"): write_b_inventory_csv,
    ("partner_c", "products", "jsonl"): write_c_products_jsonl,
    ("partner_c", "inventory", "jsonl"): write_c_inventory_jsonl,
}


def file_ext(partner, feed_type, day_idx):
    if feed_type == "products":
        if partner == "partner_a":
            return "xml" if day_idx + 1 < MIGRATION_DAY else "json"
        return "csv" if partner == "partner_b" else "jsonl"
    return "jsonl" if partner == "partner_c" else "csv"


def run_feed(partner, feed_type, catalog, day_idx, inv, log, truth):
    run_date = FIRST_DAY + timedelta(days=day_idx)
    scheduled = run_date.replace(hour=SCHEDULE_HOUR)
    sla_deadline = run_date.replace(hour=SLA_HOUR)
    run_id = f"{partner}-{feed_type}-{run_date:%Y%m%d}"

    incident = INCIDENTS.get((partner, feed_type, day_idx + 1))
    if incident == "late":
        started = scheduled + timedelta(hours=random.randint(2, 5), minutes=random.randint(0, 59))
    else:
        started = scheduled + timedelta(minutes=random.randint(0, 20))

    entry = {
        "run_id": run_id, "partner": partner, "feed_type": feed_type,
        "run_date": f"{run_date:%Y-%m-%d}",
        "scheduled_at": iso(scheduled), "started_at": iso(started), "finished_at": None,
        "status": None, "rows_expected": len(catalog), "rows_sent": 0,
        "file_name": None, "error_message": None,
    }

    def add_truth(defect, master_sku=None, sku_as_sent=None):
        truth.append({
            "run_id": run_id, "partner": partner, "feed_type": feed_type,
            "run_date": entry["run_date"], "master_sku": master_sku,
            "sku_as_sent": sku_as_sent, "defect": defect,
        })

    if incident == "failed":
        entry["status"] = "Failed"
        entry["finished_at"] = iso(started + timedelta(minutes=random.randint(1, 10)))
        entry["error_message"] = random.choice(ERRORS)
        log.append(entry)
        add_truth("failed_run")
        return

    sent_catalog = catalog
    if incident == "truncated":
        sent_catalog = catalog[: int(len(catalog) * random.uniform(0.6, 0.85))]
        add_truth("truncated_file")

    rows, flagged = build_rows(partner, feed_type, sent_catalog, day_idx, scheduled, inv)
    for master_sku, sku_as_sent, defect in flagged:
        add_truth(defect, master_sku, sku_as_sent)

    ext = file_ext(partner, feed_type, day_idx)
    path = FEED_DIR / partner / feed_type / f"{partner}_{feed_type}_{run_date:%Y%m%d}.{ext}"
    WRITERS[(partner, feed_type, ext)](rows, path)

    finished = started + timedelta(seconds=30 + len(rows) // 20 + random.randint(0, 60))
    entry["status"] = "Succeeded"
    entry["finished_at"] = iso(finished)
    entry["rows_sent"] = len(rows)
    entry["file_name"] = path.name
    if finished > sla_deadline:
        add_truth("late_run")
    log.append(entry)


def main():
    master = load_master()
    random.shuffle(master)
    universe = master[:UNIVERSE_SIZE]
    catalogs = {p: random.sample(universe, SKUS_PER_PARTNER) for p in PARTNERS}
    inv = build_inventory(catalogs)

    MASTER_DIR.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(master).to_parquet(MASTER_DIR / "master_products.parquet", index=False)
    inv_rows = [
        {"sku": sku, "run_date": f"{FIRST_DAY + timedelta(days=d):%Y-%m-%d}", "on_hand": qty}
        for (sku, d), qty in inv.items()
    ]
    pd.DataFrame(inv_rows).to_parquet(MASTER_DIR / "master_inventory.parquet", index=False)

    for partner in PARTNERS:
        for feed_type in FEED_TYPES:
            (FEED_DIR / partner / feed_type).mkdir(parents=True, exist_ok=True)

    log, truth = [], []
    for day_idx in range(N_DAYS):
        for partner in PARTNERS:
            for feed_type in FEED_TYPES:
                run_feed(partner, feed_type, catalogs[partner], day_idx, inv, log, truth)

    log_df = pd.DataFrame(log)
    truth_df = pd.DataFrame(truth)
    log_df.to_csv(LOG_FILE, index=False)
    truth_df.to_csv(TRUTH_FILE, index=False)

    print("Runs by status:")
    print(log_df["status"].value_counts())
    print()
    print("Files written:", int(log_df["file_name"].notna().sum()))
    print()
    print("Injected problems:")
    print(truth_df["defect"].value_counts())


if __name__ == "__main__":
    main()