import pandas as pd

df = pd.read_csv(
    "data/raw/en.openbeautyfacts.org.products.csv",
    sep="\t",
    dtype={"code": "string"},     # keep leading zeros
    low_memory=False,
    on_bad_lines="skip",
)

keep = [
    "code", "product_name", "brands", "categories", "quantity",
    "countries_en", "image_url", "last_modified_t",
]
slim = df[keep].copy()
print("start:", len(slim))

# Fields a partner feed row can't go live without
required = ["code", "product_name", "brands", "image_url"]
slim = slim.dropna(subset=required)
print("after required fields:", len(slim))

# Valid barcode lengths: GTIN-8, UPC-12, EAN-13, GTIN-14
valid_lengths = [8, 12, 13, 14]
slim = slim[slim["code"].str.len().isin(valid_lengths)]
print("after valid barcode length:", len(slim))

slim = slim.drop_duplicates(subset="code")
print("after dedupe:", len(slim))

slim.to_parquet("data/raw/obf_clean.parquet", index=False)
print(slim.head())