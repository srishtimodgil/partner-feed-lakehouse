import pandas as pd

df = pd.read_csv(
    "data/raw/en.openbeautyfacts.org.products.csv",
    sep="\t",
    low_memory=False,
    on_bad_lines="skip",
)

candidates = [
    "code", "product_name", "brands", "categories", "main_category_en",
    "quantity", "countries_en", "image_url", "ingredients_text",
    "last_modified_t", "created_t",
]

print("Share of rows filled in:")
print((1 - df[candidates].isna().mean()).round(3))

print()
print("Duplicate codes:", df["code"].duplicated().sum())
print("Code length counts:")
print(df["code"].astype(str).str.len().value_counts().head(6))

print()
print(df[["code", "product_name", "brands", "categories", "quantity", "last_modified_t"]].head(5).to_string())
