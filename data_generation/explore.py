import pandas as pd

df = pd.read_csv(
    "data/raw/en.openbeautyfacts.org.products.csv",
    sep="\t",
    low_memory=False,
    on_bad_lines="skip",
)

print(df.shape)
print(df.columns.tolist())
print(df.isna().mean().sort_values().head(15))
