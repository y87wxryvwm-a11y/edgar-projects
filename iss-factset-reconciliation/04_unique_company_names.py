"""Distill iss_only_v2.csv's company_name column to unique values."""

import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
save_directory = "/Users/avilae/claude-code/projects/edgar-projects/iss-factset-reconciliation"
input_filename = "iss_only_v2.csv"
output_filename = "iss_only_v2_unique_company_names.csv"
company_column = "company_name"
# -----------------------------------------------------------------------------

input_path = os.path.join(save_directory, input_filename)
output_path = os.path.join(save_directory, output_filename)

df = pd.read_csv(input_path)

unique_names = (
    df[company_column]
    .dropna()
    .astype(str)
    .str.strip()
    .loc[lambda s: s != ""]
    .drop_duplicates()
    .sort_values()
    .reset_index(drop=True)
)

print(f"Input rows:             {len(df):,}")
print(f"Unique company names:   {len(unique_names):,}")

unique_names.to_frame(name=company_column).to_csv(output_path, index=False)
print(f"Wrote {output_path}")
