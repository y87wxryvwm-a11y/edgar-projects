"""Merge predicted_cik and edgar_landing_page back onto the original iss_only_v2
on company_name so the final file has only the original columns plus those two.
"""

import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
save_directory = "/Users/avilae/claude-code/projects/edgar-projects/iss-factset-reconciliation"
enriched_filename = "iss_only_v2_with_edgar_landing_page.csv"
original_filename = "iss_only_v2.csv"
output_filename = "iss_only_v2_final.csv"

company_col = "company_name"
cik_col = "predicted_cik"
url_col = "edgar_landing_page"
# -----------------------------------------------------------------------------

enriched_path = os.path.join(save_directory, enriched_filename)
original_path = os.path.join(save_directory, original_filename)
output_path = os.path.join(save_directory, output_filename)

enriched = pd.read_csv(enriched_path, dtype={cik_col: str})
original = pd.read_csv(original_path)

lookup = (
    enriched[[company_col, cik_col, url_col]]
    .drop_duplicates(subset=[company_col])
    .reset_index(drop=True)
)

merged = original.merge(lookup, on=company_col, how="left")

matched = merged[cik_col].notna().sum()
print(f"Original rows:             {len(original):,}")
print(f"Lookup rows (unique name): {len(lookup):,}")
print(f"Rows with CIK attached:    {matched:,}")
print(f"Rows with no match:        {len(merged) - matched:,}")

merged.to_csv(output_path, index=False)
print(f"Wrote {output_path}")
