"""Combine two CIK files (2024 + 2025) into one unique-by-CIK output.

2025 is the baseline: every 2025 row is kept intact. 2024 rows are appended
only if their CIK is not already present in the 2025 file.
"""

import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
save_directory = "/Users/avilae/claude-code/projects/edgar-projects/iss-factset-reconciliation"
baseline_2025_filename = "ciks_2025.csv"
add_2024_filename = "ciks_2024.csv"
output_filename = "ciks_combined_unique.csv"
cik_column = "CIK"
# -----------------------------------------------------------------------------

baseline_path = os.path.join(save_directory, baseline_2025_filename)
add_path = os.path.join(save_directory, add_2024_filename)
output_path = os.path.join(save_directory, output_filename)

df_2025 = pd.read_csv(baseline_path, dtype={cik_column: str})
df_2024 = pd.read_csv(add_path, dtype={cik_column: str})

baseline_ciks = set(df_2025[cik_column])
df_2024_new = df_2024[~df_2024[cik_column].isin(baseline_ciks)]

combined = pd.concat([df_2025, df_2024_new], ignore_index=True)

print(f"2025 baseline rows:           {len(df_2025):,}")
print(f"2024 input rows:              {len(df_2024):,}")
print(f"2024 rows added (new CIKs):   {len(df_2024_new):,}")
print(f"2024 rows skipped (in 2025):  {len(df_2024) - len(df_2024_new):,}")
print(f"Combined rows:                {len(combined):,}")
print(f"Combined unique CIKs:         {combined[cik_column].nunique():,}")

combined.to_csv(output_path, index=False)
print(f"Wrote {output_path}")
