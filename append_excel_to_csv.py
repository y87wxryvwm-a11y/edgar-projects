"""
Convert all xlsx files in a folder to CSV, then combine into one CSV file.
Output is written to combined_output.csv inside the same folder.
"""

import glob
import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
directory = r"C:\path\to\your\folder"
# -----------------------------------------------------------------------------

directory = directory.replace("\\", "/")

excel_files = sorted(glob.glob(os.path.join(directory, "*.xlsx")))

frames = []
for filepath in excel_files:
    filename = os.path.basename(filepath)
    try:
        sheets = pd.read_excel(filepath, sheet_name=None, engine="openpyxl")
    except Exception as e:
        print(f"  SKIPPED {filename}: {e}")
        continue
    for sheet_name, df in sheets.items():
        df["_source_file"] = filename
        df["_source_sheet"] = sheet_name
        csv_path = filepath.replace(".xlsx", f"_{sheet_name}.csv")
        df.to_csv(csv_path, index=False)
        frames.append(df)
    print(f"  Converted {len(sheets)} sheet(s) from {filename}")

combined = pd.concat(frames, ignore_index=True)
output_path = os.path.join(directory, "combined_output.csv")
combined.to_csv(output_path, index=False)
print(f"\nWrote {len(combined):,} rows to {output_path}")
