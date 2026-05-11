"""
Append all Excel worksheets found in a folder into a single CSV file.
Output is written to combined_output.csv inside the same folder.
"""

import glob
import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
directory = "/path/to/your/folder"
# -----------------------------------------------------------------------------

pattern_xlsx = os.path.join(directory, "*.xlsx")
pattern_xls = os.path.join(directory, "*.xls")
excel_files = sorted(glob.glob(pattern_xlsx) + glob.glob(pattern_xls))

frames = []
for filepath in excel_files:
    xl = pd.ExcelFile(filepath)
    for sheet_name in xl.sheet_names:
        df = xl.parse(sheet_name)
        df["_source_file"] = os.path.basename(filepath)
        df["_source_sheet"] = sheet_name
        frames.append(df)
    print(f"  Read {len(xl.sheet_names)} sheet(s) from {os.path.basename(filepath)}")

combined = pd.concat(frames, ignore_index=True)
output_path = os.path.join(directory, "combined_output.csv")
combined.to_csv(output_path, index=False)
print(f"\nWrote {len(combined):,} rows to {output_path}")
