"""
Convert all xlsx files in a folder to CSV, then combine into one CSV file.
Output is written to combined_output.csv inside the same folder.
An audit CSV (combined_audit.csv) records row counts per source file/sheet
and confirms the grand total matches what was written to disk.
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
audit_rows = []

for filepath in excel_files:
    filename = os.path.basename(filepath)
    try:
        sheets = pd.read_excel(filepath, sheet_name=None, engine="openpyxl")
    except Exception as e:
        print(f"  SKIPPED {filename}: {e}")
        audit_rows.append({
            "source_file": filename,
            "source_sheet": "—",
            "rows_read": 0,
            "status": f"SKIPPED: {e}",
        })
        continue
    for sheet_name, df in sheets.items():
        df["_source_file"] = filename
        df["_source_sheet"] = sheet_name
        csv_path = filepath.replace(".xlsx", f"_{sheet_name}.csv")
        df.to_csv(csv_path, index=False)
        frames.append(df)
        audit_rows.append({
            "source_file": filename,
            "source_sheet": sheet_name,
            "rows_read": len(df),
            "status": "OK",
        })
    print(f"  Converted {len(sheets)} sheet(s) from {filename}")

combined = pd.concat(frames, ignore_index=True)
output_path = os.path.join(directory, "combined_output.csv")
combined.to_csv(output_path, index=False)

# --- verify write ---
written = pd.read_csv(output_path)
expected = sum(r["rows_read"] for r in audit_rows)
match = len(written) == expected

audit_rows.append({
    "source_file": "— TOTAL —",
    "source_sheet": "—",
    "rows_read": expected,
    "status": f"OUTPUT ROWS: {len(written)} | {'MATCH' if match else 'MISMATCH — CHECK OUTPUT'}",
})

audit_path = os.path.join(directory, "combined_audit.csv")
pd.DataFrame(audit_rows).to_csv(audit_path, index=False)

print(f"\nWrote {len(written):,} rows to {output_path}")
print(f"Audit:  {audit_path}")
if not match:
    print(f"  WARNING: expected {expected:,} rows but output has {len(written):,}")
else:
    print(f"  Row count verified: {len(written):,} rows match source total")
