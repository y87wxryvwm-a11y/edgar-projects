#!/usr/bin/env python3
"""
Appends all Excel worksheets found in a directory into a single CSV file.

Usage:
    python append_excel_to_csv.py /path/to/directory
"""

import sys
import glob
import os
import pandas as pd


def append_excel_files(directory: str) -> None:
    if not os.path.isdir(directory):
        print(f"Error: '{directory}' is not a valid directory.", file=sys.stderr)
        sys.exit(1)

    pattern_xlsx = os.path.join(directory, "*.xlsx")
    pattern_xls = os.path.join(directory, "*.xls")
    excel_files = sorted(glob.glob(pattern_xlsx) + glob.glob(pattern_xls))

    if not excel_files:
        print(f"No Excel files found in '{directory}'.", file=sys.stderr)
        sys.exit(1)

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


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(f"Usage: python {os.path.basename(__file__)} <directory>", file=sys.stderr)
        sys.exit(1)

    append_excel_files(sys.argv[1])
