"""Run the Stage-3 diagnostic on an existing (ISS_full, iss_only) pair.

Use this when you already have the anti-join output and don't need to re-run
the FactSet comparison. Edit the two paths at the top, then run.

Both files can be .xlsx or .csv. The script auto-detects by extension.
Matching is done on (cusip_6, Meeting_Date) after the usual normalization
(zero-pad cusip_6 to 6, strip any trailing .0, parse Meeting_Date to datetime).
"""

import os

import pandas as pd

# ---- EDIT THESE --------------------------------------------------------------
# Directory where the input files live and where outputs get written.
# Use an absolute path so the script can be run from anywhere.
save_directory = "/Users/avilae/claude-code/projects/edgar-projects/iss-factset-reconciliation"

# Filenames (resolved relative to save_directory).
ISS_FULL_FILENAME = "ISS_proposals_2016to2025.xlsx"
ISS_ONLY_FILENAME = "iss_only.csv"
OUT_XLSX_FILENAME = "iss_only_diagnostic.xlsx"
# -----------------------------------------------------------------------------

ISS_FULL_PATH = os.path.join(save_directory, ISS_FULL_FILENAME)
ISS_ONLY_PATH = os.path.join(save_directory, ISS_ONLY_FILENAME)
OUT_XLSX = os.path.join(save_directory, OUT_XLSX_FILENAME)

ISS_NUMERIC = [
    "requirement",
    "levelofsupport",
    "support_for_against",
    "support_for_against_abstain",
    "support_outstanding",
]

ISS_STRING_COLS = [
    "company_name", "Meeting_Year", "cusip_6", "cusip", "iss_companyid",
    "indexname", "ticker", "meeting_code", "resolution", "resolution_type",
    "other_status", "foot_note", "omit_reason", "sponsor_name", "sponsor_type",
    "passed", "meetingid", "issagendaitemid", "itemonagendaid", "base",
    "foot_note_text", "omit_reason_text",
]

DIAGNOSTIC_COLS = [
    "indexname", "meeting_code", "sponsor_type", "resolution_type",
    "other_status", "omit_reason", "Meeting_Year",
]


def normalize_cusip6(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    s = s.str.replace(r"\.0+$", "", regex=True)
    s = s.str.zfill(6)
    return s


def read_iss(path: str) -> pd.DataFrame:
    """Read an ISS-shaped file (.xlsx or .csv) with correct dtypes."""
    ext = os.path.splitext(path)[1].lower()
    dtype = {c: str for c in ISS_STRING_COLS}
    if ext in (".xlsx", ".xls"):
        df = pd.read_excel(path, dtype=dtype, engine="openpyxl")
    elif ext == ".csv":
        df = pd.read_csv(path, dtype=dtype)
    else:
        raise ValueError(f"Unsupported file extension: {ext} (expected .xlsx or .csv)")

    # Coerce the five numeric columns; leave everything else as string.
    for col in ISS_NUMERIC:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["cusip_6"] = normalize_cusip6(df["cusip_6"])
    df["Meeting_Date"] = pd.to_datetime(df["Meeting_Date"], errors="coerce")

    for col in ISS_STRING_COLS:
        if col in df.columns:
            df[col] = df[col].fillna("").replace({"nan": "", "NaT": ""})
    return df


def diagnostic_table(iss_df: pd.DataFrame, iss_only_keys: set, col: str) -> pd.DataFrame:
    flags = pd.Series(
        ["iss_only" if k in iss_only_keys else "matched"
         for k in zip(iss_df["cusip_6"], iss_df["Meeting_Date"])],
        index=iss_df.index,
    )
    values = iss_df[col].fillna("").replace({"": "(blank)"})
    ct = pd.crosstab(values, flags)
    for group in ("matched", "iss_only"):
        if group not in ct.columns:
            ct[group] = 0
    ct = ct[["matched", "iss_only"]]
    totals = ct.sum(axis=0)
    pct = ct.divide(totals.replace(0, pd.NA), axis=1) * 100
    out = pd.DataFrame({
        "matched_count": ct["matched"],
        "matched_pct": pct["matched"].round(2),
        "iss_only_count": ct["iss_only"],
        "iss_only_pct": pct["iss_only"].round(2),
    })
    out["pct_delta"] = (out["iss_only_pct"] - out["matched_pct"]).round(2)
    out = out.sort_values("pct_delta", ascending=False)
    out.index.name = col
    return out


def main():
    print(f"Loading ISS full: {ISS_FULL_PATH}")
    iss = read_iss(ISS_FULL_PATH)
    print(f"Loading iss_only: {ISS_ONLY_PATH}")
    iss_only = read_iss(ISS_ONLY_PATH)

    iss_only_keys = set(zip(iss_only["cusip_6"], iss_only["Meeting_Date"]))

    iss_meetings = iss[["cusip_6", "Meeting_Date"]].drop_duplicates().shape[0]
    iss_only_meetings = iss_only[["cusip_6", "Meeting_Date"]].drop_duplicates().shape[0]

    # Sanity check: every iss_only key should be present in the full ISS file.
    iss_keys = set(zip(iss["cusip_6"], iss["Meeting_Date"]))
    stray = iss_only_keys - iss_keys
    if stray:
        print(f"WARNING: {len(stray):,} keys in iss_only are not in the full ISS file. "
              "Check that the two files come from the same universe and that normalization "
              "(cusip_6 padding, Meeting_Date parsing) produced identical keys.")

    print()
    print("=" * 60)
    print("ROW COUNTS")
    print("=" * 60)
    print(f"ISS full rows (proposal-grain):     {len(iss):>8,}")
    print(f"ISS full unique meetings:           {iss_meetings:>8,}")
    print(f"iss_only rows (proposal-grain):     {len(iss_only):>8,}")
    print(f"iss_only unique meetings:           {iss_only_meetings:>8,}")
    print()

    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
        for col in DIAGNOSTIC_COLS:
            if col not in iss.columns:
                print(f"(skipping {col!r}: not present in ISS file)")
                continue
            table = diagnostic_table(iss, iss_only_keys, col)
            sheet_name = col[:31] or "blank"
            table.to_excel(writer, sheet_name=sheet_name)
            print("=" * 60)
            print(f"DIAGNOSTIC: {col}")
            print("=" * 60)
            print(table.to_string())
            print()

    print(f"Wrote {OUT_XLSX}")


if __name__ == "__main__":
    main()
