"""Reconcile ISS and FactSet shareholder-proposal datasets.

Stage 1: Load and normalize.
Stage 2: Anti-join on (cusip_6, Meeting_Date); write iss_only.csv, fs_only.csv.
Stage 3: Distributional diagnostic on the iss_only rows vs matched rows.
"""

import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
# Directory where input files live and where outputs get written.
save_directory = "/Users/avilae/claude-code/projects/edgar-projects/iss-factset-reconciliation"
# -----------------------------------------------------------------------------

ISS_PATH = os.path.join(save_directory, "ISS_proposals_2016to2025.xlsx")
FS_PATH = os.path.join(save_directory, "Factset_proposals_2016to2025.xlsx")
ISS_ONLY_CSV = os.path.join(save_directory, "iss_only.csv")
FS_ONLY_CSV = os.path.join(save_directory, "fs_only.csv")
DIAGNOSTIC_XLSX = os.path.join(save_directory, "iss_only_diagnostic.xlsx")

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

FS_STRING_COLS = [
    "CompanyName", "Meeting_Year", "cusip_6", "CIK_from_url", "Symbol",
    "EntityID", "Sedol", "Cusip", "Ticker",
]

DIAGNOSTIC_COLS = [
    "indexname", "meeting_code", "sponsor_type", "resolution_type",
    "other_status", "omit_reason", "Meeting_Year",
]


def normalize_cusip6(series: pd.Series) -> pd.Series:
    """Force to string, strip trailing .0, zero-pad to 6.
    Letter-containing CUSIPs pass through untouched after the strip/pad."""
    s = series.astype(str).str.strip()
    # Strip trailing .0 (Excel numeric coercion artifact).
    s = s.str.replace(r"\.0+$", "", regex=True)
    # Zero-pad to 6 on the left. This is safe for letter-containing codes too.
    s = s.str.zfill(6)
    return s


def load_iss(path: str) -> pd.DataFrame:
    dtype = {c: str for c in ISS_STRING_COLS}
    df = pd.read_excel(path, dtype=dtype, engine="openpyxl")
    for col in ISS_NUMERIC:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["cusip_6"] = normalize_cusip6(df["cusip_6"])
    df["Meeting_Date"] = pd.to_datetime(df["Meeting_Date"], errors="coerce")
    # Normalize blanks: NaN and "nan" string artifacts -> "".
    for col in ISS_STRING_COLS:
        df[col] = df[col].fillna("").replace({"nan": "", "NaT": ""})
    return df


def load_factset(path: str) -> pd.DataFrame:
    dtype = {c: str for c in FS_STRING_COLS}
    df = pd.read_excel(path, dtype=dtype, engine="openpyxl")
    df["cusip_6"] = normalize_cusip6(df["cusip_6"])
    df["Meeting_Date"] = pd.to_datetime(df["Meeting_Date"], errors="coerce")
    for col in FS_STRING_COLS:
        df[col] = df[col].fillna("").replace({"nan": "", "NaT": ""})
    return df


def anti_join(left: pd.DataFrame, right: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    """Return rows of `left` whose `keys` combination is absent from `right`."""
    right_keys = right[keys].drop_duplicates()
    merged = left.merge(right_keys, on=keys, how="left", indicator=True)
    only = merged[merged["_merge"] == "left_only"].drop(columns="_merge")
    return only


def diagnostic_table(iss_df: pd.DataFrame, iss_only_keys: set, col: str) -> pd.DataFrame:
    """Side-by-side distribution of `col` for matched vs iss_only groups."""
    flags = pd.Series(
        ["iss_only" if k in iss_only_keys else "matched"
         for k in zip(iss_df["cusip_6"], iss_df["Meeting_Date"])],
        index=iss_df.index,
    )
    # Treat blanks as their own category; preserve them.
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
    print("Loading ISS…")
    iss = load_iss(ISS_PATH)
    print("Loading FactSet…")
    fs = load_factset(FS_PATH)

    keys = ["cusip_6", "Meeting_Date"]

    iss_only = anti_join(iss, fs, keys)
    fs_only = anti_join(fs, iss, keys)

    iss_only.to_csv(ISS_ONLY_CSV, index=False)
    fs_only.to_csv(FS_ONLY_CSV, index=False)

    iss_meetings = iss[keys].drop_duplicates().shape[0]
    iss_only_meetings = iss_only[keys].drop_duplicates().shape[0]

    print()
    print("=" * 60)
    print("ROW COUNTS")
    print("=" * 60)
    print(f"ISS rows (proposal-grain):          {len(iss):>8,}")
    print(f"ISS unique meetings:                {iss_meetings:>8,}")
    print(f"FactSet rows (meeting-grain):       {len(fs):>8,}")
    print(f"iss_only rows (proposal-grain):     {len(iss_only):>8,}")
    print(f"iss_only unique meetings:           {iss_only_meetings:>8,}")
    print(f"fs_only rows:                       {len(fs_only):>8,}")
    print()

    # Build iss_only key set for the diagnostic flagging.
    iss_only_keys = set(zip(iss_only["cusip_6"], iss_only["Meeting_Date"]))

    with pd.ExcelWriter(DIAGNOSTIC_XLSX, engine="openpyxl") as writer:
        for col in DIAGNOSTIC_COLS:
            table = diagnostic_table(iss, iss_only_keys, col)
            sheet_name = col[:31] or "blank"
            table.to_excel(writer, sheet_name=sheet_name)
            print("=" * 60)
            print(f"DIAGNOSTIC: {col}")
            print("=" * 60)
            print(table.to_string())
            print()

    print(f"Wrote {ISS_ONLY_CSV} ({len(iss_only):,} rows)")
    print(f"Wrote {FS_ONLY_CSV} ({len(fs_only):,} rows)")
    print(f"Wrote {DIAGNOSTIC_XLSX}")


if __name__ == "__main__":
    main()
