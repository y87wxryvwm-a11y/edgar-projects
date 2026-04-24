"""Compare two iss_only outputs (v1 vs v2).

Reports row/column counts, key-set overlap (cusip_6 + Meeting_Date),
and per-column value-distribution differences for string columns.
"""

import os

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
save_directory = "/Users/avilae/claude-code/projects/edgar-projects/iss-factset-reconciliation"
v1_filename = "iss_only.csv"
v2_filename = "iss_only_v2.csv"
# -----------------------------------------------------------------------------

v1_path = os.path.join(save_directory, v1_filename)
v2_path = os.path.join(save_directory, v2_filename)

KEY = ["cusip_6", "Meeting_Date"]


FILTER_YEAR = 2025


def load(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"cusip_6": str})
    df["Meeting_Date"] = pd.to_datetime(df["Meeting_Date"], errors="coerce")
    df = df[df["Meeting_Date"].dt.year == FILTER_YEAR].copy()
    return df


def section(title: str) -> None:
    print()
    print("=" * 64)
    print(title)
    print("=" * 64)


def basic_info(v1: pd.DataFrame, v2: pd.DataFrame) -> None:
    section("BASIC INFO")
    print(f"v1 file: {v1_filename}  (filtered to Meeting_Date year == {FILTER_YEAR})")
    print(f"v2 file: {v2_filename}  (filtered to Meeting_Date year == {FILTER_YEAR})")
    print(f"v1 rows:     {len(v1):>8,}     v1 cols: {v1.shape[1]}")
    print(f"v2 rows:     {len(v2):>8,}     v2 cols: {v2.shape[1]}")
    print(f"row delta:   {len(v2) - len(v1):>+8,}")

    only_v1_cols = [c for c in v1.columns if c not in v2.columns]
    only_v2_cols = [c for c in v2.columns if c not in v1.columns]
    print(f"columns only in v1: {only_v1_cols or '(none)'}")
    print(f"columns only in v2: {only_v2_cols or '(none)'}")


def key_overlap(v1: pd.DataFrame, v2: pd.DataFrame) -> tuple[set, set]:
    section("KEY OVERLAP (cusip_6, Meeting_Date)")
    v1_keys = set(zip(v1["cusip_6"], v1["Meeting_Date"]))
    v2_keys = set(zip(v2["cusip_6"], v2["Meeting_Date"]))
    both = v1_keys & v2_keys
    only_v1 = v1_keys - v2_keys
    only_v2 = v2_keys - v1_keys
    print(f"unique keys in v1:    {len(v1_keys):>8,}")
    print(f"unique keys in v2:    {len(v2_keys):>8,}")
    print(f"keys in both:         {len(both):>8,}")
    print(f"keys only in v1:      {len(only_v1):>8,}")
    print(f"keys only in v2:      {len(only_v2):>8,}")
    return only_v1, only_v2


def string_columns(df: pd.DataFrame) -> list[str]:
    return [
        c for c in df.columns
        if df[c].dtype == object and c not in ("Meeting_Date",)
    ]


def compare_string_distribution(
    v1: pd.DataFrame, v2: pd.DataFrame, col: str, top_n: int = 15
) -> None:
    s1 = v1[col].fillna("(blank)").astype(str).replace("", "(blank)")
    s2 = v2[col].fillna("(blank)").astype(str).replace("", "(blank)")

    c1 = s1.value_counts()
    c2 = s2.value_counts()

    df = pd.DataFrame({"v1_count": c1, "v2_count": c2}).fillna(0).astype(int)
    df["delta"] = df["v2_count"] - df["v1_count"]
    df["v1_pct"] = (df["v1_count"] / max(len(s1), 1) * 100).round(2)
    df["v2_pct"] = (df["v2_count"] / max(len(s2), 1) * 100).round(2)
    df["pct_delta"] = (df["v2_pct"] - df["v1_pct"]).round(2)

    uniq_v1 = s1.nunique()
    uniq_v2 = s2.nunique()
    print(f"\n--- {col} ---")
    print(f"unique values: v1={uniq_v1}, v2={uniq_v2}")

    df_sorted = df.reindex(df["delta"].abs().sort_values(ascending=False).index)
    shown = df_sorted.head(top_n)
    print(shown.to_string())
    if len(df) > top_n:
        print(f"... ({len(df) - top_n} more rows)")


def main() -> None:
    v1 = load(v1_path)
    v2 = load(v2_path)

    basic_info(v1, v2)
    key_overlap(v1, v2)

    section("STRING-COLUMN DISTRIBUTIONS (top 15 by |delta|)")
    shared_cols = [c for c in v1.columns if c in v2.columns]
    for col in string_columns(v1):
        if col not in shared_cols:
            continue
        compare_string_distribution(v1, v2, col)


if __name__ == "__main__":
    main()
