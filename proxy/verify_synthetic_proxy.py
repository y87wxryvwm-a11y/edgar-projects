"""Verify that synthetic_proxy.csv reproduces every cell in dataset_shape.csv.

Recomputes the per-column summary (dtype, n_missing, n_unique, mean, median,
important_values counts, and range_min/range_max where the notes column states
a range) and prints PASS/FAIL for each aspect. If every check passes, a fresh
shape printout of the dataset would match dataset_shape.csv.
"""

import os
import re

import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
data_filename = "synthetic_proxy.csv"
spec_filename = "dataset_shape.csv"
# -----------------------------------------------------------------------------

# directory is read from config.py (git-ignored) so each machine keeps its own
# data-folder path and a `git pull` never overwrites it. Copy config.example.py
# to config.py in this folder and set DATA_DIR.
try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py in this "
        "folder and set DATA_DIR to your proxy data folder."
    )

directory = DATA_DIR.replace("\\", "/")
data_path = os.path.join(directory, data_filename)
spec_path = os.path.join(directory, spec_filename)

df = pd.read_csv(data_path)
# Load spec as raw strings so we can preserve the decimal precision used in
# each cell (".7483" implies 4-decimal rounding; "20.68" implies 2).
spec = pd.read_csv(spec_path, dtype=str)


def _is_blank(s):
    return pd.isna(s) or (isinstance(s, str) and s.strip() == "")


def parse_float(s):
    if _is_blank(s):
        return None
    return float(s)


def parse_int(s):
    if _is_blank(s):
        return None
    return int(float(s))


def expected_decimals(s):
    """How many decimal places does the spec write this number with?"""
    if _is_blank(s):
        return 0
    s = str(s).strip()
    return len(s.split(".")[1]) if "." in s else 0


def parse_important_values(s):
    if _is_blank(s):
        return []
    return [(name, int(count)) for name, count in re.findall(r'"([^"]+)"\s*\((\d+)\)', s)]


def parse_range(notes):
    if not isinstance(notes, str):
        return None
    m = re.search(r"range.*?(\d+)\s+to\s+(\d+)", notes)
    return (int(m.group(1)), int(m.group(2))) if m else None


def normalize_dtype(d):
    # pandas 3.x reports "str" for what 2.x called "object".
    return "object" if d == "str" else d


def coerce_to_column(col, name):
    """Coerce a string label from important_values into the column's value type."""
    if pd.api.types.is_integer_dtype(df[col]):
        try:
            return int(name)
        except ValueError:
            return name
    return name


results = []


def check(col, aspect, expected, actual):
    results.append((col, aspect, expected == actual, expected, actual))


for _, row in spec.iterrows():
    col = row["column"]
    if col not in df.columns:
        results.append((col, "exists", False, "present", "MISSING"))
        continue

    check(col, "dtype", row["dtype"], normalize_dtype(str(df[col].dtype)))
    check(col, "n_missing", parse_int(row["n_missing"]), int(df[col].isna().sum()))
    check(col, "n_unique", parse_int(row["n_unique"]), int(df[col].nunique(dropna=True)))

    expected_mean = parse_float(row["mean"])
    if expected_mean is not None:
        decimals = expected_decimals(row["mean"])
        check(col, "mean", expected_mean, round(float(df[col].mean()), decimals))

    expected_median = parse_float(row["median"])
    if expected_median is not None:
        check(col, "median", expected_median, float(df[col].median()))

    for name, expected_count in parse_important_values(row["important_values"]):
        actual_count = int((df[col] == coerce_to_column(col, name)).sum())
        check(col, f'count["{name}"]', expected_count, actual_count)

    rng_bounds = parse_range(row["notes"])
    if rng_bounds is not None:
        lo, hi = rng_bounds
        check(col, "range_min", lo, int(df[col].min()))
        check(col, "range_max", hi, int(df[col].max()))


passed_count = sum(1 for r in results if r[2])
total = len(results)
failed = [r for r in results if not r[2]]

print(f"{'STATUS':6s} {'COLUMN':28s} {'ASPECT':22s} {'EXPECTED':22s} ACTUAL")
print("-" * 100)
for col, aspect, ok, expected, actual in results:
    status = "PASS" if ok else "FAIL"
    print(f"{status:6s} {col:28s} {aspect:22s} {repr(expected):22s} {repr(actual)}")

print()
print(f"{passed_count}/{total} aspects passed")
if failed:
    print()
    print("VERIFICATION FAILED")
    for col, aspect, _, expected, actual in failed:
        print(f"  {col} / {aspect}: expected {expected!r}, got {actual!r}")
