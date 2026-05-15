"""
Profile a CSV dataset's shape into a summary CSV.

Run in Spyder. Produces <input_stem>__shape.csv next to the input CSV.
"""

import os

import numpy as np
import pandas as pd

# ---- EDIT THIS --------------------------------------------------------------
directory = r"C:\path\to\folder"
filename  = "your_dataset.csv"
# -----------------------------------------------------------------------------

directory = directory.replace("\\", "/")
input_path  = os.path.join(directory, filename)
stem        = os.path.splitext(filename)[0]
output_path = os.path.join(directory, stem + "__shape.csv")

# ---- Load -------------------------------------------------------------------
sampled_rows = None
try:
    df = pd.read_csv(input_path, low_memory=False)
except (MemoryError, pd.errors.ParserError):
    sampled_rows = 200_000
    df = pd.read_csv(input_path, low_memory=False, nrows=sampled_rows)

file_size_mb = os.path.getsize(input_path) / (1024 * 1024)
n_rows, n_cols = df.shape

# ---- Helpers ----------------------------------------------------------------
def _truncate(s, n=40):
    s = "" if s is None else str(s)
    s = s.replace("\n", " ").replace("\r", " ")
    return s if len(s) <= n else s[: n - 1] + "…"

def _fmt_num(x):
    if pd.isna(x):
        return ""
    if isinstance(x, (int, np.integer)):
        return f"{x:,}"
    ax = abs(x)
    if ax != 0 and (ax < 1e-3 or ax >= 1e7):
        return f"{x:.3g}"
    return f"{x:,.4g}"

def _is_datetime_like(series, threshold=0.8):
    s = series.dropna()
    if s.empty:
        return False, None
    sample = s.sample(min(len(s), 2000), random_state=0) if len(s) > 2000 else s
    parsed = pd.to_datetime(sample, errors="coerce", utc=False)
    frac = parsed.notna().mean()
    if frac >= threshold:
        full = pd.to_datetime(series, errors="coerce", utc=False)
        return True, full
    return False, None

# ---- Build per-column rows --------------------------------------------------
columns_header = [
    "column", "dtype", "n_missing", "pct_missing", "n_unique",
    "min", "max", "mean", "std", "median",
    "top_values", "examples",
]
rows = []

for col in df.columns:
    s = df[col]
    n_missing = int(s.isna().sum())
    pct_missing = f"{(n_missing / max(n_rows, 1) * 100):.1f}%"
    n_unique = int(s.nunique(dropna=True))

    dtype_str = str(s.dtype)
    min_v = max_v = mean_v = std_v = median_v = ""
    top_values = ""

    is_numeric = pd.api.types.is_numeric_dtype(s) and not pd.api.types.is_bool_dtype(s)
    is_dt = pd.api.types.is_datetime64_any_dtype(s)
    dt_series = None

    if not is_numeric and not is_dt:
        ok, parsed = _is_datetime_like(s)
        if ok:
            is_dt = True
            dt_series = parsed
            dtype_str = "datetime?"

    if is_numeric:
        try:
            min_v = _fmt_num(s.min())
            max_v = _fmt_num(s.max())
            mean_v = _fmt_num(s.mean())
            std_v = _fmt_num(s.std())
            median_v = _fmt_num(s.median())
        except Exception:
            pass
    elif is_dt:
        d = dt_series if dt_series is not None else pd.to_datetime(s, errors="coerce")
        try:
            min_v = str(d.min().date()) if pd.notna(d.min()) else ""
            max_v = str(d.max().date()) if pd.notna(d.max()) else ""
        except Exception:
            min_v, max_v = "", ""
    else:
        try:
            vc = s.dropna().astype(str).value_counts().head(3)
            top_values = ", ".join(f"{_truncate(v, 18)} ({c})" for v, c in vc.items())
        except Exception:
            top_values = ""

    try:
        examples_raw = s.dropna().head(2).tolist()
        examples = ", ".join(_truncate(v, 30) for v in examples_raw)
    except Exception:
        examples = ""

    rows.append([
        col,
        dtype_str,
        n_missing,
        pct_missing,
        n_unique,
        min_v, max_v, mean_v, std_v, median_v,
        top_values,
        examples,
    ])

# ---- Write CSV --------------------------------------------------------------
profile_df = pd.DataFrame(rows, columns=columns_header)
profile_df.to_csv(output_path, index=False)

print(f"Wrote {output_path}")
print(f"Shape: {n_rows:,} rows x {n_cols} cols  |  {file_size_mb:,.2f} MB")
if sampled_rows is not None:
    print(f"NOTE: profiled on first {sampled_rows:,} rows (full file too large to load).")
