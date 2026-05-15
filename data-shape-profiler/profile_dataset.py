"""
Profile a CSV dataset's shape into a single screenshottable PNG.

Run in Spyder. Produces <input_stem>__shape.png next to the input CSV.
"""

import os
from datetime import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# ---- EDIT THIS --------------------------------------------------------------
directory = r"C:\path\to\folder"
filename  = "your_dataset.csv"
# -----------------------------------------------------------------------------

directory = directory.replace("\\", "/")
input_path  = os.path.join(directory, filename)
stem        = os.path.splitext(filename)[0]
output_path = os.path.join(directory, stem + "__shape.png")

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
        _truncate(col, 36),
        dtype_str,
        f"{n_missing:,}",
        pct_missing,
        f"{n_unique:,}",
        min_v, max_v, mean_v, std_v, median_v,
        _truncate(top_values, 60),
        _truncate(examples, 50),
    ])

# ---- Render PNG -------------------------------------------------------------
fig_w = 22.0
fig_h = max(3.0, 1.6 + 0.32 * n_cols)
fig, ax = plt.subplots(figsize=(fig_w, fig_h))
ax.axis("off")

header_lines = [
    f"File:  {filename}    ({file_size_mb:,.2f} MB)",
    f"Shape: {n_rows:,} rows  x  {n_cols} columns",
    f"Profiled: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
]
if sampled_rows is not None:
    header_lines.append(f"NOTE: profiled on first {sampled_rows:,} rows (full file too large to load).")
if n_cols > 60:
    header_lines.append(f"NOTE: {n_cols} columns — image is tall; zoom in when screenshotting.")

fig.text(0.01, 0.995, "\n".join(header_lines),
         ha="left", va="top", family="monospace", fontsize=11)

# Approximate column widths (sum to ~1.0) tuned for content
col_widths = [0.14, 0.06, 0.05, 0.05, 0.06,
              0.07, 0.07, 0.06, 0.06, 0.06,
              0.17, 0.15]

table = ax.table(
    cellText=rows,
    colLabels=columns_header,
    colWidths=col_widths,
    loc="upper left",
    cellLoc="left",
    bbox=[0.0, 0.0, 1.0, 1.0 - (0.04 + 0.022 * len(header_lines))],
)
table.auto_set_font_size(False)
table.set_fontsize(8.5)

# Style header row
for j in range(len(columns_header)):
    cell = table[(0, j)]
    cell.set_facecolor("#1f3a5f")
    cell.set_text_props(color="white", weight="bold")

# Zebra striping
for i in range(1, len(rows) + 1):
    for j in range(len(columns_header)):
        if i % 2 == 0:
            table[(i, j)].set_facecolor("#f2f4f8")

footer = ("Legend: dtype shown as pandas dtype, 'datetime?' = auto-detected from strings.  "
          "top_values = top-3 categorical values with counts.  "
          "Cells truncated with '…'.")
fig.text(0.01, 0.005, footer, ha="left", va="bottom",
         family="monospace", fontsize=8, color="#444")

fig.savefig(output_path, dpi=160, bbox_inches="tight")
plt.close(fig)

print(f"Wrote {output_path}")
print(f"Shape: {n_rows:,} rows x {n_cols} cols")
