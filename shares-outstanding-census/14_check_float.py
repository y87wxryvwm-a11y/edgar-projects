"""Assertion suite for the final public-float dataset. Run after script 12;
every failure prints; exits nonzero only on hard failures.

Checks:
1. Coverage completeness — every population filing appears exactly once in
   float_coverage, no UNRESOLVED rows remain.
2. Row integrity — every public_float row's accession is in the population,
   values parse as nonnegative numbers, dates are ISO and not after the
   filing date by more than a few days.
3. Cross-dataset sanity — joined to shares_outstanding_{year}.csv (the
   shares census), implied per-share price = float / total shares must land
   in a sane band; violations are listed for review (soft).
4. Scale sanity — cover floats above $5T are listed (nothing but the very
   largest issuers should ever approach this; a violation usually means a
   scale error survived).
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
# -----------------------------------------------------------------------------

import os
import sys

import pandas as pd

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set "
        "USER_AGENT and DATA_DIR."
    )

directory = DATA_DIR.replace("\\", "/")
pf = pd.read_csv(os.path.join(directory, "public_float_%d.csv" % year),
                 dtype=str, keep_default_na=False)
cov = pd.read_csv(os.path.join(directory, "float_coverage_%d.csv" % year),
                  dtype=str, keep_default_na=False)
pop = pd.read_csv(os.path.join(directory, "population_%d.csv" % year),
                  dtype=str, keep_default_na=False)

hard_fail = []

# 1. coverage completeness
if sorted(cov["accession"]) != sorted(pop["accession"]):
    hard_fail.append("coverage accessions != population accessions")
if cov["accession"].duplicated().any():
    hard_fail.append("duplicate accessions in coverage")
unresolved = cov[cov["disposition"].str.startswith("UNRESOLVED")]
if len(unresolved):
    hard_fail.append("%d UNRESOLVED dispositions remain" % len(unresolved))
    print(unresolved.head(20).to_string())

# 2. row integrity
pop_accs = set(pop["accession"])
bad = pf[~pf["accession"].isin(pop_accs)]
if len(bad):
    hard_fail.append("%d rows with accession outside population" % len(bad))
vals = pd.to_numeric(pf["public_float_cover"], errors="coerce")
if vals.isna().any():
    hard_fail.append("%d unparseable public_float_cover values"
                     % vals.isna().sum())
if (vals < 0).any():
    hard_fail.append("negative float values")
dated = pf[pf["public_float_date"] != ""]
bad_date = dated[~dated["public_float_date"].str.match(
    r"\d{4}-\d{2}-\d{2}$")]
if len(bad_date):
    hard_fail.append("%d malformed dates" % len(bad_date))
late = dated[(dated["public_float_date"] > dated["date_filed"])]
if len(late):
    print("NOTE: %d rows dated after filing date (verify):" % len(late))
    print(late[["accession", "company_name", "public_float_date",
                "date_filed"]].to_string(index=False))

# 3. implied price vs shares census (soft)
so_path = os.path.join(directory, "shares_outstanding_%d.csv" % year)
if os.path.exists(so_path):
    so = pd.read_csv(so_path, dtype=str, keep_default_na=False)
    so["shares"] = pd.to_numeric(so["shares_outstanding"], errors="coerce")
    tot = so.groupby("accession")["shares"].sum()
    j = pf.copy()
    j["v"] = vals
    j = j[j["v"] > 0]
    j["shares"] = j["accession"].map(tot)
    j = j[j["shares"] > 0]
    j["implied"] = j["v"] / j["shares"]
    odd = j[(j["implied"] < 0.00005) | (j["implied"] > 800000)]
    print("\nimplied price check: %d rows joined, %d outside "
          "[0.00005, 800000] $/share" % (len(j), len(odd)))
    if len(odd):
        print(odd[["accession", "company_name", "public_float_cover",
                   "shares", "implied", "validation"]]
              .sort_values("implied").to_string(index=False))

# 4. scale sanity
huge = pf[vals > 5e12]
if len(huge):
    print("\nfloats above $5T (review each):")
    print(huge[["accession", "company_name", "public_float_cover",
                "validation", "quality_flags"]].to_string(index=False))

print("\nrows: %d, filings with rows: %d, coverage rows: %d"
      % (len(pf), pf["accession"].nunique(), len(cov)))
if hard_fail:
    print("\nHARD FAILURES:")
    for h in hard_fail:
        print("  - " + h)
    sys.exit(1)
print("\nall hard checks passed")
