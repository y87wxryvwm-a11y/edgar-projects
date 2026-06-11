"""Assemble the final public dataset from the validated extraction, the
committed override tables, and the population index.

Outputs (in DATA_DIR):

* shares_outstanding_{year}.csv — one row per share class per filing:
  identity (accession, CIK, company, form, URL), the class (label, type,
  designator, registrant for combined filings), the count, the as-of date,
  and the validation provenance of every row.
* filing_coverage_{year}.csv — every filing in the population (including
  ABS-excluded ones) with its disposition, so the dataset's completeness is
  externally auditable: nothing is silently dropped.

Deterministic: inputs are the pipeline CSVs plus overrides.py — no network,
no sub-agents, no randomness. Same code + same EDGAR state = same bytes.
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
# -----------------------------------------------------------------------------

import os

import pandas as pd

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set "
        "USER_AGENT and DATA_DIR."
    )

from overrides import CONFIRMED, NO_SHARES, OVERRIDES

directory = DATA_DIR.replace("\\", "/")
pop = pd.read_csv(os.path.join(directory, "population_%d.csv" % year),
                  dtype=str, keep_default_na=False)
ext = pd.read_csv(os.path.join(directory, "extraction_%d.csv" % year),
                  dtype=str, keep_default_na=False)
status = pd.read_csv(os.path.join(directory, "filing_status_%d.csv" % year),
                     dtype=str, keep_default_na=False).set_index("accession")

XBRL_OK = {"XBRL_MATCH": "XBRL_MATCH", "XBRL_AGG_MATCH": "XBRL_AGG_MATCH"}
VALIDATED_STATUSES = {"VALIDATED", "AGG_VALIDATED"}

rows, coverage = [], []
ext_by = {a: g for a, g in ext.groupby("accession")}

for p in pop.itertuples(index=False):
    acc = p.accession
    if p.excluded_abs == "True":
        coverage.append({"accession": acc, "cik": p.cik,
                         "company_name": p.company_name, "form": p.form,
                         "disposition": "EXCLUDED_ABS_SIC_6189", "n_rows": 0})
        continue
    st = status.loc[acc]["status"] if acc in status.index else "?"
    out_rows, validation, disposition = [], "", ""

    if acc in OVERRIDES:
        o = OVERRIDES[acc]
        for r in o["rows"]:
            out_rows.append(dict(r))
        validation = "OVERRIDE_VERIFIED"
        disposition = "ROWS_FROM_OVERRIDE"
    elif acc in NO_SHARES:
        disposition = "NO_SHARE_COUNT_DISCLOSED: " + NO_SHARES[acc]
    elif st in VALIDATED_STATUSES:
        g = ext_by.get(acc)
        for _, r in (g.iterrows() if g is not None else []):
            out_rows.append({
                "value": r["value"], "label": r["share_class_label"],
                "share_type": r["share_type"],
                "class_designator": r["class_designator"],
                "as_of": r["as_of"], "registrant": r["registrant"]})
        validation = "XBRL_MATCH" if st == "VALIDATED" else "XBRL_AGG_MATCH"
        disposition = "ROWS_VALIDATED_XBRL"
    elif acc in CONFIRMED:
        g = ext_by.get(acc)
        for _, r in (g.iterrows() if g is not None else []):
            out_rows.append({
                "value": r["value"], "label": r["share_class_label"],
                "share_type": r["share_type"],
                "class_designator": r["class_designator"],
                "as_of": r["as_of"], "registrant": r["registrant"]})
        validation = CONFIRMED[acc]
        disposition = "ROWS_CONFIRMED_READS"
    else:
        disposition = "UNRESOLVED: " + st

    for r in out_rows:
        rows.append({
            "accession": acc, "cik": p.cik, "company_name": p.company_name,
            "form": p.form, "date_filed": p.date_filed,
            "filing_index_url": p.filing_index_url,
            "share_class_label": r["label"], "share_type": r["share_type"],
            "class_designator": r["class_designator"],
            "registrant": r.get("registrant", ""),
            "shares_outstanding": r["value"], "as_of_date": r["as_of"],
            "validation": validation,
        })
    coverage.append({"accession": acc, "cik": p.cik,
                     "company_name": p.company_name, "form": p.form,
                     "disposition": disposition, "n_rows": len(out_rows)})

df = pd.DataFrame(rows).sort_values(
    ["accession", "shares_outstanding"]).reset_index(drop=True)
cov = pd.DataFrame(coverage).sort_values("accession").reset_index(drop=True)
out1 = os.path.join(directory, "shares_outstanding_%d.csv" % year)
out2 = os.path.join(directory, "filing_coverage_%d.csv" % year)
df.to_csv(out1, index=False, encoding="utf-8", lineterminator="\n")
cov.to_csv(out2, index=False, encoding="utf-8", lineterminator="\n")

print("wrote %s (%d class rows, %d filings)" %
      (out1, len(df), df["accession"].nunique()))
print("wrote %s (%d filings)" % (out2, len(cov)))
print("\ncoverage dispositions:")
print(cov.groupby(cov["disposition"].str.split(":").str[0]).size().to_string())
unresolved = cov[cov["disposition"].str.startswith("UNRESOLVED")]
print("\nUNRESOLVED remaining: %d" % len(unresolved))
print("\nvalidation provenance of rows:")
print(df.groupby("validation").size().to_string())
