"""Assemble the final public-float dataset from the validated extraction,
the committed override tables, and the population index.

Outputs (in DATA_DIR):

* public_float_{year}.csv — one row per filing (per registrant for the few
  combined multi-registrant filings): identity (accession, CIK, company,
  form, URL), the cover's stated float (public_float_cover, plain dollars),
  the filer's tagged value as filed (public_float_xbrl), the as-of date,
  and the validation provenance. Filings whose covers disclose no float
  (every 20-F/40-F, wholly-owned and shell 10-K filers) have no row here —
  the coverage table accounts for them.
* float_coverage_{year}.csv — every filing in the population (including
  ABS-excluded ones) with its disposition, so completeness is externally
  auditable: nothing is silently dropped.

Deterministic: inputs are the pipeline CSVs plus float_overrides.py — no
network, no sub-agents, no randomness. Same code + same EDGAR state = same
bytes.
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

from float_overrides import CONFIRMED, NO_FLOAT, OVERRIDES

directory = DATA_DIR.replace("\\", "/")
pop = pd.read_csv(os.path.join(directory, "population_%d.csv" % year),
                  dtype=str, keep_default_na=False)
ext = pd.read_csv(os.path.join(directory, "float_extraction_%d.csv" % year),
                  dtype=str, keep_default_na=False)
status = pd.read_csv(os.path.join(directory, "float_status_%d.csv" % year),
                     dtype=str, keep_default_na=False).set_index("accession")

VALIDATED_STATUSES = {"VALIDATED", "AGG_VALIDATED"}
NO_FLOAT_STATUSES = {"NO_FLOAT_STATED", "ZERO_FACT", "NIL_FACT", "EMPTY"}

rows, coverage = [], []
ext_by = {a: g for a, g in ext.groupby("accession")}


def ext_rows(acc, drop_components=True):
    g = ext_by.get(acc)
    if g is None:
        return []
    out = []
    has_total = any("TOTAL_OF_COMPONENTS" in f for f in g["flags"])
    for _, r in g.iterrows():
        flags = r["flags"]
        if drop_components and has_total and "COMPONENT" in flags.split(";"):
            continue
        out.append({
            "cover": r["value"], "xbrl": r["value_xbrl"],
            "as_of": r["as_of"], "label": r["label"],
            "flags": ";".join(sorted(set(
                f for f in flags.split(";") if f))), "xbrl_verdict": r["xbrl"],
        })
    return out


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
            rr = dict(r)
            rr.setdefault("flags", "")
            rr.setdefault("xbrl", "")
            rr.setdefault("label", "")
            out_rows.append(rr)
        validation = "OVERRIDE_VERIFIED"
        disposition = "ROWS_FROM_OVERRIDE"
    elif acc in NO_FLOAT:
        disposition = "NO_FLOAT_DISCLOSED: " + NO_FLOAT[acc]
    elif st in VALIDATED_STATUSES:
        out_rows = ext_rows(acc)
        validation = "XBRL_MATCH" if st == "VALIDATED" else "XBRL_AGG_MATCH"
        disposition = "ROWS_VALIDATED_XBRL"
    elif acc in CONFIRMED:
        out_rows = ext_rows(acc)
        validation = CONFIRMED[acc]
        disposition = "ROWS_CONFIRMED_READS"
        if st == "SCALE_DISCREPANCY":
            disposition = "ROWS_CONFIRMED_READS_TAG_SCALE_ERROR"
    elif st in NO_FLOAT_STATUSES:
        kind = {"NO_FLOAT_STATED": "STATED_ON_COVER",
                "ZERO_FACT": "TAGGED_ZERO_COVER_SILENT",
                "NIL_FACT": "TAGGED_NIL_COVER_SILENT",
                "EMPTY": "NOT_DISCLOSED"}[st]
        disposition = "NO_FLOAT_DISCLOSED: " + kind
    else:
        disposition = "UNRESOLVED: " + st

    for r in out_rows:
        flags = r.get("flags", "")
        v = r.get("xbrl_verdict", "")
        # filer-side oddities reported as filed, but visibly: a float dated
        # after the filing itself (a cover typo) and price-sized nonzero
        # "floats" the filer stated and tagged identically
        if r.get("as_of", "") and r["as_of"] > p.date_filed:
            flags = ";".join(filter(None, [flags, "IMPLAUSIBLE_DATE"]))
        try:
            _v = float(r["cover"])
        except (TypeError, ValueError):
            _v = None
        if _v is not None and 0 < _v < 1000:
            flags = ";".join(filter(None, [flags, "AS_FILED_MICRO_VALUE"]))
        row_validation = validation
        if validation.startswith("READS_") and v == "XBRL_MATCH":
            # the reads confirmed the filing; this row ALSO equals the
            # filer's own tag — the stronger provenance stands
            row_validation = "XBRL_MATCH"
        if validation in ("XBRL_MATCH", "XBRL_AGG_MATCH"):
            if v == "XBRL_AGG_MATCH":
                row_validation = "XBRL_AGG_MATCH"
            elif v == "COMPONENT_OF_MATCHED_TOTAL":
                row_validation = "XBRL_TOTAL_MATCH"
            elif v == "XBRL_ABSENT":
                # a zero row beside validated rows (a "None" registrant)
                row_validation = "STATED_ON_COVER"
        rows.append({
            "accession": acc, "cik": p.cik, "company_name": p.company_name,
            "form": p.form, "date_filed": p.date_filed,
            "filing_index_url": p.filing_index_url,
            "registrant_or_class": r.get("label", ""),
            "public_float_cover": r["cover"],
            "public_float_xbrl": r.get("xbrl", ""),
            "public_float_date": r.get("as_of", ""),
            "validation": row_validation,
            "quality_flags": flags,
        })
    coverage.append({"accession": acc, "cik": p.cik,
                     "company_name": p.company_name, "form": p.form,
                     "disposition": disposition, "n_rows": len(out_rows)})

df = pd.DataFrame(rows).sort_values(
    ["accession", "public_float_cover"]).reset_index(drop=True)
cov = pd.DataFrame(coverage).sort_values("accession").reset_index(drop=True)
out1 = os.path.join(directory, "public_float_%d.csv" % year)
out2 = os.path.join(directory, "float_coverage_%d.csv" % year)
df.to_csv(out1, index=False, encoding="utf-8", lineterminator="\n")
cov.to_csv(out2, index=False, encoding="utf-8", lineterminator="\n")

print("wrote %s (%d float rows, %d filings)" %
      (out1, len(df), df["accession"].nunique()))
print("wrote %s (%d filings)" % (out2, len(cov)))
print("\ncoverage dispositions:")
print(cov.groupby(cov["disposition"].str.split(":").str[0]).size().to_string())
unresolved = cov[cov["disposition"].str.startswith("UNRESOLVED")]
print("\nUNRESOLVED remaining: %d" % len(unresolved))
print("\nvalidation provenance of rows:")
print(df.groupby("validation").size().to_string())
