"""Assemble the final public dataset from the validated extraction, the
committed override tables, and the population index.

Outputs (in DATA_DIR):

* shares_outstanding_{year}.csv — one row per share class per filing. Each
  row's identity is the registrant the count belongs to: `cik` is that
  registrant's OWN CIK (a subsidiary co-filer's own CIK in combined
  multi-registrant filings — e.g. AEP's seven operating companies — never
  the parent's), `registrant` its EDGAR conformed name, and
  `class_or_series` any below-registrant designation that is not itself a
  share class (a fund series, a tracking-stock group). The class itself is
  share_class_label / share_type / class_designator, then the count, the
  as-of date, and the validation provenance of every row.
* filing_coverage_{year}.csv — every filing in the population (including
  ABS-excluded ones) with its disposition, so the dataset's completeness is
  externally auditable: nothing is silently dropped.

Deterministic: inputs are the pipeline CSVs, the cached SGML headers, and
overrides.py / entity_aliases.py — no network, no sub-agents, no
randomness. Same code + same EDGAR state = same bytes.
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

from census_lib import load_filers, match_label_to_filer
from entity_aliases import ENTITY_ALIASES
from overrides import CONFIRMED, NO_SHARES, OVERRIDES

directory = DATA_DIR.replace("\\", "/")
pop = pd.read_csv(os.path.join(directory, "population_%d.csv" % year),
                  dtype=str, keep_default_na=False)
ext = pd.read_csv(os.path.join(directory, "extraction_%d.csv" % year),
                  dtype=str, keep_default_na=False)
status = pd.read_csv(os.path.join(directory, "filing_status_%d.csv" % year),
                     dtype=str, keep_default_na=False).set_index("accession")
facts = pd.read_csv(os.path.join(directory, "ixbrl_facts_%d.csv" % year),
                    dtype=str, keep_default_na=False)
facts_vals = {a: set(g["value"]) for a, g in facts.groupby("accession")}

XBRL_OK = {"XBRL_MATCH": "XBRL_MATCH", "XBRL_AGG_MATCH": "XBRL_AGG_MATCH"}
VALIDATED_STATUSES = {"VALIDATED", "AGG_VALIDATED"}

rows, coverage = [], []
ext_by = {a: g for a, g in ext.groupby("accession")}
_filers_cache = {}


def filers_for(acc):
    if acc not in _filers_cache:
        _filers_cache[acc] = load_filers(directory, acc)
    return _filers_cache[acc]


def resolve_entity(acc, p, label):
    """The registrant identity of one output row: (cik, registrant,
    class_or_series, flag). A label naming a co-registrant (matched against
    the filing's own SGML FILER blocks, or via the committed alias table)
    yields that registrant's OWN CIK; anything else is a series/group
    designation under the primary filer."""
    label = (label or "").strip()
    filers = filers_for(acc)
    alias = ENTITY_ALIASES.get((acc, label))
    if alias:
        for f in filers:
            if f["cik"] == alias:
                return f["cik"], f["name"], "", ""
    if label:
        f, rem = match_label_to_filer(label, filers)
        if f is not None and f["cik"]:
            return f["cik"], f["name"], rem, ""
        if int(p.n_filers or "1") > 1:
            return p.cik, p.company_name, label, "ENTITY_LABEL_UNMATCHED"
        return p.cik, p.company_name, label, ""
    return p.cik, p.company_name, "", ""


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
            out_rows.append(rr)
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
                "as_of": r["as_of"], "registrant": r["registrant"],
                "flags": r["flags"]})
        validation = "XBRL_MATCH" if st == "VALIDATED" else "XBRL_AGG_MATCH"
        disposition = "ROWS_VALIDATED_XBRL"
    elif acc in CONFIRMED:
        g = ext_by.get(acc)
        for _, r in (g.iterrows() if g is not None else []):
            out_rows.append({
                "value": r["value"], "label": r["share_class_label"],
                "share_type": r["share_type"],
                "class_designator": r["class_designator"],
                "as_of": r["as_of"], "registrant": r["registrant"],
                "flags": r["flags"]})
        validation = CONFIRMED[acc]
        disposition = "ROWS_CONFIRMED_READS"
    else:
        disposition = "UNRESOLVED: " + st

    for r in out_rows:
        row_validation, row_flags = validation, r.get("flags", "")
        if validation == "OVERRIDE_VERIFIED" and \
                str(r["value"]) in facts_vals.get(acc, set()):
            # the value itself is the filer's own tagged number; the override
            # supplied attribution (labels/dates), not the count
            row_validation = "XBRL_MATCH"
            row_flags = ";".join(filter(None, [row_flags, "OVERRIDE_ATTRIBUTION"]))
        cik, registrant, class_or_series, ent_flag = \
            resolve_entity(acc, p, r.get("registrant", ""))
        if ent_flag:
            row_flags = ";".join(filter(None, [row_flags, ent_flag]))
        rows.append({
            "accession": acc, "cik": cik, "registrant": registrant,
            "class_or_series": class_or_series,
            "share_class_label": r["label"], "share_type": r["share_type"],
            "class_designator": r["class_designator"],
            "form": p.form, "date_filed": p.date_filed,
            "shares_outstanding": r["value"], "as_of_date": r["as_of"],
            "validation": row_validation,
            "quality_flags": row_flags,
            "filing_index_url": p.filing_index_url,
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
unmatched = df[df["quality_flags"].str.contains("ENTITY_LABEL_UNMATCHED")]
print("\nENTITY_LABEL_UNMATCHED rows (review; alias or series?): %d"
      % len(unmatched))
if len(unmatched):
    print(unmatched[["accession", "registrant", "class_or_series"]]
          .drop_duplicates().to_string(index=False))
