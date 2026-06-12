"""Assemble the final public-float dataset from the validated extraction,
the committed override tables, and the population index.

Outputs (in DATA_DIR):

* public_float_{year}.csv — one row per disclosed float (per registrant for
  combined multi-registrant filings, per class/series where the cover breaks
  the value out below the registrant). Each row's identity is the registrant
  it belongs to: `cik` is that registrant's OWN CIK (a subsidiary co-filer's
  own CIK, never its parent's), `registrant` its EDGAR conformed name, and
  `class_or_series` the below-registrant designation (share class, fund
  series) when the cover prints one. `public_float` is the single verified
  value with `float_basis` saying what kind of disclosure it is:
      STATED_VALUE         the cover prints a dollar value
      STATED_ZERO          the cover prints $0 / zero
      STATED_NONE          the cover prints "None" (wholly-owned
                           registrants in combined utility filings)
      RESOLVED_FILER_ERROR both as-filed numbers are wrong; the verified
                           value comes from a logged override judgment
                           (see float_overrides.py provenance)
  The as-filed evidence stays in public_float_cover (the cover as printed,
  plain dollars) and public_float_xbrl (the filer's tag as filed).
  Filings whose covers disclose no float (every 20-F/40-F, wholly-owned and
  shell 10-K filers) have no row here — the coverage table accounts for them.
* float_coverage_{year}.csv — every filing in the population (including
  ABS-excluded ones) with its disposition, so completeness is externally
  auditable: nothing is silently dropped.

Deterministic: inputs are the pipeline CSVs, the cached SGML headers, and
float_overrides.py / entity_aliases.py — no network, no sub-agents, no
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
_filers_cache = {}


def filers_for(acc):
    if acc not in _filers_cache:
        _filers_cache[acc] = load_filers(directory, acc)
    return _filers_cache[acc]


def resolve_entity(acc, p, label):
    """The registrant identity of one output row: (cik, registrant,
    class_or_series, flag). A label naming a co-registrant (matched against
    the filing's own SGML FILER blocks, or via the committed alias table)
    yields that registrant's OWN CIK; anything else is a class/series
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
            "cover": r["value"], "xbrl": r["value_xbrl"], "tol": r["tol"],
            "as_of": r["as_of"], "label": r["label"], "resolved": "",
            "flags": ";".join(sorted(set(
                f for f in flags.split(";") if f))), "xbrl_verdict": r["xbrl"],
        })
    return out


def consolidate(r, flags):
    """The single verified value and its basis for one row. Precedence:
    a logged override resolution; a stated zero/none; the filer's exact tag
    when the cover prints a rounded figure the tag confirms (precision
    adopted, visibly); the cover as printed."""
    if r.get("resolved"):
        return r["resolved"], "RESOLVED_FILER_ERROR", flags
    cover = r["cover"]
    try:
        cv = float(cover)
    except (TypeError, ValueError):
        cv = None
    flag_set = set(flags.split(";"))
    if cv == 0:
        basis = "STATED_NONE" if "NONE_STATED" in flag_set else "STATED_ZERO"
        return cover, basis, flags
    try:
        tol = float(r.get("tol") or 0)
    except (TypeError, ValueError):
        tol = 0.0
    if r.get("xbrl_verdict") == "XBRL_MATCH" and tol > 0 and \
            r.get("xbrl") and "ROUNDING_MATCH" in flag_set:
        flags = ";".join(sorted(flag_set | {"TAG_PRECISION_ADOPTED"}))
        return r["xbrl"], "STATED_VALUE", flags
    return cover, "STATED_VALUE", flags


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
            rr.setdefault("resolved", "")
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
        if _v is not None and 0 < _v < 1000 and not r.get("resolved"):
            flags = ";".join(filter(None, [flags, "AS_FILED_MICRO_VALUE"]))
        row_validation = validation
        if validation == "OVERRIDE_VERIFIED" and not r.get("resolved") and \
                r.get("xbrl") and r.get("xbrl") == r.get("cover"):
            # the value itself is the filer's own tagged number; the override
            # supplied attribution (labels/dates), not the value
            row_validation = "XBRL_MATCH"
            flags = ";".join(filter(None, [flags, "OVERRIDE_ATTRIBUTION"]))
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
        cik, registrant, class_or_series, ent_flag = \
            resolve_entity(acc, p, r.get("label", ""))
        if ent_flag:
            flags = ";".join(filter(None, [flags, ent_flag]))
        public_float, basis, flags = consolidate(r, flags)
        rows.append({
            "accession": acc, "cik": cik, "registrant": registrant,
            "class_or_series": class_or_series,
            "form": p.form, "date_filed": p.date_filed,
            "public_float": public_float, "float_basis": basis,
            "public_float_date": r.get("as_of", ""),
            "public_float_cover": r["cover"],
            "public_float_xbrl": r.get("xbrl", ""),
            "validation": row_validation,
            "quality_flags": flags,
            "filing_index_url": p.filing_index_url,
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
print("\nfloat_basis of rows:")
print(df.groupby("float_basis").size().to_string())
unmatched = df[df["quality_flags"].str.contains("ENTITY_LABEL_UNMATCHED")]
print("\nENTITY_LABEL_UNMATCHED rows (review; alias or class?): %d"
      % len(unmatched))
if len(unmatched):
    print(unmatched[["accession", "registrant", "class_or_series"]]
          .to_string(index=False))
