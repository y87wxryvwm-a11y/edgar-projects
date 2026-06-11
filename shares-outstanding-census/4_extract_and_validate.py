"""Run the cover extractor over every in-scope filing's cached text and
validate each extracted row against the filer's own inline-XBRL facts.

Outputs (both in DATA_DIR):

* extraction_{year}.csv — one row per share class extracted from a cover,
  with its per-row XBRL verdict.
* filing_status_{year}.csv — one row per in-scope filing with the filing-
  level validation status that drives the improvement loop:
    VALIDATED       every prose row's number matches a tagged fact
    AGG_VALIDATED   prose class rows sum to the single tagged total
    MISMATCH        facts exist, at least one prose number matches nothing
    PROSE_ONLY      prose rows but the filer tagged no dei shares fact
    MISSED_BY_PROSE the filer tagged a fact but prose extraction found nothing
    EMPTY           no prose rows and no facts (candidate no-public-shares)

Pure compute over local caches — fast to re-run after every extractor change.
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
# -----------------------------------------------------------------------------

import gzip
import os

import pandas as pd

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set "
        "USER_AGENT and DATA_DIR."
    )

import cover_extractor as cx

directory = DATA_DIR.replace("\\", "/")
text_cache = os.path.join(directory, "cache", "text")
pop_path = os.path.join(directory, "population_%d.csv" % year)
facts_path = os.path.join(directory, "ixbrl_facts_%d.csv" % year)
api_facts_path = os.path.join(directory, "xbrl_api_facts_%d.csv" % year)
extraction_path = os.path.join(directory, "extraction_%d.csv" % year)
status_path = os.path.join(directory, "filing_status_%d.csv" % year)

pop = pd.read_csv(pop_path, dtype=str, keep_default_na=False)
in_scope = pop[pop["excluded_abs"] == "False"].reset_index(drop=True)
facts = pd.read_csv(facts_path, dtype=str, keep_default_na=False)
facts["value"] = facts["value"].astype("int64")
facts_by_acc = {a: g for a, g in facts.groupby("accession")}

# secondary XBRL source: SEC's companyconcept API (joined by accession);
# used only where the filing's own inline XBRL yielded no facts
api_by_acc = {}
if os.path.exists(api_facts_path):
    api = pd.read_csv(api_facts_path, dtype=str, keep_default_na=False)
    api["value"] = api["value"].astype("int64")
    api = api.rename(columns={"end": "instant"})
    api_by_acc = {a: g for a, g in api.groupby("accession")}
n = len(in_scope)
print("in-scope filings: %d, ixbrl facts: %d, api-covered filings: %d"
      % (n, len(facts), len(api_by_acc)), flush=True)

ext_rows, status_rows = [], []
for i, row in enumerate(in_scope.itertuples(index=False), 1):
    text_path = os.path.join(text_cache, row.accession + ".txt.gz")
    if not os.path.exists(text_path):
        status_rows.append({"accession": row.accession, "form": row.form,
                            "status": "NO_TEXT", "n_rows": 0, "n_facts": 0,
                            "filing_flags": "NO_TEXT"})
        continue
    with gzip.open(text_path, "rt", encoding="utf-8") as fh:
        text = fh.read()

    multi = int(row.n_filers) > 1
    rows, filing_flags = cx.extract_cover(
        text, row.form, row.period_of_report, multi_registrant=multi)
    if multi:
        filing_flags.append("MULTI_REGISTRANT")

    f = facts_by_acc.get(row.accession)
    xbrl_source = "inline"
    if f is None or len(f) == 0:
        f = api_by_acc.get(row.accession)
        xbrl_source = "api" if f is not None else ""
    all_fact_values = sorted(set(f["value"].tolist())) if f is not None else []
    # a tagged 0 is a no-public-shares signal, not a validatable count
    fact_values = [v for v in all_fact_values if v != 0]
    # latest instant per value; instants compared on the date part only
    fact_dates = ({v: inst[:10] for v, inst in
                   f.groupby("value")["instant"].max().items()}
                  if f is not None else {})

    n_match = 0
    for r in rows:
        if r["value"] in fact_values:
            r["xbrl"] = "XBRL_MATCH"
            n_match += 1
            inst = fact_dates.get(r["value"], "")
            if r["as_of"] and inst and r["as_of"] != inst:
                r["flags"].append("DATE_DIFF_VS_XBRL")
            elif not r["as_of"] and inst:
                r["flags"].append("XBRL_HAS_DATE")
        elif fact_values:
            r["xbrl"] = "XBRL_MISMATCH"
            if r["value"] * 1000 in fact_values or \
                    (r["value"] % 1000 == 0 and r["value"] // 1000 in fact_values):
                r["flags"].append("THOUSANDS_DISCREPANCY")
        else:
            r["xbrl"] = "XBRL_ABSENT"
    matched_values = {r["value"] for r in rows if r["xbrl"] == "XBRL_MATCH"}

    if rows and fact_values and n_match == len(rows) and \
            all(v in matched_values for v in fact_values):
        status = "VALIDATED"
    elif rows and len(fact_values) == 1 and len(rows) > 1 and \
            sum(r["value"] for r in rows if r["share_type"] not in
                ("preferred", "depositary")) == fact_values[0]:
        status = "AGG_VALIDATED"
        for r in rows:
            if r["xbrl"] == "XBRL_MISMATCH":
                r["xbrl"] = "XBRL_AGG_MATCH"
    elif rows and fact_values and all(v in matched_values for v in fact_values):
        # every tagged fact is confirmed; the extra prose rows are classes
        # the filer didn't tag (e.g. exchangeable shares) — tier-2 verifies
        status = "PROSE_SUPERSET"
        for r in rows:
            if r["xbrl"] == "XBRL_MISMATCH":
                r["xbrl"] = "XBRL_NOT_COVERED"
    elif rows and fact_values and n_match == len(rows):
        # every prose row is confirmed but the filer tagged MORE facts —
        # exactly how a missed share class hides; never auto-validated
        status = "ROWS_OK_FACTS_UNMATCHED"
    elif rows and fact_values:
        status = "MISMATCH"
    elif rows:
        status = "PROSE_ONLY"
    elif fact_values:
        status = "MISSED_BY_PROSE"
    elif all_fact_values:
        status = "ZERO_FACT"
    else:
        status = "EMPTY"

    for r in rows:
        ext_rows.append({
            "accession": row.accession, "cik": row.cik,
            "company_name": row.company_name, "form": row.form,
            "value": r["value"], "share_class_label": r["label"],
            "share_type": r["share_type"],
            "class_designator": r["class_designator"],
            "registrant": r["registrant"],
            "as_of": r["as_of"], "method": r["method"], "xbrl": r["xbrl"],
            "flags": ";".join(sorted(set(r["flags"]))),
            "filing_index_url": row.filing_index_url,
        })
    status_rows.append({
        "accession": row.accession, "form": row.form, "status": status,
        "n_rows": len(rows), "n_facts": len(fact_values),
        "xbrl_source": xbrl_source,
        "filing_flags": ";".join(sorted(set(filing_flags)))})

    if i % 500 == 0 or i == n:
        print("[%d/%d] extracted" % (i, n), flush=True)

ext = pd.DataFrame(ext_rows).sort_values(
    ["accession", "value"]).reset_index(drop=True)
ext.to_csv(extraction_path, index=False, encoding="utf-8", lineterminator="\n")
st = pd.DataFrame(status_rows).sort_values("accession").reset_index(drop=True)
st.to_csv(status_path, index=False, encoding="utf-8", lineterminator="\n")

print("\nwrote %s (%d class rows)" % (extraction_path, len(ext)))
print("wrote %s (%d filings)" % (status_path, len(st)))
print("\nfiling status x form:")
print(st.groupby(["status", "form"]).size().unstack(fill_value=0).to_string())
ok = st["status"].isin(["VALIDATED", "AGG_VALIDATED"]).sum()
print("\nvalidated: %d / %d (%.1f%%)" % (ok, len(st), 100.0 * ok / len(st)))
