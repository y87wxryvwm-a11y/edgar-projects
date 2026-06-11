"""One offline pass over the cached primary documents extracting every
inline-XBRL dei:EntityPublicFloat fact (value, as-of instant, dimension
members, currency unit) — the validation side of the public-float ladder,
exactly as ixbrl_facts_{year}.csv is for shares outstanding.

Monetary semantics: a tagged 0 is a meaningful no-float signal (shells,
wholly-owned registrants) and an explicit xsi:nil fact is kept with an empty
value, so both negatives survive into the status logic instead of vanishing.

Pure compute over the local doc cache (built by script 2) — no network, no
text-cache rebuild (script 3 already wrote it). Safe to interrupt and re-run.
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

import census_lib as lib

directory = DATA_DIR.replace("\\", "/")
doc_cache = os.path.join(directory, "cache", "docs")
pop_path = os.path.join(directory, "population_%d.csv" % year)
facts_path = os.path.join(directory, "float_facts_%d.csv" % year)

pop = pd.read_csv(pop_path, dtype=str, keep_default_na=False)
in_scope = pop[pop["excluded_abs"] == "False"].reset_index(drop=True)
n = len(in_scope)
print("in-scope filings: %d" % n, flush=True)

fact_rows = []
no_doc, no_facts, n_zero, n_nil = [], 0, 0, 0
for i, row in enumerate(in_scope.itertuples(index=False), 1):
    content, meta = lib.read_cached_document(doc_cache, row.accession)
    if content is None:
        no_doc.append(row.accession)
        continue

    facts = lib.parse_ixbrl_float_facts(content)
    if not facts:
        no_facts += 1
    for f in facts:
        if f["value"] == "":
            n_nil += 1
        elif f["value"] == "0":
            n_zero += 1
        fact_rows.append({"accession": row.accession, "value": f["value"],
                          "instant": f["instant"], "dims": f["dims"],
                          "unit": f["unit"]})

    if i % 250 == 0 or i == n:
        print("[%d/%d] processed, %d facts (%d zero, %d nil), "
              "%d filings w/o facts, %d w/o doc"
              % (i, n, len(fact_rows), n_zero, n_nil, no_facts, len(no_doc)),
              flush=True)

facts_df = pd.DataFrame(
    fact_rows, columns=["accession", "value", "instant", "dims", "unit"]
).sort_values(["accession", "value", "instant", "dims", "unit"]).reset_index(
    drop=True)
facts_df.to_csv(facts_path, index=False, encoding="utf-8", lineterminator="\n")

print("\nwrote %s (%d facts from %d filings)" %
      (facts_path, len(facts_df), facts_df["accession"].nunique()))
print("filings with no float fact:   %d" % no_facts)
print("filings with no primary doc:  %d" % len(no_doc))
for a in no_doc:
    print("  NO_PRIMARY_DOC " + a)
