"""Fetch SEC's structured XBRL record of dei:EntityCommonStockSharesOutstanding
for every in-scope CIK (the companyconcept API) — the secondary XBRL source.

The filing's own inline XBRL (script 3) is the primary validation source;
this API adds coverage where the primary document carries no parseable
facts (some 40-F wrap-arounds, exhibit-tagged filers) and an independent
cross-check elsewhere. Each fact in the API carries the accession number
("accn") of the filing that reported it, so rows join our population
directly.

Output: xbrl_api_facts_{year}.csv (accession, cik, value, end, fy, fp).
Raw JSON responses are cached per CIK under DATA_DIR/cache/xbrl_api/;
404s (filer never tagged the concept) are cached as negatives.
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
# -----------------------------------------------------------------------------

import gzip
import json
import os

import pandas as pd

try:
    from config import DATA_DIR, USER_AGENT
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set "
        "USER_AGENT and DATA_DIR."
    )

import census_lib as lib

directory = DATA_DIR.replace("\\", "/")
api_cache = os.path.join(directory, "cache", "xbrl_api")
pop_path = os.path.join(directory, "population_%d.csv" % year)
out_path = os.path.join(directory, "xbrl_api_facts_%d.csv" % year)

os.makedirs(api_cache, exist_ok=True)

pop = pd.read_csv(pop_path, dtype=str, keep_default_na=False)
in_scope = pop[pop["excluded_abs"] == "False"]
accessions = set(in_scope["accession"])
ciks = sorted({int(c) for cs in in_scope["all_ciks"]
               for c in cs.split(";") if c.isdigit()})
print("unique in-scope CIKs: %d" % len(ciks), flush=True)

session = lib.make_session(USER_AGENT)
rows, n_404 = [], 0
for i, cik in enumerate(ciks, 1):
    path = os.path.join(api_cache, "%010d.json.gz" % cik)
    if os.path.exists(path):
        with gzip.open(path, "rt", encoding="utf-8") as f:
            data = json.load(f)
    else:
        url = ("https://data.sec.gov/api/xbrl/companyconcept/CIK%010d/dei/"
               "EntityCommonStockSharesOutstanding.json" % cik)
        resp = lib.throttled_get(session, url, none_on_404=True)
        data = {"notfound": True} if resp is None else resp.json()
        with gzip.open(path, "wt", encoding="utf-8") as f:
            json.dump(data, f)
    if data.get("notfound"):
        n_404 += 1
        continue
    for unit_rows in (data.get("units") or {}).values():
        for fact in unit_rows:
            if fact.get("accn") in accessions and fact.get("val") is not None:
                rows.append({
                    "accession": fact["accn"], "cik": cik,
                    "value": int(fact["val"]), "end": fact.get("end", ""),
                    "fy": fact.get("fy", ""), "fp": fact.get("fp", ""),
                })
    if i % 250 == 0 or i == len(ciks):
        print("[%d/%d] CIKs, %d facts for our filings, %d never tagged"
              % (i, len(ciks), len(rows), n_404), flush=True)

df = pd.DataFrame(rows, columns=["accession", "cik", "value", "end", "fy", "fp"])
df = df.drop_duplicates().sort_values(
    ["accession", "cik", "value", "end"]).reset_index(drop=True)
df.to_csv(out_path, index=False, encoding="utf-8", lineterminator="\n")
print("\nwrote %s (%d facts, %d filings covered)" %
      (out_path, len(df), df["accession"].nunique()))
