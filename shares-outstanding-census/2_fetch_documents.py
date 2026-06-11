"""Fetch and cache the primary document of every in-scope filing in the
population (excluded_abs=False rows of population_{year}.csv).

The primary document is the cover-page-bearing 10-K / 20-F / 40-F itself —
it carries both the cover prose the extractor reads and the filer's own
inline-XBRL dei tags used for validation. Stored gzipped under
DATA_DIR/cache/docs/ with a metadata sidecar (filename, size, sha256), so
every later stage runs offline and reproducibly.

Safe to interrupt and re-run: already-cached filings are skipped.
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
# -----------------------------------------------------------------------------

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
doc_cache = os.path.join(directory, "cache", "docs")
pop_path = os.path.join(directory, "population_%d.csv" % year)

pop = pd.read_csv(pop_path, dtype=str, keep_default_na=False)
in_scope = pop[pop["excluded_abs"] == "False"].reset_index(drop=True)
print("in-scope filings: %d" % len(in_scope), flush=True)

session = lib.make_session(USER_AGENT)
failures = []
total_bytes = 0
n = len(in_scope)
for i, row in enumerate(in_scope.itertuples(index=False), 1):
    txt_path = "edgar/data/%s/%s.txt" % (row.cik, row.accession)
    content, meta = lib.fetch_primary_document(
        session, doc_cache, txt_path, row.accession, row.form)
    if content is None:
        failures.append("%s %s %s" % (row.accession, row.form, row.company_name))
    else:
        total_bytes += meta["bytes"]
    if i % 250 == 0 or i == n:
        print("[%d/%d] cached, %.2f GB uncompressed, %d without a primary doc"
              % (i, n, total_bytes / 1e9, len(failures)), flush=True)

print("\ndone: %d filings, %d with no matching primary document" %
      (n, len(failures)))
for f in failures:
    print("  NO_PRIMARY_DOC " + f)
