"""One offline pass over the cached primary documents producing the two
inputs every later stage works from:

1. ixbrl_facts_{year}.csv — every dei:EntityCommonStockSharesOutstanding fact
   the filer tagged in its own inline XBRL (value, as-of instant, dimension
   members). This is the validation side of the ladder.
2. DATA_DIR/cache/text/{accession}.txt.gz — the document reduced to clean
   text, so the prose extractor (and later the sub-agent evidence packets)
   iterate without re-parsing megabytes of HTML.

Pure compute over the local cache — no network. Safe to interrupt and
re-run; already-converted filings are skipped via the text cache.
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

import census_lib as lib

directory = DATA_DIR.replace("\\", "/")
doc_cache = os.path.join(directory, "cache", "docs")
text_cache = os.path.join(directory, "cache", "text")
pop_path = os.path.join(directory, "population_%d.csv" % year)
facts_path = os.path.join(directory, "ixbrl_facts_%d.csv" % year)

os.makedirs(text_cache, exist_ok=True)

pop = pd.read_csv(pop_path, dtype=str, keep_default_na=False)
in_scope = pop[pop["excluded_abs"] == "False"].reset_index(drop=True)
n = len(in_scope)
print("in-scope filings: %d" % n, flush=True)

fact_rows = []
no_doc, no_facts = [], 0
for i, row in enumerate(in_scope.itertuples(index=False), 1):
    content, meta = lib.read_cached_document(doc_cache, row.accession)
    if content is None:
        no_doc.append(row.accession)
        continue

    facts = lib.parse_ixbrl_dei_facts(content)
    if not facts:
        no_facts += 1
    for f in facts:
        fact_rows.append({"accession": row.accession, "value": f["value"],
                          "instant": f["instant"], "dims": f["dims"]})

    text_path = os.path.join(text_cache, row.accession + ".txt.gz")
    if not os.path.exists(text_path):
        text = lib.doc_to_text(content, meta["filename"])
        with gzip.open(text_path, "wt", encoding="utf-8") as fh:
            fh.write(text)

    if i % 250 == 0 or i == n:
        print("[%d/%d] processed, %d facts, %d filings w/o facts, %d w/o doc"
              % (i, n, len(fact_rows), no_facts, len(no_doc)), flush=True)

facts_df = pd.DataFrame(
    fact_rows, columns=["accession", "value", "instant", "dims"]).sort_values(
    ["accession", "value", "instant", "dims"]).reset_index(drop=True)
facts_df.to_csv(facts_path, index=False, encoding="utf-8", lineterminator="\n")

print("\nwrote %s (%d facts from %d filings)" %
      (facts_path, len(facts_df), facts_df["accession"].nunique()))
print("filings with no dei shares fact: %d" % no_facts)
print("filings with no primary doc:     %d" % len(no_doc))
for a in no_doc:
    print("  NO_PRIMARY_DOC " + a)
