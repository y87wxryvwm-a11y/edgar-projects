"""Build neutral evidence packets for every filing the XBRL ladder could
not settle — the input to the tier-2 independent reads.

A packet carries everything a reader needs and nothing that biases it:
the filing's identity, the cover text itself, what the extractor produced,
and both XBRL fact sets. Readers decide the ground truth from the cover
text; their reports drive GENERAL extractor improvements (and, for the
final ≤1%, the hand-verified override table).

Output: evidence/{year}/batch_NNN.jsonl under DATA_DIR (25 packets per
batch), plus evidence_index_{year}.csv summarizing what went out and why.
"""

# ---- EDIT THIS --------------------------------------------------------------
year = 2025
batch_size = 25
# -----------------------------------------------------------------------------

import gzip
import json
import os

import pandas as pd

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set "
        "USER_AGENT and DATA_DIR."
    )

directory = DATA_DIR.replace("\\", "/")
text_cache = os.path.join(directory, "cache", "text")
evidence_dir = os.path.join(directory, "evidence", str(year))
os.makedirs(evidence_dir, exist_ok=True)

pop = pd.read_csv(os.path.join(directory, "population_%d.csv" % year),
                  dtype=str, keep_default_na=False).set_index("accession")
status = pd.read_csv(os.path.join(directory, "filing_status_%d.csv" % year),
                     dtype=str, keep_default_na=False)
ext = pd.read_csv(os.path.join(directory, "extraction_%d.csv" % year),
                  dtype=str, keep_default_na=False)
facts = pd.read_csv(os.path.join(directory, "ixbrl_facts_%d.csv" % year),
                    dtype=str, keep_default_na=False)
api_path = os.path.join(directory, "xbrl_api_facts_%d.csv" % year)
api = pd.read_csv(api_path, dtype=str, keep_default_na=False) \
    if os.path.exists(api_path) else pd.DataFrame(
        columns=["accession", "cik", "value", "end", "fy", "fp"])

NEED_REVIEW = ["MISMATCH", "MISSED_BY_PROSE", "PROSE_ONLY",
               "ROWS_OK_FACTS_UNMATCHED", "PROSE_SUPERSET", "EMPTY",
               "ZERO_FACT", "NO_TEXT"]
todo = status[status["status"].isin(NEED_REVIEW)].sort_values("accession")
print("filings needing tier-2 review, by status:")
print(todo.groupby("status").size().to_string())

ext_by = {a: g for a, g in ext.groupby("accession")}
facts_by = {a: g for a, g in facts.groupby("accession")}
api_by = {a: g for a, g in api.groupby("accession")}

index_rows, batch, n_batch = [], [], 0


def flush_batch():
    global batch, n_batch
    if not batch:
        return
    path = os.path.join(evidence_dir, "batch_%03d.jsonl" % n_batch)
    with open(path, "w", encoding="utf-8") as fh:
        for p in batch:
            fh.write(json.dumps(p, ensure_ascii=False) + "\n")
    n_batch += 1
    batch = []


for srow in todo.itertuples(index=False):
    acc = srow.accession
    p = pop.loc[acc]
    text_path = os.path.join(text_cache, acc + ".txt.gz")
    cover_text = ""
    if os.path.exists(text_path):
        with gzip.open(text_path, "rt", encoding="utf-8") as fh:
            cover_text = fh.read()[:9000]
    e = ext_by.get(acc)
    f = facts_by.get(acc)
    a = api_by.get(acc)
    packet = {
        "accession": acc, "cik": p["cik"], "company_name": p["company_name"],
        "form": srow.form, "period_of_report": p["period_of_report"],
        "filing_index_url": p["filing_index_url"], "n_filers": p["n_filers"],
        "status": srow.status, "filing_flags": srow.filing_flags,
        "extracted_rows": e.to_dict("records") if e is not None else [],
        "inline_xbrl_facts": f.to_dict("records") if f is not None else [],
        "api_xbrl_facts": a.to_dict("records") if a is not None else [],
        "cover_text": cover_text,
    }
    batch.append(packet)
    index_rows.append({"accession": acc, "status": srow.status,
                       "batch": "batch_%03d.jsonl" % n_batch})
    if len(batch) >= batch_size:
        flush_batch()
flush_batch()

idx = pd.DataFrame(index_rows)
idx_path = os.path.join(directory, "evidence_index_%d.csv" % year)
idx.to_csv(idx_path, index=False, encoding="utf-8", lineterminator="\n")
print("\nwrote %d packets in %d batches under %s" %
      (len(idx), n_batch, evidence_dir))
print("index: %s" % idx_path)
