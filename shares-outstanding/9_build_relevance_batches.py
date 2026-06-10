"""9_build_relevance_batches.py — build the evidence batches for the RELEVANCE
classification of the sampled 10-K filings.

The study's relevant universe is corporate 10-K registrants with public equity
shares. Everything else is marked not relevant: non-corporate registrants whose
equity is units (LPs, LLCs, trusts — including MLPs and commodity/crypto ETF
trusts), debt-only issuers with no public equity, asset-backed-securities
issuers, and the non-10-K annual forms (20-F / 40-F) entirely — those are ruled
out by form, no agent read needed.

For each sampled 10-K this writes a neutral cover-evidence file (full cover
region + every "outstanding" context, from the local clean-text cache — no
network) and groups the filings into batches for the independent classification
agents (two blind passes, then adjudication of disagreements; see
audit_workflows/relevance_round1.workflow.js).
"""

import os
import re
import csv
import json

import shares_lib as L

# ---- EDIT THIS --------------------------------------------------------------
YEAR = 2025
SAMPLE_SIZE = 1000               # must match the sample file
BATCH_SIZE = 10                  # filings per classification agent
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError("config.py not found. Copy config.example.py to config.py.")

directory = DATA_DIR.replace("\\", "/")
sample_csv = os.path.join(directory, f"sample_{YEAR}_n{SAMPLE_SIZE}.csv")
rel_dir = os.path.join(directory, "relevance")
covers_dir = os.path.join(rel_dir, "covers")
batches_dir = os.path.join(rel_dir, "batches")


def cover_evidence(accession):
    """Full cover region + every 'outstanding' context, from the clean cache."""
    c = L._read_clean_cache(accession)
    if c is None:
        d = L._read_doc_cache(accession)
        if d is None:
            return None
        doc_type, raw, period = d
        text = L.html_to_text(raw)
    else:
        doc_type, text, period = c
    cover = L.cover_region(text)
    parts = [f"DOC_TYPE={doc_type}  PERIOD_OF_REPORT={period}  FULL_LEN={len(text):,}",
             "", "===== FULL COVER REGION (up to 15000 chars) =====", cover[:15000],
             "", "===== ALL 'outstanding' CONTEXTS (±260 chars) ====="]
    for i, m in enumerate(re.finditer(r"outstanding", text, re.I)):
        a, b = max(0, m.start() - 260), min(len(text), m.end() + 180)
        parts.append(f"[{i+1}] ...{re.sub(chr(92) + 's+', ' ', text[a:b])}...")
    return "\n".join(parts)


def main():
    os.makedirs(covers_dir, exist_ok=True)
    os.makedirs(batches_dir, exist_ok=True)

    rows = [r for r in csv.DictReader(open(sample_csv, encoding="utf-8"))
            if r["form"] == "10-K"]
    print(f"{len(rows)} 10-K filings in the sample")

    items, missing = [], []
    for r in rows:
        acc = r["accession"]
        ev = cover_evidence(acc)
        if ev is None:
            missing.append(acc)
            continue
        p = os.path.join(covers_dir, f"{acc}.txt")
        with open(p, "w", encoding="utf-8") as f:
            f.write(ev)
        items.append({"accession": acc, "company": r["company"], "cik": r["cik"],
                      "form": r["form"], "cover_path": p})

    n_batches = 0
    for i in range(0, len(items), BATCH_SIZE):
        bid = f"{i // BATCH_SIZE:03d}"
        with open(os.path.join(batches_dir, f"batch_{bid}.json"), "w", encoding="utf-8") as f:
            json.dump(items[i:i + BATCH_SIZE], f, indent=1)
        n_batches += 1

    print(f"wrote {len(items)} cover files -> {covers_dir}")
    print(f"wrote {n_batches} batches of <= {BATCH_SIZE} -> {batches_dir}")
    if missing:
        print(f"WARNING: {len(missing)} filings had no cached document: {missing}")


if __name__ == "__main__":
    main()
