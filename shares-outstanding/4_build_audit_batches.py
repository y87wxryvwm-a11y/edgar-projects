"""4_build_audit_batches.py — split the sample into per-batch audit work files.

Prepares the independent adversarial audit. For each sampled filing it records
the accession, form, company, and the absolute path to that filing's neutral
evidence packet (written by 3_dump_evidence.py). Filings are grouped into small
batches; one audit sub-agent processes one batch file and writes one results
file. This keeps the orchestration's arguments tiny (the agents read their own
batch file from disk) and makes the whole audit re-runnable and inspectable.

Run AFTER 3_dump_evidence.py. Writes under DATA_DIR/audit/<ROUND>/:
  batches/batch_000.json ...   the work, ~BATCH_SIZE filings each
  results/                     where each audit agent writes its verdicts
"""

import os
import csv
import json

# ---- EDIT THIS --------------------------------------------------------------
YEAR = 2025
SAMPLE_SIZE = 1000
BATCH_SIZE = 8          # filings per audit sub-agent
ROUND = "round1"        # subfolder under audit/ for this pass
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError("config.py not found. Copy config.example.py to config.py.")

directory = DATA_DIR.replace("\\", "/")
sample_csv = os.path.join(directory, f"sample_{YEAR}_n{SAMPLE_SIZE}.csv")
evidence_dir = os.path.join(directory, "evidence")
audit_dir = os.path.join(directory, "audit", ROUND)
batch_dir = os.path.join(audit_dir, "batches")
result_dir = os.path.join(audit_dir, "results")


def main():
    os.makedirs(batch_dir, exist_ok=True)
    os.makedirs(result_dir, exist_ok=True)
    with open(sample_csv, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    records = [{
        "accession": r["accession"],
        "form": r["form"],
        "company": r["company"],
        "cik": r["cik"],
        "evidence_path": os.path.abspath(os.path.join(evidence_dir, f"{r['accession']}.txt")),
    } for r in rows]

    n_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(n_batches):
        chunk = records[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
        with open(os.path.join(batch_dir, f"batch_{i:03d}.json"), "w", encoding="utf-8") as f:
            json.dump(chunk, f)

    print(f"{len(records)} filings -> {n_batches} batches of <= {BATCH_SIZE}")
    print(f"batches dir : {batch_dir}")
    print(f"results dir : {result_dir}")
    print(f"BATCH_COUNT={n_batches}")
    print(f"AUDIT_DIR={audit_dir}")


if __name__ == "__main__":
    main()
