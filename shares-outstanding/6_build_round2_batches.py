"""6_build_round2_batches.py — build adjudication work files from the
reconciliation disagreements.

Reads disagreements_<year>_n<size>_<round>.json (written by 5_reconcile_audit.py),
drops the not-yet-audited rows, attaches each filing's evidence-packet path, and
splits the rest into small batches for the round-2 adjudicators. One adjudicator
agent settles one batch against the authoritative full cover.

Writes under DATA_DIR/audit/round2/:
  batches/adjbatch_000.json ...   the disagreements to adjudicate
  results/                        where each adjudicator writes its rulings
"""

import os
import csv
import json

# ---- EDIT THIS --------------------------------------------------------------
YEAR = 2025
SAMPLE_SIZE = 1000
ROUND = "round1"        # which reconciliation round's disagreements to adjudicate
BATCH_SIZE = 4          # disagreements per adjudicator agent
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError("config.py not found. Copy config.example.py to config.py.")

directory = DATA_DIR.replace("\\", "/")
disagree_json = os.path.join(directory, f"disagreements_{YEAR}_n{SAMPLE_SIZE}_{ROUND}.json")
evidence_dir = os.path.join(directory, "evidence")
batch_dir = os.path.join(directory, "audit", "round2", "batches")
result_dir = os.path.join(directory, "audit", "round2", "results")

# statuses that are genuine disagreements to adjudicate (skip not-yet-audited)
ADJUDICATE = {"DISAGREE_EXTRA", "DISAGREE_NUMBER", "DISAGREE_MISS", "DISAGREE_TYPE",
              "DISAGREE_DATE", "DISAGREE_FALSE_POS", "NEEDS_REVIEW", "NO_AUDIT"}


def main():
    os.makedirs(batch_dir, exist_ok=True)
    os.makedirs(result_dir, exist_ok=True)
    items = json.load(open(disagree_json, encoding="utf-8"))
    items = [d for d in items if d["status"] in ADJUDICATE]
    for d in items:
        d["evidence_path"] = os.path.abspath(os.path.join(evidence_dir, f"{d['accession']}.txt"))

    n_batches = (len(items) + BATCH_SIZE - 1) // BATCH_SIZE
    for i in range(n_batches):
        chunk = items[i * BATCH_SIZE:(i + 1) * BATCH_SIZE]
        with open(os.path.join(batch_dir, f"adjbatch_{i:03d}.json"), "w", encoding="utf-8") as f:
            json.dump(chunk, f)

    print(f"{len(items)} disagreements -> {n_batches} batches of <= {BATCH_SIZE}")
    print(f"batches dir : {batch_dir}")
    print(f"results dir : {result_dir}")
    print(f"BATCH_COUNT={n_batches}")


if __name__ == "__main__":
    main()
