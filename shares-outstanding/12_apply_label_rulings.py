"""12_apply_label_rulings.py — fold the label-adjudication rulings into the
golden table.

The original golden labels were never label-audited (round 1 compared numbers,
types and dates only; `7_build_golden.py` copied agree-row labels from the
extractor of the day). When the label check (8_check_golden, CHECK_LABEL)
disputes a row, audit_workflows/label_adjudication.workflow.js has a fresh
agent rule the correct label per share count from the full cover; this script
applies those rulings to golden_*.json — replacing share_class (and share_type
when ruled) for each matched number — and stamps the row's source with
"+label_adjudication" so the provenance is visible.

Numbers must match the golden row exactly; a ruling that lists different
numbers than golden is reported, not applied (that is a number dispute, not a
label fix).
"""

import os
import json
import glob

# ---- EDIT THIS --------------------------------------------------------------
YEAR = 2025
SAMPLE_SIZE = 1000
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError("config.py not found. Copy config.example.py to config.py.")

directory = DATA_DIR.replace("\\", "/")
golden_json = os.path.join(directory, f"golden_{YEAR}_n{SAMPLE_SIZE}.json")
rulings_dir = os.path.join(directory, "audit", "labels", "results")


def main():
    golden = json.load(open(golden_json, encoding="utf-8"))
    applied, skipped = 0, []
    for fp in sorted(glob.glob(os.path.join(rulings_dir, "*.json"))):
        r = json.load(open(fp, encoding="utf-8"))
        acc = r["accession"]
        g = golden.get(acc)
        if g is None:
            skipped.append((acc, "not in golden"))
            continue
        ruled = {c["number"]: c for c in r.get("classes", []) if c.get("number")}
        gold_nums = [c["number"] for c in g.get("classes", [])]
        if sorted(ruled) != sorted(gold_nums):
            skipped.append((acc, f"number mismatch ruled={sorted(ruled)} golden={sorted(gold_nums)}"))
            continue
        changed = False
        for c in g["classes"]:
            rc = ruled[c["number"]]
            if c.get("share_class") != rc.get("share_class") or \
               (rc.get("share_type") and c.get("share_type") != rc.get("share_type")):
                c["share_class"] = rc.get("share_class", c.get("share_class"))
                if rc.get("share_type"):
                    c["share_type"] = rc["share_type"]
                changed = True
        if changed and "+label_adjudication" not in g.get("source", ""):
            g["source"] = g.get("source", "") + "+label_adjudication"
        applied += 1

    with open(golden_json, "w", encoding="utf-8") as f:
        json.dump(golden, f, indent=1)
    print(f"applied {applied} rulings -> {golden_json}")
    for acc, why in skipped:
        print(f"  SKIPPED {acc}: {why}")


if __name__ == "__main__":
    main()
