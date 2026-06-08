"""7_build_golden.py — assemble the ground-truth ("golden") table for all 1000
filings from the two audit rounds.

Ground truth per filing comes from the strongest available source:
  - round-1 AGREE / AGREE_TRUE_NEG  -> the extractor and the independent auditor
    already agreed, so that value is ground truth.
  - round-2 ruling                  -> for every disagreement, the adjudicator's
    definitive call (made against the full cover) is ground truth.

Writes golden_<year>_n<size>.json: {accession: {verdict, classes:[{number,
share_type, as_of_date, share_class}], source}}. This is the fixed target the
extractor must match; 8_check_golden.py compares any re-extraction against it.
"""

import os
import csv
import json
import glob

# ---- EDIT THIS --------------------------------------------------------------
YEAR = 2025
SAMPLE_SIZE = 1000
ROUND = "round1"
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError("config.py not found. Copy config.example.py to config.py.")

directory = DATA_DIR.replace("\\", "/")
recon_csv = os.path.join(directory, f"reconciliation_{YEAR}_n{SAMPLE_SIZE}_{ROUND}.csv")
val_jsonl = os.path.join(directory, f"validation_input_{YEAR}_n{SAMPLE_SIZE}.jsonl")
round2_dir = os.path.join(directory, "audit", "round2", "results")
golden_json = os.path.join(directory, f"golden_{YEAR}_n{SAMPLE_SIZE}.json")

AGREE_STATUSES = {"AGREE", "AGREE_ROUNDED", "AGREE_TRUE_NEG"}


def _coerce_int(x):
    if isinstance(x, (int, float)):
        return int(x)
    try:
        return int(str(x).replace(",", "").strip())
    except (ValueError, AttributeError):
        return None


def main():
    statuses = {}
    with open(recon_csv, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            statuses[r["accession"]] = r["status"]

    claims = {}
    with open(val_jsonl, encoding="utf-8") as f:
        for ln in f:
            r = json.loads(ln)
            claims[r["accession"]] = r

    # round-2 definitive rulings (latest write wins if duplicated)
    rulings = {}
    for p in glob.glob(os.path.join(round2_dir, "*.json")):
        try:
            data = json.load(open(p, encoding="utf-8"))
        except Exception as e:
            print(f"  !! unparseable {os.path.basename(p)}: {e}")
            continue
        for r in data.get("rulings", []):
            rulings[r["accession"]] = r

    golden = {}
    src_counts = {}
    for acc, claim in claims.items():
        st = statuses.get(acc, "")
        if acc in rulings:
            d = rulings[acc].get("definitive", {})
            classes = [{"number": _coerce_int(c.get("number")),
                        "share_type": c.get("share_type", ""),
                        "as_of_date": c.get("as_of_date", ""),
                        "share_class": c.get("share_class", "")}
                       for c in d.get("classes", []) if _coerce_int(c.get("number"))]
            verdict = "TRUE_NEGATIVE" if (d.get("verdict") == "TRUE_NEGATIVE" or not classes) else "HAS_SHARES"
            golden[acc] = {"verdict": verdict, "classes": classes, "source": "round2_ruling"}
            src = "round2_ruling"
        elif st in AGREE_STATUSES:
            entries = claim.get("script_entries", [])
            classes = [{"number": e["shares"], "share_type": e.get("share_type", ""),
                        "as_of_date": e.get("as_of_date", ""), "share_class": e.get("class_label", "")}
                       for e in entries if e.get("shares")]
            verdict = "TRUE_NEGATIVE" if not classes else "HAS_SHARES"
            golden[acc] = {"verdict": verdict, "classes": classes, "source": "round1_agree"}
            src = "round1_agree"
        else:
            golden[acc] = {"verdict": "UNKNOWN", "classes": [], "source": "UNRESOLVED"}
            src = "UNRESOLVED"
        src_counts[src] = src_counts.get(src, 0) + 1

    with open(golden_json, "w", encoding="utf-8") as f:
        json.dump(golden, f, indent=1)

    print(f"golden table for {len(golden)} filings")
    for s, c in sorted(src_counts.items(), key=lambda x: -x[1]):
        print(f"  {s:16} {c}")
    unresolved = [a for a, g in golden.items() if g["source"] == "UNRESOLVED"]
    if unresolved:
        print(f"  !! UNRESOLVED ({len(unresolved)}): {unresolved[:10]}")
    print(f"Wrote {golden_json}")


if __name__ == "__main__":
    main()
