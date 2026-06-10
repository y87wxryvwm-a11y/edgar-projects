"""10_reconcile_relevance.py — reconcile the two blind relevance-classification
passes and build the final relevance table.

Stage logic (auto-detected):
  1. After relevance_round1.workflow.js: compares pass A vs pass B per filing
     (in code). Agreements become final; disagreements (or UNDETERMINABLE / a
     missing read) are written as round-2 adjudication batches.
  2. After relevance_round2.workflow.js: merges the adjudicators' definitive
     rulings and writes relevance_{YEAR}_n{SAMPLE_SIZE}.json covering ALL
     sampled filings — 10-Ks from the agent rounds, 20-F / 40-F marked
     NOT_RELEVANT_FORM by rule (the study's relevant universe is corporate
     10-K registrants with public equity shares).

Categories: RELEVANT | NOT_RELEVANT_ABS | NOT_RELEVANT_UNITS |
NOT_RELEVANT_DEBT_ONLY | NOT_RELEVANT_FORM.
"""

import os
import csv
import json
import glob
import collections

# ---- EDIT THIS --------------------------------------------------------------
YEAR = 2025
SAMPLE_SIZE = 1000
ROUND2_BATCH_SIZE = 4
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError("config.py not found. Copy config.example.py to config.py.")

directory = DATA_DIR.replace("\\", "/")
sample_csv = os.path.join(directory, f"sample_{YEAR}_n{SAMPLE_SIZE}.csv")
rel_dir = os.path.join(directory, "relevance")
batches_dir = os.path.join(rel_dir, "batches")
r1_dir = os.path.join(rel_dir, "round1")
r2_batches_dir = os.path.join(rel_dir, "round2", "batches")
r2_results_dir = os.path.join(rel_dir, "round2", "results")
out_json = os.path.join(directory, f"relevance_{YEAR}_n{SAMPLE_SIZE}.json")
recon_csv = os.path.join(rel_dir, f"relevance_reconciliation_{YEAR}.csv")

NOT_REL = {"NOT_RELEVANT_ABS", "NOT_RELEVANT_UNITS", "NOT_RELEVANT_DEBT_ONLY"}
VALID = NOT_REL | {"RELEVANT", "UNDETERMINABLE"}


def load_pass(p):
    out = {}
    for fp in sorted(glob.glob(os.path.join(r1_dir, p, "batch_*.json"))):
        try:
            d = json.load(open(fp, encoding="utf-8"))
        except Exception as e:
            print(f"WARNING: unreadable {fp}: {e}")
            continue
        for r in d.get("results", []):
            out[r["accession"]] = r
    return out


def load_round2():
    out = {}
    for fp in sorted(glob.glob(os.path.join(r2_results_dir, "batch_*.json"))):
        try:
            d = json.load(open(fp, encoding="utf-8"))
        except Exception as e:
            print(f"WARNING: unreadable {fp}: {e}")
            continue
        for r in d.get("results", []):
            out[r["accession"]] = r
    return out


def main():
    rows = list(csv.DictReader(open(sample_csv, encoding="utf-8")))
    tenk = [r for r in rows if r["form"] == "10-K"]
    items = {}
    for fp in sorted(glob.glob(os.path.join(batches_dir, "batch_*.json"))):
        for it in json.load(open(fp, encoding="utf-8")):
            items[it["accession"]] = it

    A, B = load_pass("A"), load_pass("B")
    rulings = load_round2()
    print(f"pass A: {len(A)}  pass B: {len(B)}  round2 rulings: {len(rulings)}")

    final, disputes, recon = {}, [], []
    for r in tenk:
        acc = r["accession"]
        a, b = A.get(acc), B.get(acc)
        ca = (a or {}).get("category", "")
        cb = (b or {}).get("category", "")
        ruling = rulings.get(acc)
        agree = (a is not None and b is not None and ca == cb
                 and ca in VALID and ca != "UNDETERMINABLE")
        recon.append({"accession": acc, "company": r["company"], "cat_A": ca,
                      "cat_B": cb, "agree": agree, "ruled": ruling is not None})
        if ruling is not None:
            final[acc] = {"relevant": ruling["category"] == "RELEVANT",
                          "category": ruling["category"],
                          "registrant_kind": ruling.get("registrant_kind", ""),
                          "borderline": bool(ruling.get("borderline")),
                          "confidence": ruling.get("confidence", ""),
                          "evidence": ruling.get("evidence", ""),
                          "source": "round2_ruling"}
        elif agree:
            final[acc] = {"relevant": ca == "RELEVANT", "category": ca,
                          "registrant_kind": a.get("registrant_kind", ""),
                          "borderline": bool(a.get("borderline")) or bool(b.get("borderline")),
                          "confidence": a.get("confidence", ""),
                          "evidence": a.get("evidence", ""),
                          "source": "round1_agree"}
        else:
            disputes.append({**items[acc], "verdict_A": a, "verdict_B": b})

    with open(recon_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(recon[0].keys()))
        w.writeheader()
        w.writerows(recon)

    if disputes:
        os.makedirs(r2_batches_dir, exist_ok=True)
        os.makedirs(r2_results_dir, exist_ok=True)
        n = 0
        for i in range(0, len(disputes), ROUND2_BATCH_SIZE):
            bid = f"{i // ROUND2_BATCH_SIZE:03d}"
            with open(os.path.join(r2_batches_dir, f"batch_{bid}.json"), "w",
                      encoding="utf-8") as f:
                json.dump(disputes[i:i + ROUND2_BATCH_SIZE], f, indent=1)
            n += 1
        print(f"\n{len(disputes)} filings need adjudication -> wrote {n} round-2 "
              f"batches to {r2_batches_dir}")
        print("Run audit_workflows/relevance_round2.workflow.js, then re-run this script.")
        return

    # everything resolved — add the form rule and write the final table
    for r in rows:
        if r["form"] != "10-K":
            final[r["accession"]] = {"relevant": False,
                                     "category": "NOT_RELEVANT_FORM",
                                     "registrant_kind": "", "borderline": False,
                                     "confidence": "high",
                                     "evidence": f"form {r['form']} — non-10-K annual report, excluded by rule",
                                     "source": "rule_form"}
    assert len(final) == len(rows), (len(final), len(rows))
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(final, f, indent=1)

    cats = collections.Counter(v["category"] for v in final.values())
    print(f"\nWrote {out_json}  ({len(final)} filings)")
    for k, v in cats.most_common():
        print(f"  {k:24} {v:>4}")
    bl = [(a, v) for a, v in final.items() if v["borderline"]]
    if bl:
        print(f"\nborderline calls ({len(bl)}):")
        comp = {r["accession"]: r["company"] for r in rows}
        for a, v in bl:
            print(f"  {a}  {comp.get(a, ''):45.45}  {v['category']}  [{v['source']}]")


if __name__ == "__main__":
    main()
