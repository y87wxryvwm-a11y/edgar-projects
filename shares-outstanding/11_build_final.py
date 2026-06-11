"""11_build_final.py — assemble the final study dataset over the sampled
filings.

Merges the sample, the relevance table (10-K corporate equity issuers vs
ABS / units / debt-only / non-10-K forms), the extractor's output, and the
golden-check verdict into one CSV:

  - relevant filings: one row per share class — number, class designator
    ("AX", "B", "" for an undesignated sole class), type, as-of date — plus
    the golden status (the extraction is shipped only because it matches the
    adversarially-audited golden table).
  - not-relevant filings: one row with relevant=False and the audited
    relevance category; share fields empty.
  - filing_url: the EDGAR index page of the filing, for manual verification.

Run after 2_extract.py and 8_check_golden.py.
"""

import os
import csv
import json

from shares_lib import class_designator

# ---- EDIT THIS --------------------------------------------------------------
YEAR = 2025
SAMPLE_SIZE = 1000
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError("config.py not found. Copy config.example.py to config.py.")

directory = DATA_DIR.replace("\\", "/")
sample_csv = os.path.join(directory, f"sample_{YEAR}_n{SAMPLE_SIZE}.csv")
val_jsonl = os.path.join(directory, f"validation_input_{YEAR}_n{SAMPLE_SIZE}.jsonl")
relevance_json = os.path.join(directory, f"relevance_{YEAR}_n{SAMPLE_SIZE}.json")
golden_json = os.path.join(directory, f"golden_{YEAR}_n{SAMPLE_SIZE}.json")
fail_json = os.path.join(directory, f"golden_failures_{YEAR}_n{SAMPLE_SIZE}.json")
out_csv = os.path.join(directory, f"final_{YEAR}_n{SAMPLE_SIZE}.csv")

FIELDS = ["accession", "cik", "company", "form", "date_filed",
          "relevant", "relevance_category", "relevance_source",
          "shares", "share_class", "share_type", "as_of_date",
          "golden_status", "golden_source", "filing_url"]


def index_url(cik, accession):
    return (f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
            f"{accession.replace('-', '')}/{accession}-index.html")


def main():
    sample = list(csv.DictReader(open(sample_csv, encoding="utf-8")))
    relevance = json.load(open(relevance_json, encoding="utf-8"))
    golden = json.load(open(golden_json, encoding="utf-8"))
    failing = {f["accession"]: f["status"]
               for f in json.load(open(fail_json, encoding="utf-8"))}
    claims = {}
    with open(val_jsonl, encoding="utf-8") as f:
        for ln in f:
            r = json.loads(ln)
            claims[r["accession"]] = r

    rows, n_rel, n_classes = [], 0, 0
    for s in sample:
        acc = s["accession"]
        rel = relevance[acc]
        base = {"accession": acc, "cik": s["cik"], "company": s["company"],
                "form": s["form"], "date_filed": s["date_filed"],
                "relevant": rel["relevant"], "relevance_category": rel["category"],
                "relevance_source": rel["source"],
                "golden_status": failing.get(acc, "PASS"),
                "golden_source": golden.get(acc, {}).get("source", ""),
                "filing_url": index_url(s["cik"], acc)}
        if not rel["relevant"]:
            rows.append({**base, "shares": "", "share_class": "",
                         "share_type": "", "as_of_date": ""})
            continue
        n_rel += 1
        entries = claims.get(acc, {}).get("script_entries", [])
        if not entries:
            rows.append({**base, "shares": "", "share_class": "NO_COVER_COUNT",
                         "share_type": "", "as_of_date": ""})
            continue
        for e in entries:
            n_classes += 1
            # bare designator ("AX", "JX"); a class with no designator keeps
            # its label (lowercased so cover-page casing doesn't vary row to
            # row) so multi-class rows stay distinguishable
            label = e.get("class_label", "")
            rows.append({**base, "shares": e.get("shares", ""),
                         "share_class": class_designator(label) or label.lower(),
                         "share_type": e.get("share_type", ""),
                         "as_of_date": e.get("as_of_date", "")})

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(rows)

    n_pass = sum(1 for s in sample if relevance[s["accession"]]["relevant"]
                 and s["accession"] not in failing)
    print(f"Wrote {out_csv}")
    print(f"  {len(sample)} filings -> {len(rows)} rows "
          f"({n_rel} relevant, {n_classes} class rows)")
    print(f"  relevant filings passing the golden check: {n_pass}/{n_rel}")
    if n_pass != n_rel:
        print("  WARNING: not all relevant filings pass — fix before shipping.")


if __name__ == "__main__":
    main()
