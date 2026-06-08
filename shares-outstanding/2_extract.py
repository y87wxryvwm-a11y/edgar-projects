"""2_extract.py — run the shares-outstanding extractor over a sample (or the
full index) and write the results.

Reads the sample CSV produced by 1_build_index_and_sample.py, downloads each
filing's primary document, extracts (shares, class, date) per share class,
cross-checks against the XBRL dei fact, and writes:

  - extraction_results_*.csv   one row per (filing x share class); misses get a
                               blank row so they're visible. Includes method,
                               confidence, flags, and the matched text snippet.
  - validation_input_*.jsonl   one JSON record per filing (what the script
                               claims) — input for the independent sub-agent
                               validation pass.

Then it prints a QA summary: coverage, flag distribution, mean confidence.
"""

import os
import csv
import json

import shares_lib as L

# ---- EDIT THIS --------------------------------------------------------------
YEAR = 2025
SAMPLE_SIZE = 1000               # must match the sample file you built
INPUT_CSV = None                 # None -> use sample_{YEAR}_n{SAMPLE_SIZE}.csv
DO_XBRL = True                   # cross-check against SEC structured data
LIMIT = None                     # cap rows for a quick test; None = all
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set DATA_DIR/USER_AGENT."
    )

directory = DATA_DIR.replace("\\", "/")
in_csv = INPUT_CSV or os.path.join(directory, f"sample_{YEAR}_n{SAMPLE_SIZE}.csv")
out_csv = os.path.join(directory, f"extraction_results_{YEAR}_n{SAMPLE_SIZE}.csv")
out_jsonl = os.path.join(directory, f"validation_input_{YEAR}_n{SAMPLE_SIZE}.jsonl")

ROW_FIELDS = ["cik", "company", "form", "date_filed", "accession", "method",
              "doc_type", "confidence", "flags", "xbrl_n", "shares", "raw_number",
              "scale", "class_label", "share_type", "as_of_date", "matched_text",
              "txt_url", "error"]


def load_sample(path):
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    return [L.Filing(cik=r["cik"], company=r["company"], form=r["form"],
                     date_filed=r["date_filed"], accession=r["accession"],
                     filename=r["filename"]) for r in rows]


def main():
    session = L.build_session()
    filings = load_sample(in_csv)
    if LIMIT:
        filings = filings[:LIMIT]
    print(f"Extracting from {len(filings)} filings...")

    all_rows, val_records = [], []
    flag_counts, conf_sum, matched = {}, 0.0, 0
    for i, fi in enumerate(filings, 1):
        ex = L.process_filing(session, fi, do_xbrl=DO_XBRL)
        all_rows.extend(L.extraction_to_rows(ex))
        conf_sum += ex.confidence
        if ex.entries:
            matched += 1
        for fl in ex.flags:
            flag_counts[fl] = flag_counts.get(fl, 0) + 1
        val_records.append({
            "cik": fi.cik, "company": fi.company, "form": fi.form,
            "date_filed": fi.date_filed, "accession": fi.accession,
            "txt_url": fi.txt_url, "method": ex.method,
            "confidence": ex.confidence, "flags": ex.flags,
            "period_of_report": ex.period_of_report,
            "script_entries": [
                {"shares": e.shares, "share_type": e.share_type,
                 "class_label": e.class_label, "as_of_date": e.as_of_date,
                 "scale": e.scale, "matched_text": e.matched_text}
                for e in ex.entries],
            "xbrl_values": [v for v in ex.xbrl_values
                            if v.get("form", "").startswith(fi.form)][:6],
            "error": ex.error,
        })
        if i % 10 == 0 or i == len(filings):
            print(f"  {i}/{len(filings)}  (matched so far: {matched})")

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=ROW_FIELDS)
        w.writeheader()
        for r in all_rows:
            w.writerow(r)
    with open(out_jsonl, "w", encoding="utf-8") as f:
        for rec in val_records:
            f.write(json.dumps(rec) + "\n")

    n = len(filings)
    print(f"\n=== QA SUMMARY (n={n}) ===")
    print(f"  filings with >=1 extraction : {matched}/{n} ({matched/n:.0%})")
    print(f"  mean confidence             : {conf_sum/n:.3f}")
    print(f"  output rows                 : {len(all_rows)}")
    print("  flag distribution:")
    for fl, c in sorted(flag_counts.items(), key=lambda x: -x[1]):
        print(f"    {fl:24} {c}")
    print(f"\nWrote {out_csv}")
    print(f"Wrote {out_jsonl}")


if __name__ == "__main__":
    main()
