"""3_dump_evidence.py — write a neutral evidence packet per sampled filing.

For each filing in the sample, writes data/evidence/<accession>.txt containing
the cover text, all 'outstanding' contexts, and the SEC dei fact. These files
are the independent ground-truth inputs for the sub-agent validation pass — each
agent reads one and judges what the filing actually says, without seeing the
extractor's regex output.
"""

import os
import csv

import shares_lib as L
from validate_helper import build_evidence

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
evidence_dir = os.path.join(directory, "evidence")
os.makedirs(evidence_dir, exist_ok=True)


def main():
    session = L.build_session()
    with open(sample_csv, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"Dumping evidence for {len(rows)} filings -> {evidence_dir}")
    for i, r in enumerate(rows, 1):
        path = os.path.join(evidence_dir, f"{r['accession']}.txt")
        try:
            ev = build_evidence(session, r["cik"], r["accession"], r["form"])
        except Exception as e:
            ev = f"FETCH_ERROR: {type(e).__name__}: {e}"
        with open(path, "w", encoding="utf-8") as out:
            out.write(f"COMPANY={r['company']}\nCIK={r['cik']}\nACCESSION={r['accession']}\n"
                      f"DATE_FILED={r['date_filed']}\n\n{ev}\n")
        if i % 10 == 0 or i == len(rows):
            print(f"  {i}/{len(rows)}")
    print("Done.")


if __name__ == "__main__":
    main()
