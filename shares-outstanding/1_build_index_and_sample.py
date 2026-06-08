"""1_build_index_and_sample.py — build the EDGAR annual-filing index for a year
and draw a stratified random sample (50% 10-K / 40% 20-F / 10% 40-F).

Run this first. It downloads the four quarterly EDGAR full-index files for YEAR
(cached locally, so re-runs are free), writes the full index and a sampled
subset to DATA_DIR, and prints a sampling report.

Set SAMPLE_SIZE = 1000 for the full study run; keep it small (e.g. 60) for a
validation pass. The split and seed are fixed so the sample is reproducible.
"""

import os
import csv

import shares_lib as L

# ---- EDIT THIS --------------------------------------------------------------
YEAR = 2025            # the "prior calendar year" of filings to sample from
SAMPLE_SIZE = 1000     # 60 for a validation pass; 1000 for the full study
SEED = 20260607        # fixed -> reproducible sample
# Mix is the requested 50/40/10; override here only if the study spec changes.
MIX = {"10-K": 0.50, "20-F": 0.40, "40-F": 0.10}
# -----------------------------------------------------------------------------

try:
    from config import DATA_DIR
except ImportError:
    raise RuntimeError(
        "config.py not found. Copy config.example.py to config.py and set DATA_DIR/USER_AGENT."
    )

directory = DATA_DIR.replace("\\", "/")
os.makedirs(directory, exist_ok=True)
cache_dir = os.path.join(os.path.dirname(__file__), ".cache", "edgar")

index_csv = os.path.join(directory, f"index_{YEAR}.csv")
sample_csv = os.path.join(directory, f"sample_{YEAR}_n{SAMPLE_SIZE}.csv")

FIELDS = ["cik", "company", "form", "date_filed", "accession", "filename", "txt_url"]


def _write(path, filings):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for fi in filings:
            w.writerow({"cik": fi.cik, "company": fi.company, "form": fi.form,
                        "date_filed": fi.date_filed, "accession": fi.accession,
                        "filename": fi.filename, "txt_url": fi.txt_url})


def main():
    session = L.build_session()
    print(f"Building {YEAR} annual-filing index (10-K / 20-F / 40-F)...")
    filings = L.build_index(session, YEAR, cache_dir)

    # de-dupe by accession for the report (same filing can appear under >1 CIK)
    by_form = {}
    seen = set()
    for fi in filings:
        if fi.accession in seen:
            continue
        seen.add(fi.accession)
        by_form.setdefault(fi.form, []).append(fi)
    print("Unique filings available:")
    for form in ("10-K", "20-F", "40-F"):
        print(f"  {form:5} {len(by_form.get(form, [])):>6}")

    _write(index_csv, [fi for v in by_form.values() for fi in v])
    print(f"Wrote full index -> {index_csv}")

    sample, report = L.stratified_sample(filings, SAMPLE_SIZE, MIX, SEED)
    _write(sample_csv, sample)
    print(f"\nStratified sample (n={len(sample)}, seed={SEED}):")
    for form, r in report.items():
        short = "  *** fewer available than target!" if r["taken"] < r["target"] else ""
        print(f"  {form:5} target={r['target']:>4}  available={r['available']:>6}  "
              f"taken={r['taken']:>4}{short}")
    print(f"Wrote sample -> {sample_csv}")


if __name__ == "__main__":
    main()
