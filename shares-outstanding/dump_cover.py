"""dump_cover.py — print the full cleaned cover text + every 'outstanding'
context for one filing, straight from the local document cache.

Usage:  python dump_cover.py <accession>

Used during adjudication when the 9k-char evidence packet is not enough to settle
a disagreement: this shows the full cover region (up to 15k chars, what the
extractor actually sees) and every 'outstanding' context in the document, with no
truncation. Reads the gzip doc cache populated by the extractor — no network.
"""

import os
import re
import sys
import gzip
import json

import shares_lib as L

DOC_CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache", "docs")


def main():
    if len(sys.argv) != 2:
        print("usage: dump_cover.py <accession>")
        sys.exit(2)
    acc = sys.argv[1]
    p = os.path.join(DOC_CACHE_DIR, f"{acc}.json.gz")
    if not os.path.exists(p):
        print(f"(not cached: {p}) — run the extractor first")
        sys.exit(1)
    with gzip.open(p, "rt", encoding="utf-8") as f:
        d = json.load(f)
    text = L.html_to_text(d["raw"])
    cover = L.cover_region(text)
    print(f"DOC_TYPE={d['doc_type']}  PERIOD_OF_REPORT={d['period']}  FULL_LEN={len(text):,}")
    print("\n===== FULL COVER REGION (up to 15000 chars) =====")
    print(cover[:15000])
    print("\n===== ALL 'outstanding' CONTEXTS (±260 chars) =====")
    for i, m in enumerate(re.finditer(r"outstanding", text, re.I)):
        a, b = max(0, m.start() - 260), min(len(text), m.end() + 180)
        print(f"[{i+1}] ...{re.sub(chr(92)+'s+', ' ', text[a:b])}...")


if __name__ == "__main__":
    main()
